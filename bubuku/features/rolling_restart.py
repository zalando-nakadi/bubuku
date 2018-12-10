import logging

import requests

from bubuku.api import ApiConfig
from bubuku.aws import AWSResources, node, volume
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ec2_node import EC2
from bubuku.controller import Change
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rolling_restart')


class RollingRestartChange(Change):
    def __init__(self, zk: BukuExhibitor, cluster_config: ClusterConfig,
                 broker_id_to_restart: str,
                 image: str,
                 instance_type: str,
                 scalyr_key: str,
                 scalyr_region: str,
                 kms_key_id: str,
                 vpc_id: str):
        self.zk = zk
        self.broker_id_to_restart = broker_id_to_restart
        self.cluster_config = cluster_config
        self.cluster_config.set_application_version(image)
        self.cluster_config.set_instance_type(instance_type)
        self.cluster_config.set_scalyr_account_key(scalyr_key)
        self.cluster_config.set_scalyr_region(scalyr_region)
        self.cluster_config.set_kms_key_id(kms_key_id)
        self.cluster_config.set_vpc_id(vpc_id)
        self.kafka_stopped = False
        self.requests_session = requests.sessions.Session()

    def get_name(self) -> str:
        return 'rolling_restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'stop', 'restart', 'rebalance']])

    def run(self, current_actions) -> bool:
        if not self._is_cluster_in_good_state():
            _LOG.error('Cluster is not stable skipping restart iteration')
            return True

        if not self._gracefully_stop_kafka():
            return True

        self._relaunch_kafka_instance(broker_ip_to_restart)

        return False

    def _gracefully_stop_kafka(self) -> bool:
        broker_ip_to_restart = self.zk.get_broker_address(self.broker_id_to_restart)
        if not self.kafka_stopped:
            _LOG.info('Stopping broker {} {}'.format(self.broker_id_to_restart, broker_ip_to_restart))
            resp = self.requests_session.post(ApiConfig.get_url(broker_ip_to_restart, 'stop'))
            if resp.status_code != 200:
                _LOG.error('Failed to stop Kafka: {} {}', resp.status_code, resp.text)
                return False
            self.kafka_stopped = True

        resp = self.requests_session.get(ApiConfig.get_url(broker_ip_to_restart, 'state')).json()
        _LOG.info('Check broker is stopped {}'.format(resp))
        if resp.get('state') == 'stopped':
            return True

        return False

    def _relaunch_kafka_instance(self, broker_ip_to_restart) -> bool:
        aws_ = AWSResources(region=self.cluster_config.get_aws_region())

        instance = node.get_instance_by_ip(aws_.ec2_resource, self.cluster_config, broker_ip_to_restart)

        _LOG.info('Searching for instance %s volumes', instance.instance_id)
        volumes = aws_.ec2_client.describe_instance_attribute(InstanceId=instance.instance_id,
                                                              Attribute='blockDeviceMapping')
        data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
        data_volume_id = data_volume['Ebs']['VolumeId']

        _LOG.info('Creating tag:Name=%s for %s', volume.KAFKA_LOGS_EBS, data_volume_id)
        vol = aws_.ec2_resource.Volume(data_volume_id)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': volume.KAFKA_LOGS_EBS}])
        _LOG.info('Detaching %s from %s', data_volume_id, instance.instance_id)

        aws_.ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

        node.terminate(aws_, self.cluster_config, instance)
        self.cluster_config.set_availability_zone(vol.availability_zone)

        ec2 = EC2(aws_)

        ec2.create(self.cluster_config, 1)
        # volumes are going to be attached by taupage
        volume.wait_volumes_attached(aws_)

    def _is_cluster_in_good_state(self):
        return True
