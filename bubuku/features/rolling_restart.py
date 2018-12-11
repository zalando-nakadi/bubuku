import logging
from time import time

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
        self.cluster_config = cluster_config
        self.cluster_config.set_application_version(image)
        self.cluster_config.set_instance_type(instance_type)
        self.cluster_config.set_scalyr_account_key(scalyr_key)
        self.cluster_config.set_scalyr_region(scalyr_region)
        self.cluster_config.set_kms_key_id(kms_key_id)
        self.cluster_config.set_vpc_id(vpc_id)
        self.state_context = StateContext(self.cluster_config, broker_id_to_restart,
                                          self.zk.get_broker_address(broker_id_to_restart))

    def get_name(self) -> str:
        return 'rolling_restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'stop', 'restart', 'rebalance']])

    def run(self, current_actions) -> bool:
        if not self._is_cluster_in_good_state():
            _LOG.error('Cluster is not stable skipping restart iteration')
            return True

        return self.state_context.run()

    def _is_cluster_in_good_state(self):
        return True


class StateContext:
    def __init__(self, cluster_config, broker_id_to_restart, broker_ip_to_restart):
        self.broker_id_to_restart = broker_id_to_restart
        self.broker_ip_to_restart = broker_ip_to_restart
        self.cluster_config = cluster_config
        self.aws = AWSResources(region=self.cluster_config.get_aws_region())
        self.current_state = StopKafka(self)
        self.state_wait_broker_stopped = WaitBrokerStopped(self)
        self.state_detach_volume = DetachVolume(self)
        self.state_terminate_instance = TerminateInstance(self)
        self.state_launch_instance = LaunchInstance(self)
        self.state_attach_volume = AttachVolume(self)

    def run(self):
        """
        Runs states one after another. If state is finished, it takes the next one.
        """
        try:
            if self.current_state.run():
                next_state = self.current_state.next()
                _LOG.info('Next state is {}'.format(next_state))
                if next_state is None:
                    return False
                self.current_state = next_state
            return True
        except Exception as e:
            _LOG.error('Failed to run state', exc_info=e)
            return True


class State:
    """
    State which can be run as many times as required before it finishes it work. The progress of the state has to be
    recoverable
    """

    def __init__(self, state_context):
        self.state_context = state_context
        self.time_to_check_s = time()

    def run(self) -> bool:
        """
        Runs the state, and if state finishes successfully it returns True, otherwise it returns False, which means
        that state has to be executed again
        """
        pass

    def next(self):
        """
        Return the next state, which has to be executed after the current state
        """
        pass

    def run_with_timeout(self, func, timeout_s=10):
        """
        Runs func() with timeout
        :param func function to execute
        :param timeout_s timeout before executing state next time
        """
        if time() >= self.time_to_check_s:
            self.time_to_check_s = time() + timeout_s
            return func()
        return False


class StopKafka(State):
    def run(self):
        _LOG.info('Stopping broker {} {}'.format(self.state_context.broker_id_to_restart,
                                                 self.state_context.broker_ip_to_restart))
        resp = requests.post(ApiConfig.get_url(self.state_context.broker_ip_to_restart, 'stop'))
        if resp.status_code != 200:
            _LOG.error('Failed to stop Kafka: {} {}', resp.status_code, resp.text)
            return False
        return True

    def next(self):
        return self.state_context.state_wait_broker_stopped


class WaitBrokerStopped(State):
    def __init__(self, state_context):
        super(WaitBrokerStopped, self).__init__(state_context)
        self.time_to_check_s = time()

    def run(self):
        def func():
            resp = requests.get(ApiConfig.get_url(self.state_context.broker_ip_to_restart, 'state')).json()
            return resp.get('state') == 'stopped'

        return self.run_with_timeout(func)

    def next(self):
        return self.state_context.state_detach_volume


class DetachVolume(State):
    def run(self):
        instance = node.get_instance_by_ip(self.state_context.aws.ec2_resource,
                                           self.state_context.cluster_config,
                                           self.state_context.broker_ip_to_restart)

        _LOG.info('Searching for instance %s volumes', instance.instance_id)
        volumes = self.state_context.aws.ec2_client.describe_instance_attribute(InstanceId=instance.instance_id,
                                                                                Attribute='blockDeviceMapping')
        data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
        data_volume_id = data_volume['Ebs']['VolumeId']

        _LOG.info('Creating tag:Name=%s for %s', volume.KAFKA_LOGS_EBS, data_volume_id)
        vol = self.state_context.aws.ec2_resource.Volume(data_volume_id)
        self.state_context.cluster_config.set_availability_zone(vol.availability_zone)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': volume.KAFKA_LOGS_EBS}])
        _LOG.info('Detaching %s from %s', data_volume_id, instance.instance_id)

        self.state_context.aws.ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)
        return True

    def next(self):
        return self.state_context.state_terminate_instance


class TerminateInstance(State):
    def run(self):
        instance = node.get_instance_by_ip(self.state_context.aws.ec2_resource,
                                           self.state_context.cluster_config,
                                           self.state_context.broker_ip_to_restart)

        def func():
            node.terminate(self.state_context.aws, self.state_context.cluster_config, instance)

        return self.run_with_timeout(func)

    def next(self):
        return self.state_context.state_launch_instance


class LaunchInstance(State):
    def run(self):
        ec2 = EC2(self.state_context.aws)
        ec2.create(self.state_context.cluster_config, 1)

    def next(self):
        return self.state_context.state_attach_volume


class AttachVolume(State):
    def __init__(self, state_context):
        super(AttachVolume, self).__init__(state_context)
        self.time_to_check_s = time()

    def run(self):
        def func():
            volume.are_volumes_attached(self.state_context.aws)

        return self.run_with_timeout(func)

    def next(self):
        return None
