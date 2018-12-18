import logging

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig

_LOG = logging.getLogger('bubuku.aws.node')

KAFKA_LOGS_EBS = 'kafka-logs-ebs'


class Ec2Node(object):
    def __init__(self, aws: AWSResources, cluster_config: ClusterConfig, ip: str):
        self.aws = aws
        self.cluster_config = cluster_config
        self.ip = ip
        self.instance = self._get_instance_by_ip()
        _LOG.info('Searching for instance %s volumes', self.instance.instance_id)
        volumes = self.aws.ec2_client.describe_instance_attribute(InstanceId=self.instance.instance_id,
                                                                  Attribute='blockDeviceMapping')
        data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
        data_volume_id = data_volume['Ebs']['VolumeId']
        self.volume = self.aws.ec2_resource.Volume(data_volume_id)

    def get_node_availability_zone(self):
        return self.volume.availability_zone

    def is_volume_in_use(self):
        self.volume.load()
        if self.volume.state == 'in-use':
            _LOG.info('Volume %s is attached. Clearing tag:Name', self.volume)
            self.volume.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
            _LOG.info('Completed clearing tag:Name for %s', self.volume)
            return True
        return False

    def is_volume_available(self):
        self.volume.load()
        return self.volume.state == 'available'

    def detach_volume(self):
        self.volume.create_tags(Tags=[{'Key': 'Name', 'Value': KAFKA_LOGS_EBS}])
        _LOG.info('Detaching %s from %s', self.volume.id, self.instance.instance_id)
        self.aws.ec2_client.detach_volume(VolumeId=self.volume.id, Force=False)

    def terminate(self):
        _LOG.info('Terminating %s in %s', self.instance, self.cluster_config.get_cluster_name())
        alarm_name = '{}-{}-auto-recover'.format(self.cluster_name, self.instance.instance_id)
        _LOG.info('Deleting alarm %s in %s for %s', alarm_name, self.cluster_name, self.instance)
        self.aws.cloudwatch_client.delete_alarms(AlarmNames=[alarm_name])
        self.instance.terminate()

    def is_terminated(self):
        self.instance.load()
        _LOG.info('Instance state is %s. Waiting ...', self.instance.state['Name'])
        if self.instance.state['Name'] == 'terminated':
            _LOG.info('%s is successfully terminated', self.instance)
            return True
        return False

    def _get_instance_by_ip(self):
        instances = self.aws.ec2_resource.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
            {'Name': 'network-interface.addresses.private-ip-address', 'Values': [self.ip]},
            {'Name': 'tag:Name', 'Values': [self.cluster_config.get_cluster_name()]}])
        instances = list(instances)
        if not instances:
            raise Exception('Instance by ip {} not found in cluster {}'
                            .format(self.ip, self.cluster_config.get_cluster_name()))
        _LOG.info('Found %s by ip %s', instances[0], self.ip)
        return instances[0]
