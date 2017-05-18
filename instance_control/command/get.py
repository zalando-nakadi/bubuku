import logging

import boto3
from instance_control import config

from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.get')


class GetCommand(Command):
    def __init__(self, cluster_name: str, cluster_config_path: str):
        super().__init__(cluster_name, cluster_config_path)

    def init(self):
        config.validate_config(self.cluster_name, self.cluster_config)

    def start(self):
        ec2_resource = boto3.resource('ec2', region_name=self.cluster_config['region'])
        instances = list(ec2_resource.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
            {'Name': 'tag:Name', 'Values': [self.cluster_name]}]))

        if len(instances) == 0:
            _LOG.info('Cluster has no running instances')
            return

        max_ip = 0
        max_id = 0
        max_ebs = 0
        for instance in instances:
            max_ip = max_ip if max_ip > len(instance.private_ip_address) else len(instance.private_ip_address)
            max_id = max_id if max_id > len(instance.instance_id) else len(instance.instance_id)
            max_ebs = max_ebs if max_ebs > len(instance.block_device_mappings[0]['Ebs']['VolumeId']) else len(
                instance.block_device_mappings[0]['Ebs']['VolumeId'])

        print('ip' + ' ' * max_ip + ' id' + ' ' * max_id + ' volume' + ' ' * max_ebs)
        for instance in instances:
            len_ip = max_ip - len(instance.private_ip_address)
            len_id = max_id - len(instance.instance_id)
            len_ebs = max_ebs - len(instance.block_device_mappings[0]['Ebs']['VolumeId'])
            print(instance.private_ip_address + ' ' * len_ip + '   ' +
                  instance.instance_id + ' ' * len_id + '   ' +
                  instance.block_device_mappings[0]['Ebs']['VolumeId'] + ' ' * len_ebs)
