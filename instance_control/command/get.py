import logging

import boto3

from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.get')


class GetCommand(Command):
    def __init__(self, cluster_config_path: str):
        super().__init__(cluster_config_path)

    def execute(self):
        ec2_resource = boto3.resource('ec2', region_name=self.cluster_config['region'])
        instances = list(ec2_resource.instances.filter(Filters=[
            {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
            {'Name': 'tag:Name', 'Values': [self.cluster_config['cluster_name']]}]))

        if len(instances) == 0:
            _LOG.info('Cluster has no running instances')
            return

        max_ip = max([len(instance.private_ip_address) for instance in instances])
        max_id = max([len(instance.instance_id) for instance in instances])

        pattern = '{0:' + str(max_ip) + '}   {1:' + str(max_id) + '}   {2}'
        print(pattern.format('ip', 'id', 'volume'))
        for instance in instances:
            print(pattern.format(instance.private_ip_address,
                                 instance.instance_id,
                                 [x['Ebs']['VolumeId'] for x in instance.block_device_mappings if
                                  x['DeviceName'] == '/dev/xvdk'][0]))
