import logging

import boto3

from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.get')


class GetCommand(Command):
    def __init__(self, cluster_name: str, cluster_config_path: str):
        super().__init__(cluster_name, cluster_config_path)

    def execute(self):
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

        print('ip{:<ip} id{:<id} volume{:<volume}'.format(ip=max_ip, id=max_id, volume=max_ebs))
        # print('ip' + ' ' * max_ip + ' id' + ' ' * max_id + ' volume' + ' ' * max_ebs)
        for instance in instances:
            len_ip = max_ip - len(instance.private_ip_address)
            len_id = max_id - len(instance.instance_id)
            len_ebs = max_ebs - len(instance.block_device_mappings[0]['Ebs']['VolumeId'])
            print('{ip: <{len_ip}}   {id: <{len_id}}   {volume: <{len_ebs}}'.format(
                id=instance.private_ip_address, len_ip=len_ip,
                ip=instance.instance_id, len_id=len_id,
                volume=instance.block_device_mappings[0]['Ebs']['VolumeId'], len_ebs=len_ebs))
            # print(instance.private_ip_address + ' ' * len_ip + '   ' +
            #       instance.instance_id + ' ' * len_id + '   ' +
            #       instance.block_device_mappings[0]['Ebs']['VolumeId'] + ' ' * len_ebs)
