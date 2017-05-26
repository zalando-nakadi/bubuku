import logging

import boto3
from instance_control import config
from instance_control import volume

from instance_control.aws import ec2_node
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.attach')


class AttachCommand(Command):
    def __init__(self, cluster_config_path: str, volume_id: str):
        super().__init__(cluster_config_path)
        self.volume_id = volume_id

    def execute(self):
        ec2_client = boto3.client('ec2', region_name=self.cluster_config['region'])
        ec2_resource = boto3.resource('ec2', region_name=self.cluster_config['region'])
        vol = ec2_resource.Volume(self.volume_id)
        volume.check_volume_available(vol)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': config.KAFKA_LOGS_EBS}])
        _LOG.info('Created tag:Name=%s for %s', config.KAFKA_LOGS_EBS, self.volume_id)

        self.cluster_config['create_ebs'] = False
        self.cluster_config['availability_zone'] = vol.availability_zone
        self.cluster_config['cluster_size'] = 1

        ec2_node.create(self.cluster_config)
        volume.wait_volumes_attached(ec2_client, ec2_resource)
