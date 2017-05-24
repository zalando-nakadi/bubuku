import logging

import boto3
from instance_control import config
from instance_control import volume

from instance_control.aws import ec2_node
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.attach')


class CreateCommand(Command):
    def __init__(self, cluster_config_path: str,
                 cluster_size: int,
                 availability_zone: str,
                 image_version: str):
        super().__init__(cluster_config_path)
        self.cluster_size = cluster_size
        self.availability_zone = availability_zone
        self.image_version = image_version

    def alter_config(self):
        if self.cluster_size:
            self.cluster_config['cluster_size'] = self.cluster_size
        if self.image_version:
            self.cluster_config['image_version'] = self.image_version
        self.cluster_config['create_ebs'] = True
        self.cluster_config['availability_zone'] = self.availability_zone

    def execute(self):
        ec2_node.create(self.cluster_config)
        ec2_client = boto3.client('ec2', region_name=self.cluster_config['region'])
        ec2_resource = boto3.resource('ec2', region_name=self.cluster_config['region'])
        volume.wait_volumes_attached(ec2_client, ec2_resource)
