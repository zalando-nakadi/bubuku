import logging

from instance_control import volume
from instance_control.aws import AWSResources
from instance_control.aws.ec2_node import EC2
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.attach')


class CreateCommand(Command):
    def __init__(self, cluster_config_path: str,
                 count: int,
                 availability_zone: str,
                 image_version: str):
        super().__init__(cluster_config_path)
        self.count = count
        self.availability_zone = availability_zone
        self.image_version = image_version

    def alter_config(self):
        if self.image_version:
            self.cluster_config['image_version'] = self.image_version
        self.cluster_config['create_ebs'] = True
        self.cluster_config['availability_zone'] = self.availability_zone

    def execute(self):
        aws_ = AWSResources(region=self.cluster_config['region'])
        ec2 = EC2(aws_)

        ec2.create(self.cluster_config, self.count)
        volume.wait_volumes_attached(aws_)
