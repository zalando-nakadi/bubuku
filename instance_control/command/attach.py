import logging

from instance_control import config
from instance_control import volume
from instance_control.aws import AWSResources
from instance_control.aws.ec2_node import EC2
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.attach')


class AttachCommand(Command):
    def __init__(self, cluster_config_path: str, volume_id: str):
        super().__init__(cluster_config_path)
        self.volume_id = volume_id

    def execute(self):
        aws_ = AWSResources(region=self.cluster_config['region'])

        vol = aws_.ec2_resource.Volume(self.volume_id)
        volume.check_volume_available(vol)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': config.KAFKA_LOGS_EBS}])
        _LOG.info('Created tag:Name=%s for %s', config.KAFKA_LOGS_EBS, self.volume_id)

        self.cluster_config['create_ebs'] = False
        self.cluster_config['availability_zone'] = vol.availability_zone

        ec2 = EC2(aws_)
        ec2.create(self.cluster_config, 1)
        volume.wait_volumes_attached(aws_)
