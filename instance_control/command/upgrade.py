import base64
import logging

from instance_control import config
from instance_control import node
from instance_control import piu
from instance_control import volume
from instance_control.aws import AWSResources
from instance_control.aws.ec2_node import EC2
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.upgrade')


class UpgradeCommand(Command):
    def __init__(self, cluster_config_path: str, image_version: str, ip: str, user: str, odd: str, force: bool):
        super().__init__(cluster_config_path)
        self.image_version = image_version
        self.ip = ip
        self.user = user
        self.odd = odd
        self.force = force

    def alter_config(self):
        if self.image_version:
            self.cluster_config['image_version'] = self.image_version

    def execute(self):
        aws_ = AWSResources(region=self.cluster_config['region'])

        instance = node.get_instance_by_ip(aws_.ec2_resource, self.cluster_config['cluster_name'], self.ip)
        if not self.force and self.cluster_config['image_version'] == get_image_version(instance):
            raise Exception("Current running Bubuku version is the same as provided ({}), stopping upgrade".format(
                self.cluster_config['image_version']))

        _LOG.info('Searching for instance %s volumes', instance.instance_id)
        volumes = aws_.ec2_client.describe_instance_attribute(InstanceId=instance.instance_id,
                                                              Attribute='blockDeviceMapping')
        data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
        data_volume_id = data_volume['Ebs']['VolumeId']

        piu.stop_taupage(self.ip, self.user, self.odd, self.cluster_config['region'])

        _LOG.info('Creating tag:Name=%s for %s', config.KAFKA_LOGS_EBS, data_volume_id)
        vol = aws_.ec2_resource.Volume(data_volume_id)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': config.KAFKA_LOGS_EBS}])
        _LOG.info('Detaching %s from %s', data_volume_id, instance.instance_id)

        aws_.ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

        node.terminate(aws_, self.cluster_config['cluster_name'], instance)
        self.cluster_config['availability_zone'] = vol.availability_zone
        self.cluster_config['create_ebs'] = False

        ec2 = EC2(aws_)

        ec2.create(self.cluster_config, 1)
        # volumes are going to be attached by taupage
        volume.wait_volumes_attached(aws_)


def get_image_version(instance):
    response = instance.describe_attribute(Attribute='userData')
    env = base64.b64decode(response['UserData']['Value']).decode('UTF-8')
    variables = env.split("\n")
    for v in variables:
        if v.startswith("application_version"):
            return v.split(": ", 1)[1]


def check_current_image_version(instance, provided_image_version) -> bool:
    _LOG.info('Checking running Bubuku version on %s', instance.instance_id)
    running_version = get_image_version(instance)
    return running_version == provided_image_version
