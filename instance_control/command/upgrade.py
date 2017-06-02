import base64
import logging

import boto3

from instance_control import config
from instance_control import node
from instance_control import piu
from instance_control import volume
from instance_control.aws import ec2_node
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.upgrade')


class UpgradeCommand(Command):
    def __init__(self, cluster_config_path: str, image_version: str, ip: str, user: str, odd: str):
        super().__init__(cluster_config_path)
        self.image_version = image_version
        self.ip = ip
        self.user = user
        self.odd = odd

    def alter_config(self):
        if self.image_version:
            self.cluster_config['image_version'] = self.image_version

    def execute(self):
        ec2_client = boto3.client('ec2', region_name=self.cluster_config['region'])
        ec2_resource = boto3.resource('ec2', region_name=self.cluster_config['region'])

        instance = node.get_instance_by_ip(ec2_resource, self.cluster_config['cluster_name'], self.ip)
        check_current_image_version(instance, self.cluster_config['image_version'])

        _LOG.info('Searching for instance %s volumes', instance.instance_id)
        volumes = ec2_client.describe_instance_attribute(InstanceId=instance.instance_id,
                                                         Attribute='blockDeviceMapping')
        data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
        data_volume_id = data_volume['Ebs']['VolumeId']

        piu.stop_taupage(self.ip, self.user, self.odd)

        _LOG.info('Creating tag:Name=%s for %s', config.KAFKA_LOGS_EBS, data_volume_id)
        vol = ec2_resource.Volume(data_volume_id)
        vol.create_tags(Tags=[{'Key': 'Name', 'Value': config.KAFKA_LOGS_EBS}])
        _LOG.info('Detaching %s from %s', data_volume_id, instance.instance_id)
        ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

        node.terminate(self.cluster_config['cluster_name'], instance)
        self.cluster_config['availability_zone'] = vol.availability_zone
        self.cluster_config['create_ebs'] = False
        self.cluster_config['cluster_size'] = 1

        ec2_node.create(self.cluster_config)
        # volumes are going to be attached by taupage
        volume.wait_volumes_attached(ec2_client, ec2_resource)


def check_current_image_version(instance, provided_image_version):
    _LOG.info('Checking running Bubuku version on %s', instance.instance_id)
    response = instance.describe_attribute(Attribute='userData')
    env = base64.b64decode(response['UserData']['Value']).decode('UTF-8')
    variables = env.split("\n")
    for v in variables:
        if v.startswith("application_version"):
            current_image_version = v.split(": ")[1]
            _LOG.info('Current running Bubuku version %s', current_image_version)
            if current_image_version == provided_image_version:
                raise Exception("Current running Bubuku version is the same as provided, stopping upgrade")
