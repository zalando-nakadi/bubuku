from subprocess import call

import boto3
import logging

from instance_control.aws import ec2_node

from instance_control import config

from instance_control import node
from instance_control import piu
from instance_control import volume

_LOG = logging.getLogger('bubuku.cluster.command.upgrade')


def run(cluster_name: str, image_version: str, cluster_config: str, ip: str, user: str, odd: str):
    _LOG.info('Upgrading cluster %s', cluster_name)
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    if image_version:
        cluster_config['image_version'] = image_version
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_client = boto3.client('ec2', region_name=cluster_config['region'])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])

    instances = ec2_resource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running']},
        {'Name': 'network-interface.addresses.private-ip-address', 'Values': [ip]}])
    _LOG.info('Found %s by ip %s', instances[0], ip)

    if not instances[0]:
        raise Exception('Instance by ip %s not found in cluster %s', ip, cluster_name)
    piu.stop_taupage(ip, user, odd)

    _LOG.info('Searching for %s volumes', instances[0])
    volumes = ec2_client.describe_instance_attribute(InstanceId=instances[0].instance_id,
                                                     Attribute='blockDeviceMapping')
    data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
    data_volume_id = data_volume['Ebs']['VolumeId']

    _LOG.info('Creating tag:Name=%s for %s', config.KAFKA_LOGS_EBS, data_volume_id)
    vol = ec2_resource.Volume(data_volume_id)
    vol.create_tags(Tags=[{'Key': 'Name', 'Value': config.KAFKA_LOGS_EBS}])
    _LOG.info('Detaching %s from %s', data_volume_id, instances[0])
    ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

    node.terminate(cluster_name, instances[0])
    cluster_config['availability_zone'] = vol.availability_zone
    cluster_config['create_ebs'] = False

    ec2_node.create(cluster_config)
    volume.wait_volumes_attached(ec2_client, ec2_resource)
