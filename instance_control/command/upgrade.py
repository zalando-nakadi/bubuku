from subprocess import call

import boto3

from instance_control.aws import ec2_node

from instance_control import config

from instance_control.node import terminate
from instance_control import piu
from instance_control import volume


def run(cluster_name: str, image_version: str, cluster_config: str, ip: str, user: str, odd: str):
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    if image_version:
        cluster_config['image_version'] = image_version
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_client = boto3.client('ec2', region_name=cluster_config['region'])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])

    piu.stop_taupage(ip, user, odd)
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    instance = next(i for i in instances if i.private_ip_address == ip)
    # detach ebs
    volumes = ec2_client.describe_instance_attribute(InstanceId=instance.instance_id, Attribute='blockDeviceMapping')
    data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
    data_volume_id = data_volume['Ebs']['VolumeId']
    vol = ec2_resource.Volume(data_volume_id)
    vol.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])
    ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)

    terminate(cluster_name, instance)
    cluster_config['availability_zone'] = vol.availability_zone
    cluster_config['create_ebs'] = False

    ec2_node.create(cluster_config)
    volume.wait_volumes_attached(ec2_client, ec2_resource)
