from subprocess import call

import boto3
from instance_control import config
from instance_control import volume

from instance_control.aws import ec2_node


def run(cluster_name: str, cluster_size: int, availability_zone: str, image_version: str, cluster_config: str):
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    if cluster_size:
        cluster_config['cluster_size'] = cluster_size
    if image_version:
        cluster_config['image_version'] = image_version
    cluster_config['create_ebs'] = True
    cluster_config['availability_zone'] = availability_zone
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_node.create(cluster_config)
    ec2_client = boto3.client('ec2', region_name=cluster_config['region'])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])
    volume.wait_volumes_attached(ec2_client, ec2_resource)
