import logging
from subprocess import call

import boto3
from instance_control import config
from instance_control import volume

from instance_control.aws import ec2_node

_LOG = logging.getLogger('bubuku.cluster.attach')


def run(cluster_name: str, volume_id: str, cluster_config: str):
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_client = boto3.client('ec2', region_name=cluster_config['region'])
    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])
    vol = ec2_resource.Volume(volume_id)
    vol.create_tags(Tags=[{'Key': 'Name', 'Value': 'kafka-logs-ebs'}])

    cluster_config['create_ebs'] = False
    cluster_config['availability_zone'] = vol.availability_zone
    _LOG.info('Launching instance in availability zone: %s', cluster_config['availability_zone'])

    ec2_node.create(cluster_config)
    volume.wait_volumes_attached(ec2_client, ec2_resource)
