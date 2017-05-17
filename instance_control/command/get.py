import logging
from subprocess import call

import boto3
from instance_control import config

_LOG = logging.getLogger('bubuku.cluster.get')


def run(cluster_name: str, cluster_config: str):
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])
    instances = list(ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']},
                                                            {'Name': 'tag:Name',
                                                             'Values': [cluster_name]}]))
    max_ip = 0
    max_id = 0
    max_ebs = 0
    for instance in instances:
        max_ip = max_ip if max_ip > len(instance.private_ip_address) else len(instance.private_ip_address)
        max_id = max_id if max_id > len(instance.instance_id) else len(instance.instance_id)
        max_ebs = max_ebs if max_ebs > len(instance.block_device_mappings[0]['Ebs']['VolumeId']) else len(
            instance.block_device_mappings[0]['Ebs']['VolumeId'])

    if len(instances) == 0:
        _LOG.info('Nothing to print')
        return

    _LOG.info('ip' + ' ' * max_ip + ' id' + ' ' * max_id + ' volume' + ' ' * max_ebs)
    for instance in instances:
        len_ip = max_ip - len(instance.private_ip_address)
        len_id = max_id - len(instance.instance_id)
        len_ebs = max_ebs - len(instance.block_device_mappings[0]['Ebs']['VolumeId'])
        _LOG.info(instance.private_ip_address + ' ' * len_ip + '   ' +
                  instance.instance_id + ' ' * len_id + '   ' +
                  instance.block_device_mappings[0]['Ebs']['VolumeId'] + ' ' * len_ebs)
