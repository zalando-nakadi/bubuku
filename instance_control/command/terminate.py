from subprocess import call

import boto3
import logging

from instance_control import config
from instance_control import node
from instance_control import piu

_LOG = logging.getLogger('bubuku.cluster.command.terminate')


def run(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    _LOG.info('Terminating node %s in cluster %s', ip, cluster_name)
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])

    instances = ec2_resource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running']},
        {'Name': 'network-interface.addresses.private-ip-address', 'Values': [ip]}])
    # instance = next(i for i in instances if i.private_ip_address == ip)
    _LOG.info('Found %s by ip %s', instances[0], ip)

    if not instances[0]:
        raise Exception('Instance by ip %s not found in cluster %s', ip, cluster_name)
    piu.stop_taupage(ip, user, odd)
    node.terminate(cluster_name, instances[0])
