from subprocess import call

import boto3

from instance_control import config
from instance_control import node
from instance_control import piu


def run(cluster_name: str, cluster_config: str, ip: str, user: str, odd: str):
    cluster_config = config.read_cluster_config(cluster_name, cluster_config)
    config.validate_config(cluster_name, cluster_config)
    call(["zaws", "login", cluster_config['account']])

    ec2_resource = boto3.resource('ec2', region_name=cluster_config['region'])

    piu.stop_taupage(ip, user, odd)
    instances = ec2_resource.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    instance = next(i for i in instances if i.private_ip_address == ip)
    node.terminate(cluster_name, instance)
