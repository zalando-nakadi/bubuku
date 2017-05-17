import logging

import boto3

_LOG = logging.getLogger('bubuku.cluster.aws.security_group')


def create_or_ger_security_group(cluster_config: dict) -> dict:
    _LOG.info('Configuring Security Group ...')
    ec2 = boto3.client('ec2', cluster_config['region'])
    security_groups = ec2.describe_security_groups(
        Filters=[{'Name': 'group-name', 'Values': [cluster_config['cluster_name']]}])
    if security_groups['SecurityGroups']:
        return security_groups['SecurityGroups'][0]

    _LOG.info("Security Group does not exists, creating Security Group rule")
    sg_name = cluster_config['cluster_name']
    sg = ec2.create_security_group(GroupName=sg_name, VpcId=cluster_config['vpc_id'],
                                   Description='Bubuku Security Group')
    ec2.create_tags(Resources=[sg['GroupId']], Tags=[{'Key': 'Name', 'Value': sg_name}])
    ec2.authorize_security_group_ingress(GroupId=sg['GroupId'],
                                         IpPermissions=[get_ip_permission(22), get_ip_permission(8004),
                                                        get_ip_permission(8080), get_ip_permission(8778),
                                                        get_ip_permission(9100), get_ip_permission(9092),
                                                        get_ip_permission(cluster_config['environment']['health_port'])])
    return sg


def get_ip_permission(port: int):
    return {
        'IpProtocol': 'tcp',
        'FromPort': port,
        'ToPort': port,
        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
    }
