import logging
import time

import boto3

_LOG = logging.getLogger('bubuku.cluster.node')


def terminate(cluster_name: str, instance):
    _LOG.info('Terminating %s in %s', instance, cluster_name)
    _delete_alarm(cluster_name, instance)
    instance.terminate()

    _LOG.info('Instance state is %s. Waiting ...', instance.state['Name'])
    while True:
        instance.load()
        if instance.state['Name'] == 'terminated':
            _LOG.info('%s is successfully terminated', instance)
            return
        _LOG.info('Instance state is %s. Waiting 10 secs more ...', instance.state['Name'])
        time.sleep(10)


def _delete_alarm(cluster_name: str, instance):
    alarm_name = '{}-{}-auto-recover'.format(cluster_name, instance.instance_id)
    _LOG.info('Deleting alarm %s in %s for %s', alarm_name, cluster_name, instance)
    cw_client = boto3.client('cloudwatch')
    cw_client.delete_alarms(
        AlarmNames=[alarm_name])


def get_instance_by_ip(ec2_resource, cluster_name, ip):
    instances = ec2_resource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
        {'Name': 'network-interface.addresses.private-ip-address', 'Values': [ip]},
        {'Name': 'tag:Name', 'Values': [cluster_name]}])
    instances = list(instances)
    if not instances:
        raise Exception('Instance by ip {} not found in cluster {}'.format(ip, cluster_name))
    _LOG.info('Found %s by ip %s', instances[0], ip)
    return instances[0]
