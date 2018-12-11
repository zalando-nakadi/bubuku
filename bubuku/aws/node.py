import logging

from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig

_LOG = logging.getLogger('bubuku.aws.node')


def terminate(aws_: AWSResources, cluster_config: ClusterConfig, instance):
    _LOG.info('Terminating %s in %s', instance, cluster_config.get_cluster_name())
    _delete_alarm(aws_, cluster_config.get_cluster_name(), instance)
    instance.terminate()


def is_terminated(instance):
    instance.load()
    _LOG.info('Instance state is %s. Waiting ...', instance.state['Name'])
    if instance.state['Name'] == 'terminated':
        _LOG.info('%s is successfully terminated', instance)
        return True
    return False


def _delete_alarm(aws_: AWSResources, cluster_name: str, instance):
    alarm_name = '{}-{}-auto-recover'.format(cluster_name, instance.instance_id)
    _LOG.info('Deleting alarm %s in %s for %s', alarm_name, cluster_name, instance)
    aws_.cloudwatch_client.delete_alarms(AlarmNames=[alarm_name])


def get_instance_by_ip(ec2_resource, cluster_config: ClusterConfig, ip):
    instances = ec2_resource.instances.filter(Filters=[
        {'Name': 'instance-state-name', 'Values': ['running', 'pending']},
        {'Name': 'network-interface.addresses.private-ip-address', 'Values': [ip]},
        {'Name': 'tag:Name', 'Values': [cluster_config.get_cluster_name()]}])
    instances = list(instances)
    if not instances:
        raise Exception('Instance by ip {} not found in cluster {}'.format(ip, cluster_config.get_cluster_name()))
    _LOG.info('Found %s by ip %s', instances[0], ip)
    return instances[0]
