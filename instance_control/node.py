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
            _LOG.info('%s is terminated', instance)
            return
        _LOG.info('Instance state is %s. Waiting 10 secs more ...', instance.state['Name'])
        time.sleep(10)
    _LOG.info('%s is successfully terminated', instance)


def _delete_alarm(cluster_name: str, instance):
    alarm_name = '{}-{}-auto-recover'.format(cluster_name, instance.instance_id)
    _LOG.info('Deleting alarm %s in %s for %s', alarm_name, cluster_name, instance)
    cw_client = boto3.client('cloudwatch')
    cw_client.delete_alarms(
        AlarmNames=[alarm_name])
