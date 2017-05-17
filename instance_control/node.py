import logging
import time

import boto3

_LOG = logging.getLogger('bubuku.cluster.instance')


def terminate(cluster_name: str, instance):
    _delete_alarm(cluster_name, instance)
    instance.terminate()

    _LOG.info('Instance is %s. Waiting ...', instance.state['Name'])
    while True:
        instance.load()
        if instance.state['Name'] == 'terminated':
            _LOG.info('Instance is terminated')
            return
        time.sleep(10)


def _delete_alarm(cluster_name: str, instance):
    cw_client = boto3.client('cloudwatch')
    cw_client.delete_alarms(
        AlarmNames=['{}-{}-auto-recover'.format(cluster_name, instance.instance_id)])
