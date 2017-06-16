import logging
import time

from instance_control import config

_LOG = logging.getLogger('bubuku.cluster.volume')


def wait_volumes_attached(ec2_client, ec2_resource):
    _LOG.info('Searching for volumes with tag %s to attach', config.KAFKA_LOGS_EBS)
    response = ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': [config.KAFKA_LOGS_EBS]}])
    volumes = [ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]

    _LOG.info('Waiting for %s to be attached', volumes)
    while True:
        volumes = [v for v in volumes if clear_tag(v)]
        if len(volumes) == 0:
            _LOG.info('All volumes are attached')
            return
        else:
            _LOG.info('Waiting 10 secs more for %s to be attached', volumes)
            time.sleep(10)


def clear_tag(v):
    v.load()
    if v.state == 'in-use':
        _LOG.info('Volume %s is attached. Clearing tag:Name', v)
        v.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
        _LOG.info('Completed clearing tag:Name for %s', v)
        return False
    return True


def check_volume_available(v):
    v.load()
    if v.state != 'available':
        _LOG.info('Volume %s is attached. Clearing tag:Name', v)
        raise Exception('Volume {} is not available for attaching. State: {}'.format(v.VolumeId, v.state))

