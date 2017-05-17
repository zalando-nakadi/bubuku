import logging

import time

_LOG = logging.getLogger('bubuku.cluster.volume')


def wait_volumes_attached(ec2_client, ec2_resource):
    response = ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': ['kafka-logs-ebs']}])
    volumes = [ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]
    _LOG.info('Wait for %s volume(s) to be attached',  len(volumes))
    while True:
        volumes = [v for v in volumes if clear_tag(v)]
        if len(volumes) == 0:
            _LOG.info('Volume is attached, tag:Name is cleaned')
            return
        else:
            time.sleep(10)


def clear_tag(v):
    v.load()
    if v.state == 'in-use':
        v.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
        return False
    return True