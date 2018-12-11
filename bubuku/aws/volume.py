import logging

from bubuku.aws import AWSResources

_LOG = logging.getLogger('bubuku.aws.volume')
KAFKA_LOGS_EBS = 'kafka-logs-ebs'


def are_volumes_attached(aws_: AWSResources):
    _LOG.info('Searching for volumes with tag %s to attach', KAFKA_LOGS_EBS)
    response = aws_.ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': [KAFKA_LOGS_EBS]}])
    volumes = [aws_.ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]

    _LOG.info('Waiting for %s to be attached', volumes)
    volumes = [v for v in volumes if clear_tag(v)]
    return len(volumes) == 0


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
