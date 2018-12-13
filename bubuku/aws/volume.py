import logging

from bubuku.aws import AWSResources

_LOG = logging.getLogger('bubuku.aws.volume')
KAFKA_LOGS_EBS = 'kafka-logs-ebs'


def are_volumes_attached(aws_: AWSResources):
    _LOG.info('Searching for volumes with tag %s to attach', KAFKA_LOGS_EBS)
    response = aws_.ec2_client.describe_volumes(Filters=[{'Name': 'tag:Name', 'Values': [KAFKA_LOGS_EBS]}])
    volumes = [aws_.ec2_resource.Volume(v['VolumeId']) for v in response['Volumes']]

    _LOG.info('Waiting for %s to be attached', volumes)
    volumes = [v for v in volumes if not clear_volume_tag_if_in_use(v)]
    return len(volumes) == 0


def clear_volume_tag_if_in_use(volume):
    volume.load()
    if volume.state == 'in-use':
        _LOG.info('Volume %s is attached. Clearing tag:Name', volume)
        volume.create_tags(Tags=[{'Key': 'Name', 'Value': ''}])
        _LOG.info('Completed clearing tag:Name for %s', volume)
        return True
    return False


def is_volume_available(volume):
    volume.load()
    return volume.state == 'available'


def detach_volume(aws_: AWSResources, instance):
    _LOG.info('Searching for instance %s volumes', instance.instance_id)
    volumes = aws_.ec2_client.describe_instance_attribute(InstanceId=instance.instance_id,
                                                          Attribute='blockDeviceMapping')
    data_volume = next(v for v in volumes['BlockDeviceMappings'] if v['DeviceName'] == '/dev/xvdk')
    data_volume_id = data_volume['Ebs']['VolumeId']

    _LOG.info('Creating tag:Name=%s for %s', KAFKA_LOGS_EBS, data_volume_id)
    vol = aws_.ec2_resource.Volume(data_volume_id)
    vol.create_tags(Tags=[{'Key': 'Name', 'Value': KAFKA_LOGS_EBS}])

    _LOG.info('Detaching %s from %s', data_volume_id, instance.instance_id)
    aws_.ec2_client.detach_volume(VolumeId=data_volume_id, Force=False)
    return vol
