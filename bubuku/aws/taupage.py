import logging

_LOG = logging.getLogger('bubuku.cluster.aws.taupage')


def find_amis(ec2_resource, region: str) -> dict:
    '''
    Find latest Taupage AMI for the region
    '''
    _LOG.info('Finding latest Taupage AMI in %s..', region)

    filters = [{'Name': 'name', 'Values': ['*Taupage-AMI-*']},
               {'Name': 'is-public', 'Values': ['false']},
               {'Name': 'state', 'Values': ['available']},
               {'Name': 'root-device-type', 'Values': ['ebs']}]
    images = list(ec2_resource.images.filter(Filters=filters))
    if not images:
        raise Exception('No Taupage AMI found')
    most_recent_image = sorted(images, key=lambda i: i.name)[-1]

    _LOG.info('The latest AMI is %s', most_recent_image)

    return most_recent_image
