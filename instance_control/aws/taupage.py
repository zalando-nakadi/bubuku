import logging

import boto3

_LOG = logging.getLogger('bubuku.cluster.aws.taupage')


def find_amis(region: str) -> dict:
    '''
    Find latest Taupage AMI for the region
    '''
    _LOG.info('Finding latest Taupage AMI in %s..', region)

    ec2 = boto3.resource('ec2', region)
    filters = [{'Name': 'name', 'Values': ['*Taupage-AMI-*']},
               {'Name': 'is-public', 'Values': ['false']},
               {'Name': 'state', 'Values': ['available']},
               {'Name': 'root-device-type', 'Values': ['ebs']}]
    images = list(ec2.images.filter(Filters=filters))
    if not images:
        raise Exception('No Taupage AMI found')
    most_recent_image = sorted(images, key=lambda i: i.name)[-1]

    _LOG.info('The latest AMI is %s', most_recent_image)

    return most_recent_image


def generate_user_data(cluster_config: dict) -> str:
    '''
    Generate Taupage user data to start a Bubuku node
    http://docs.stups.io/en/latest/components/taupage.html
    '''
    _LOG.info('Generating Taupage user data')

    environment = cluster_config['environment']
    data = {'runtime': 'Docker',
            'source': cluster_config['docker_image'],
            'application_id': cluster_config['cluster_name'],
            'application_version': cluster_config['image_version'],
            'networking': 'host',
            'ports': {'9092': '9092',
                      '8004': '8004',
                      '8080': '8080',
                      '8778': '8778',
                      environment['health_port']: environment['health_port']},
            'enhanced_cloudwatch_metrics': True,
            'application_logrotate_size': '100M',
            'application_logrotate_rotate': 4,
            'environment': {
                'CLUSTER_NAME': cluster_config['cluster_name'],
                'ZOOKEEPER_STACK_NAME': environment['zookeeper_stack_name'],
                'ZOOKEEPER_PREFIX': environment['zookeeper_prefix'],
                'BUBUKU_MODE': environment['bubuku_mode'],
                'BUKU_FEATURES': environment['buku_features'],
                'JMX_PORT': environment['jmx_port'],
                'KAFKA_HEAP_OPTS': environment['kafka_heap_opts'],
                'HEALTH_PORT': environment['health_port'],
                'STARTUP_TIMEOUT_TYPE': environment['startup_timeout_type'],
                'STARTUP_TIMEOUT_INITIAL': environment['startup_timeout_initial'],
                'STARTUP_TIMEOUT_STEP': environment['startup_timeout_step']
            },
            'volumes': {
                'ebs': {
                    '/dev/xvdk': None
                }
            },
            'mounts': {
                '/data': {
                    'partition': '/dev/xvdk',
                    'filesystem': 'ext4',
                    'options': 'noatime,nodiratime,nobarrier'
                }
            },
            'scalyr_account_key': cluster_config['scalyr_key']
            }

    _LOG.info('Generated data:')
    for k, v in data.items():
        if k == 'environment':
            _LOG.info('--- environment variables')
            for ek, ev in environment.items():
                _LOG.info('%s=%s', ek, ev)
            _LOG.info('--- environment variables')
        else:
            _LOG.info('%s=%s', k, v)

    return data
