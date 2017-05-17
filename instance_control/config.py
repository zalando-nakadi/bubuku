import json
import re

import logging
import requests

_LOG = logging.getLogger('bubuku.cluster.config')
KAFKA_LOGS_EBS = 'kafka-logs-ebs'


def read_cluster_config(cluster_name: str, cluster_config_path: str):
    _LOG.info('Reading config %s from %s', cluster_name, cluster_config_path)
    with open(cluster_config_path) as data_file:
        cluster_configs = json.load(data_file)
    cluster_config = cluster_configs[cluster_name]
    if cluster_config:
        return cluster_config
    else:
        raise Exception("No cluster config found for {} in {}".format(cluster_name, cluster_config_path))


def validate_config(cluster_name: str, cluster_config: dict):
    _LOG.info("Validating configuration for %s", cluster_name)
    cluster_name_re = '^[a-z][a-z0-9-]*[a-z0-9]$'
    if not re.match(cluster_name_re, cluster_name):
        raise Exception(
            'Cluster name must only contain lowercase latin letters, digits and dashes '
            '(it also must start with a letter and cannot end with a dash), in other words '
            'it must be matched by the following regular expression: {}'.format(cluster_name_re))

    if not cluster_config['region']:
        raise Exception('Please specify at least one region')

    artifact_name = 'bubuku-appliance'
    url = 'https://registry.opensource.zalan.do/teams/aruha/artifacts/{}/tags'.format(artifact_name)
    if not next((tag for tag in requests.get(url).json() if tag['name'] == cluster_config['image_version']), None):
        raise Exception('Docker image was not found')

    _LOG.info("Using cluster config: %s", cluster_name)
    for k, v in cluster_config.items():
        if k == 'environment':
            for ek, ev in cluster_config['environment'].items():
                _LOG.info('%s=%s', ek, ev)
        else:
            _LOG.info('%s=%s', k, v)

    cluster_config['cluster_name'] = cluster_name
