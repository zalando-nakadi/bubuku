import json
import logging
import re

import requests

_LOG = logging.getLogger('bubuku.cluster.config')
KAFKA_LOGS_EBS = 'kafka-logs-ebs'


def read_cluster_config(cluster_config_path: str):
    _LOG.info('Reading config from %s', cluster_config_path)
    with open(cluster_config_path) as data_file:
        return json.load(data_file)


def validate_config(cluster_config: dict):
    _LOG.info("Validating configuration for %s", cluster_config['cluster_name'])
    cluster_name_re = '^[a-z][a-z0-9-]*[a-z0-9]$'
    if not re.match(cluster_name_re, cluster_config['cluster_name']):
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

    _LOG.info(json.dumps(cluster_config, indent=4))
