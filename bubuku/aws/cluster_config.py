import logging

import requests
import yaml

_ARTIFACT_NAME = 'bubuku-appliance'

_LOG = logging.getLogger('bubuku.aws.cluster_config')


class ConfigLoader(object):
    def load_user_data(self) -> dict:
        pass

    def load_region(self) -> str:
        pass

    def load_ami_id(self) -> str:
        pass


class AwsInstanceUserDataLoader(ConfigLoader):
    def load_user_data(self):
        return yaml.load(requests.get('http://169.254.169.254/latest/user-data').text)

    def load_region(self) -> str:
        return requests.get('http://169.254.169.254/latest/meta-data/placement/region').text

    def load_ami_id(self) -> str:
        return requests.get('http://169.254.169.254/latest/meta-data/ami-id').text


class ClusterConfig(object):

    def __init__(self, config_loader: ConfigLoader):
        self._user_data = config_loader.load_user_data()
        self._env_vars = self._user_data.get('environment')
        self._aws_region = config_loader.load_region()
        self._ami_id = config_loader.load_ami_id()
        self._overrides = {}

    def get_cluster_name(self):
        return self._env_vars.get('CLUSTER_NAME')

    def get_aws_region(self):
        return self._aws_region

    def get_instance_type(self):
        return self._user_data.get('instance_type')

    def get_ami_id(self):
        return self._ami_id

    def get_vpc_id(self):
        return self._user_data.get('vpc_id')

    def get_tags(self):
        return self._user_data.get('tags', [])

    def get_user_data(self):
        return dict(self._user_data)

    def get_overrides(self):
        return self._overrides

    def set_overrides(self, **overrides):
        self._overrides = overrides

        if overrides.get('application_version'):
            self._user_data['application_version'] = overrides['application_version']
            self._user_data['source'] = '{}:{}'.format(
                self._user_data['source'].split(':', 1)[0], overrides['application_version'])
        if overrides.get('instance_type'):
            self._user_data['instance_type'] = overrides['instance_type']
        if overrides.get('scalyr_account_key'):
            self._user_data['scalyr_account_key'] = overrides['scalyr_account_key']
        if overrides.get('scalyr_region'):
            self._user_data['scalyr_region'] = overrides['scalyr_region']
        if overrides.get('kms_key_id'):
            self._user_data['kms_key_id'] = overrides['kms_key_id']
        if overrides.get('ami_id'):
            self._ami_id = overrides['ami_id']

        for k, v in overrides.items():
            if k not in ('application_version', 'instance_type', 'scalyr_account_key', 'scalyr_region', 'kms_key_id'):
                _LOG.warning("Unsupported argument %s with value %s", k, v)

    @staticmethod
    def create_overrides_dict(
            application_version: str = None,
            instance_type: str = None,
            scalyr_account_key: str = None,
            scalyr_region: str = None,
            kms_key_id: str = None,
            ami_id: str = None):

        # Pack arguments by the name passed to the method, in case if value for the argument is set
        def _filter_out_empty(**kwargs) -> dict:
            return {k: v for k, v in kwargs.items() if v}

        return _filter_out_empty(
            application_version=application_version,
            instance_type=instance_type,
            scalyr_account_key=scalyr_account_key,
            scalyr_region=scalyr_region,
            kms_key_id=kms_key_id,
            ami_id=ami_id
        )
