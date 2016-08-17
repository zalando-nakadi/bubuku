import json
import logging

import requests

from bubuku.zookeeper.exhibior import AWSExhibitorAddressProvider
from bubuku.zookeeper.exhibior import LocalAddressProvider
from bubuku.config import Config
import uuid

_LOG = logging.getLogger('bubuku.amazon')


class EnvProvider(object):
    def get_own_ip(self) -> str:
        raise NotImplementedError('Not implemented')

    def get_address_provider(self, config: Config):
        raise NotImplementedError('Not implemented')

    @staticmethod
    def create_env_provider(dev_mode: bool):
        return LocalEnvProvider() if dev_mode else AmazonEnvProvider()


class AmazonEnvProvider(EnvProvider):
    NONE = object()

    def __init__(self):
        self.document = None
        self.aws_addr = '169.254.169.254'

    def _get_document(self) -> dict:
        if not self.document:
            try:
                self.document = requests.get(
                    'http://{}/latest/dynamic/instance-identity/document'.format(self.aws_addr),
                    timeout=5).json()
                _LOG.info("Amazon specific information loaded from AWS: {}".format(
                    json.dumps(self.document, indent=2)))
            except Exception as ex:
                _LOG.warn('Failed to download AWS document', exc_info=ex)
                self.document = AmazonEnvProvider.NONE
        return self.document if self.document != AmazonEnvProvider.NONE else None

    def get_aws_region(self) -> str:
        doc = self._get_document()
        return doc['region'] if doc else None

    def get_own_ip(self) -> str:
        doc = self._get_document()
        return doc['privateIp'] if doc else '127.0.0.1'

    def get_address_provider(self, config: Config):
        return AWSExhibitorAddressProvider(self, config.zk_stack_name)


class LocalEnvProvider(EnvProvider):
    unique_id = str(uuid.uuid4())

    def get_own_ip(self) -> str:
        return self.unique_id

    def get_address_provider(self, config: Config):
        return LocalAddressProvider()

