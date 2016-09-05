import json
import logging

import requests

from bubuku.id_generator import BrokerIDByIp, BrokerIdAutoAssign
from bubuku.zookeeper import BukuExhibitor
from bubuku.zookeeper.exhibitor import AWSExhibitorAddressProvider
from bubuku.zookeeper.exhibitor import LocalAddressProvider
from bubuku.config import Config, KafkaProperties
import uuid

_LOG = logging.getLogger('bubuku.amazon')


class EnvProvider(object):
    def get_id(self) -> str:
        raise NotImplementedError('Not implemented')

    def get_address_provider(self):
        raise NotImplementedError('Not implemented')

    def create_broker_id_manager(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        raise NotImplementedError('Not implemented')

    @staticmethod
    def create_env_provider(config: Config):
        if config.mode == 'amazon':
            return AmazonEnvProvider(config)
        elif config.mode == 'local':
            return LocalEnvProvider()
        else:
            raise NotImplementedError('Configuration mode "{}" is not supported'.format(config.mode))


class AmazonEnvProvider(EnvProvider):
    def __init__(self, config: Config):
        self.document = None
        self.aws_addr = '169.254.169.254'
        self.config = config

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
        return self.document

    def get_aws_region(self) -> str:
        doc = self._get_document()
        return doc['region'] if doc else None

    def get_id(self) -> str:
        doc = self._get_document()
        return doc['privateIp'] if doc else '127.0.0.1'

    def get_address_provider(self):
        return AWSExhibitorAddressProvider(self.config.zk_stack_name, self.get_aws_region())

    def create_broker_id_manager(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        return BrokerIDByIp(zk, self.get_id(), kafka_props)


class LocalEnvProvider(EnvProvider):
    unique_id = str(uuid.uuid4())

    def get_id(self) -> str:
        return self.unique_id

    def get_address_provider(self):
        return LocalAddressProvider()

    def create_broker_id_manager(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        return BrokerIdAutoAssign(zk, kafka_props)

