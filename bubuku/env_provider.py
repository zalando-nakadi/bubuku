import json
import logging
import uuid
from functools import partial

import boto3
import requests

from bubuku.config import Config, KafkaProperties
from bubuku.id_generator import BrokerIdGenerator
from bubuku.zookeeper import BukuExhibitor, AddressListProvider
from bubuku.zookeeper.exhibitor import ExhibitorAddressProvider

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
        self.aws_addr = '169.254.169.254'
        self.config = config
        self.ip_address = None

    def _get_document(self) -> dict:
        document = requests.get('http://{}/latest/dynamic/instance-identity/document'.format(self.aws_addr),
                                timeout=5).json()
        _LOG.info("Amazon specific information loaded from AWS: {}".format(json.dumps(document, indent=2)))
        return document

    def get_id(self) -> str:
        if not self.ip_address:
            self.ip_address = self._get_document()['privateIp']
        return self.ip_address

    def _load_instance_ips(self, lb_name: str):
        region = self._get_document()['region']

        public_ips = []

        elb = boto3.client('elb', region_name=region)
        ec2 = boto3.client('ec2', region_name=region)

        response = elb.describe_instance_health(LoadBalancerName=lb_name)

        for instance in response['InstanceStates']:
            if instance['State'] == 'InService':
                ip = ec2.describe_instances(
                    InstanceIds=[instance['InstanceId']])['Reservations'][0]['Instances'][0]['PublicIpAddress']
                if not ip:
                    raise Exception('Public ip address is not allocated for %s'.format(instance))
                public_ips.append(ip)

        _LOG.info("Ip addresses for {} are: {}".format(lb_name, public_ips))
        return public_ips

    def get_address_provider(self):
        return ExhibitorAddressProvider(partial(self._load_instance_ips, self.config.zk_stack_name))

    def create_broker_id_manager(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        return BrokerIdGenerator(zk, kafka_props)


class _LocalAddressProvider(AddressListProvider):
    def get_latest_address(self) -> (list, int):
        return ('zookeeper',), 2181


class LocalEnvProvider(EnvProvider):
    unique_id = str(uuid.uuid4())

    def get_id(self) -> str:
        return self.unique_id

    def get_address_provider(self):
        return _LocalAddressProvider()

    def create_broker_id_manager(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        return BrokerIdGenerator(zk, kafka_props)
