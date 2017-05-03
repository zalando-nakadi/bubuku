#!/usr/bin/env python3
import functools
import logging
import os
import re
from time import sleep, time

from bubuku.config import KafkaProperties
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.id_generator')


class BrokerIdGenerator(object):
    def get_broker_id(self) -> str:
        raise NotImplementedError('Not implemented')

    def detect_broker_id(self):
        raise NotImplementedError('Not implemented')

    def wait_for_broker_id_absence(self):
        while self.is_registered():
            sleep(1)

    def wait_for_broker_id_presence(self, timeout) -> bool:
        start = time()
        while not self.is_registered():
            if (time() - start) > timeout:
                return False
            sleep(1)
        return True

    def is_registered(self):
        raise NotImplementedError('Not implemented')


def _create_rfc1918_address_hash(ip: str) -> (str, str):
    address = [int(v) for v in ip.split('.')]
    # the goal of this hashing is to get positive 4-bytes int which can not be changed during restarts
    if address[0] == 10:
        address[0] = 1
    elif address[0] == 192 and address[1] == 168:
        address[0] = 2
    elif address[0] == 172 and (address[1] & 0xF0) == 16:
        address[0] = 3
    else:
        return None
    return str(functools.reduce(lambda o, v: o * 256 + v, address, 0)), str(256 * 256 * 256 * 4 + 1)


class BrokerIDByIp(BrokerIdGenerator):
    def __init__(self, zk: BukuExhibitor, ip: str, kafka_props: KafkaProperties):
        self.zk = zk
        self.broker_id = _read_broker_id_from_meta_properties(kafka_props)
        if self.broker_id is None:
            self.broker_id, max_id = _create_rfc1918_address_hash(ip)
            _LOG.info('Built broker id {} from ip: {}'.format(self.broker_id, ip))
        else:
            max_id = str(256 * 256 * 256 * 4 + 1)
            _LOG.info('Using existing broker id {}'.format(self.broker_id))
        kafka_props.set_property('reserved.broker.max.id', max_id)

        if self.broker_id is None:
            raise NotImplementedError('Broker id from ip address supported only for rfc1918 private addresses')

    def get_broker_id(self):
        return self.broker_id

    def detect_broker_id(self):
        return self.broker_id

    def is_registered(self):
        return self.zk.is_broker_registered(self.broker_id)


class BrokerIdAutoAssign(BrokerIdGenerator):
    def __init__(self, zk: BukuExhibitor, kafka_props: KafkaProperties):
        super().__init__()
        self.zk = zk
        self.kafka_props = kafka_props
        self.broker_id = None

    def get_broker_id(self):
        return None

    def detect_broker_id(self):
        return _read_broker_id_from_meta_properties(self.kafka_props)

    def is_registered(self):
        broker_id = self.detect_broker_id()
        if broker_id:
            return self.zk.is_broker_registered(broker_id)
        return False


def _read_broker_id_from_meta_properties(kafka_props: KafkaProperties):
    meta_path = '{}/meta.properties'.format(kafka_props.get_property('log.dirs'))
    while not os.path.isfile(meta_path):
        return None
    with open(meta_path) as f:
        lines = f.readlines()
        for line in lines:
            match = re.search('broker\.id=(\d+)', line)
            if match:
                return match.group(1)
    return None
