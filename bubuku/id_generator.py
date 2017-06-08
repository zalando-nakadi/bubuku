#!/usr/bin/env python3
import logging
import re
from time import time, sleep

import os

from bubuku.config import KafkaProperties
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.id_generator')


class BrokerIdGenerator(object):
    def __init__(self, zk: BukuExhibitor, kafka_properties: KafkaProperties):
        super().__init__()
        self.zk = zk
        self.kafka_properties = kafka_properties
        self.broker_id = None

    def get_broker_id(self):
        if self.broker_id:
            return self.broker_id

        meta_path = '{}/meta.properties'.format(self.kafka_properties.get_property('log.dirs'))
        while not os.path.isfile(meta_path):
            return None
        with open(meta_path) as f:
            lines = f.readlines()
            for line in lines:
                match = re.search('broker\.id=(\d+)', line)
                if match:
                    self.broker_id = match.group(1)
                    return self.broker_id
        return None

    def is_registered(self):
        broker_id = self.get_broker_id()
        if broker_id:
            return self.zk.is_broker_registered(broker_id)
        return False

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
