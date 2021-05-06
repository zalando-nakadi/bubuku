#!/usr/bin/env python3
import logging
import os
import re
from typing import Optional, List

from bubuku.config import KafkaProperties
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.id_generator')


def _search_broker_id(lines: List[str]) -> Optional[str]:
    for line in lines:
        match = re.search('^broker\\.id=(\\d+)$', line.strip())
        if match:
            return match.group(1)


class BrokerIdExtractor(object):
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
            self.broker_id = _search_broker_id(f.readlines())
        return self.broker_id

    def is_registered(self):
        broker_id = self.get_broker_id()
        if broker_id:
            return self.zk.is_broker_registered(broker_id)
        return False
