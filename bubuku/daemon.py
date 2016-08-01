#!/usr/bin/env python3
"""General Kafka Start Script."""

import logging

from bubuku import health
from bubuku.amazon import Amazon
from bubuku.broker import BrokerManager
from bubuku.config import load_config, KafkaProperties
from bubuku.controller import Controller
from bubuku.features.rebalance import RebalanceOnStartCheck, RebalanceOnBrokerListChange
from bubuku.features.restart_if_dead import CheckBrokerStopped
from bubuku.features.restart_on_zk_change import CheckExhibitorAddressChanged
from bubuku.features.terminate import register_terminate_on_interrupt
from bubuku.id_generator import get_broker_id_policy
from bubuku.zookeeper import load_exhibitor, Exhibitor

_LOG = logging.getLogger('bubuku.main')


def apply_features(features: str, controller: Controller, exhibitor: Exhibitor, broker: BrokerManager,
                   kafka_properties: KafkaProperties, amazon: Amazon) -> list:
    for feature in set(features.split(',')):
        if feature == 'restart_on_exhibitor':
            controller.add_check(CheckExhibitorAddressChanged(exhibitor, broker))
        elif feature == 'rebalance_on_start':
            controller.add_check(RebalanceOnStartCheck(exhibitor, broker))
        elif feature == 'rebalance_on_brokers_change':
            controller.add_check(RebalanceOnBrokerListChange(exhibitor, broker))
        elif feature == 'graceful_terminate':
            register_terminate_on_interrupt(controller, broker)
        elif feature == 'use_ip_address':
            kafka_properties.set_property('advertised.host.name', amazon.get_own_ip())
        else:
            _LOG.error('Using of unsupported feature "{}", skipping it'.format(feature))


def main():
    logging.basicConfig(level=getattr(logging, 'INFO', None))

    config = load_config()
    _LOG.info("Using configuration: {}".format(config))
    kafka_properties = KafkaProperties(config.kafka_settings_template,
                                       '{}/config/server.properties'.format(config.kafka_dir))

    amazon = Amazon()

    _LOG.info("Loading exhibitor configuration")
    exhibitor = load_exhibitor(amazon.get_addresses_by_lb_name(config.zk_stack_name), config.zk_prefix)

    _LOG.info("Loading broker_id policy")
    broker_id_manager = get_broker_id_policy(config.id_policy, exhibitor, kafka_properties, amazon)

    _LOG.info("Building broker manager")
    broker = BrokerManager(config.kafka_dir, exhibitor, broker_id_manager, kafka_properties)

    _LOG.info("Creating controller")
    controller = Controller(broker, exhibitor, amazon)

    controller.add_check(CheckBrokerStopped(broker, exhibitor))

    apply_features(config.features, controller, exhibitor, broker, kafka_properties, amazon)

    _LOG.info('Starting health server')
    health.start_server(config.health_port)

    _LOG.info('Starting main controller loop')
    controller.loop()


if __name__ == '__main__':
    main()
