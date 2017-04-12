#!/usr/bin/env python3
"""General Kafka Start Script."""

import logging

from bubuku import health
from bubuku.broker import BrokerManager, KafkaProcessHolder, StartupTimeout
from bubuku.config import load_config, KafkaProperties, Config
from bubuku.controller import Controller
from bubuku.env_provider import EnvProvider
from bubuku.features.data_size_stats import GenerateDataSizeStatistics
from bubuku.features.rebalance.check import RebalanceOnStartCheck, RebalanceOnBrokerListCheck
from bubuku.features.remote_exec import RemoteCommandExecutorCheck
from bubuku.features.restart_if_dead import CheckBrokerStopped
from bubuku.features.restart_on_zk_change import CheckExhibitorAddressChanged, RestartBrokerChange
from bubuku.features.swap_partitions import CheckBrokersDiskImbalance
from bubuku.features.terminate import register_terminate_on_interrupt
from bubuku.utils import CmdHelper
from bubuku.zookeeper import BukuExhibitor, load_exhibitor_proxy

_LOG = logging.getLogger('bubuku.main')


def apply_features(api_port, features: dict, controller: Controller, buku_proxy: BukuExhibitor, broker: BrokerManager,
                   kafka_properties: KafkaProperties, env_provider: EnvProvider) -> list:
    for feature, config in features.items():
        if feature == 'restart_on_exhibitor':
            controller.add_check(CheckExhibitorAddressChanged(buku_proxy, broker))
        elif feature == 'rebalance_on_start':
            controller.add_check(RebalanceOnStartCheck(buku_proxy, broker))
        elif feature == 'rebalance_on_brokers_change':
            controller.add_check(RebalanceOnBrokerListCheck(buku_proxy, broker))
        elif feature == 'balance_data_size':
            controller.add_check(
                CheckBrokersDiskImbalance(buku_proxy, broker, config["diff_threshold_mb"] * 1024, api_port))
        elif feature == 'graceful_terminate':
            register_terminate_on_interrupt(controller, broker)
        elif feature == 'use_ip_address':
            kafka_properties.set_property('advertised.host.name', env_provider.get_id())
        else:
            _LOG.error('Using of unsupported feature "{}", skipping it'.format(feature))


def run_daemon_loop(config: Config, process_holder: KafkaProcessHolder, cmd_helper: CmdHelper, restart_on_init: bool):
    _LOG.info("Using configuration: {}".format(config))
    kafka_props = KafkaProperties(config.kafka_settings_template,
                                  '{}/config/server.properties'.format(config.kafka_dir))

    env_provider = EnvProvider.create_env_provider(config)
    address_provider = env_provider.get_address_provider()
    startup_timeout = StartupTimeout.build(config.timeout)

    _LOG.info("Loading exhibitor configuration")
    with load_exhibitor_proxy(address_provider, config.zk_prefix) as zookeeper:
        _LOG.info("Loading broker_id policy")
        broker_id_manager = env_provider.create_broker_id_manager(zookeeper, kafka_props)

        _LOG.info("Building broker manager")
        broker = BrokerManager(process_holder, config.kafka_dir, zookeeper, broker_id_manager, kafka_props,
                               startup_timeout)

        _LOG.info("Creating controller")
        controller = Controller(broker, zookeeper, env_provider)

        controller.add_check(CheckBrokerStopped(broker, zookeeper))
        controller.add_check(RemoteCommandExecutorCheck(zookeeper, broker, config.health_port))
        controller.add_check(GenerateDataSizeStatistics(zookeeper, broker, cmd_helper,
                                                        kafka_props.get_property("log.dirs").split(",")))
        apply_features(config.health_port, config.features, controller, zookeeper, broker, kafka_props, env_provider)

        _LOG.info('Starting main controller loop')
        controller.loop(RestartBrokerChange(zookeeper, broker, lambda: False) if restart_on_init else None)


def main():
    logging.basicConfig(level=getattr(logging, 'INFO', None))

    config = load_config()
    _LOG.info("Using configuration: {}".format(config))
    process_holder = KafkaProcessHolder()
    _LOG.info('Starting health server')
    cmd_helper = CmdHelper()
    health.start_server(config.health_port, cmd_helper)
    restart_on_init = False
    while True:
        try:
            run_daemon_loop(config, process_holder, cmd_helper, restart_on_init)
            break
        except Exception as ex:
            _LOG.error("WOW! Almost died! Will try to restart from the begin. "
                       "After initialization will be complete, will try to restart", exc_info=ex)
            if process_holder.get():
                restart_on_init = False


if __name__ == '__main__':
    main()
