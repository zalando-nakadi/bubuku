import logging

import click

from bubuku.config import load_config, KafkaProperties, Config
from bubuku.env_provider import EnvProvider
from bubuku.features.remote_exec import RemoteCommandExecutorCheck
from bubuku.id_generator import get_broker_id_policy
from bubuku.zookeeper import load_exhibitor_proxy, BukuExhibitor

_LOG = logging.getLogger('bubuku.cli')


def __get_opt_broker_id(broker_id: str, config: Config, zk: BukuExhibitor, env_provider: EnvProvider) -> str:
    if not broker_id:
        kafka_properties = KafkaProperties(config.kafka_settings_template, '/tmp/tmp.props'.format(config.kafka_dir))
        broker_id_manager = get_broker_id_policy(config.id_policy, zk, kafka_properties, env_provider)
        broker_id = broker_id_manager.detect_broker_id()
        _LOG.info('Will use broker_id {}'.format(broker_id))
    running_brokers = zk.get_broker_ids()
    if broker_id not in running_brokers:
        raise Exception('Broker id {} is not registered ({}), can not restart'.format(broker_id, running_brokers))
    return broker_id


def __prepare_configs():
    config = load_config()
    _LOG.info('Using config: {}'.format(config))
    env_provider = EnvProvider.create_env_provider(config.development_mode)
    zookeeper = load_exhibitor_proxy(env_provider.get_address_provider(config), config.zk_prefix)
    return config, env_provider, zookeeper


logging.basicConfig(level=getattr(logging, 'INFO', None))


@click.group()
def cli():
    pass


@cli.command('restart')
@click.option('--broker', type=click.STRING,
              help='Broker id to restart. By default current broker id is restarted')
def restart_broker(broker: str):
    config, env_provider, zookeeper = __prepare_configs()
    broker_id = __get_opt_broker_id(broker, config, zookeeper, env_provider)
    RemoteCommandExecutorCheck.register_restart(zookeeper, broker_id)


@cli.command('rebalance')
@click.option('--broker', type=click.STRING,
              help="Broker instance on which to perform rebalance. By default, any free broker will start it")
def rebalance_partitions(broker: str):
    config, env_provider, zookeeper = __prepare_configs()
    broker_id = __get_opt_broker_id(broker, config, zookeeper, env_provider) if broker else None
    RemoteCommandExecutorCheck.register_rebalance(zookeeper, broker_id)


if __name__ == '__main__':
    cli()
