import logging

import click

from bubuku.amazon import Amazon
from bubuku.config import load_config, KafkaProperties, Config
from bubuku.features.remote_exec import RemoteCommandExecutorCheck
from bubuku.features.swap_partitions import load_swap_data
from bubuku.id_generator import get_broker_id_policy
from bubuku.zookeeper import load_exhibitor_proxy, BukuExhibitor
from bubuku.zookeeper.exhibior import AWSExhibitorAddressProvider

_LOG = logging.getLogger('bubuku.cli')


def __get_opt_broker_id(broker_id: str, config: Config, zk: BukuExhibitor, amazon: Amazon) -> str:
    if not broker_id:
        kafka_properties = KafkaProperties(config.kafka_settings_template, '/tmp/tmp.props'.format(config.kafka_dir))
        broker_id_manager = get_broker_id_policy(config.id_policy, zk, kafka_properties, amazon)
        broker_id = broker_id_manager.get_broker_id()
        _LOG.info('Will use broker_id {}'.format(broker_id))
    running_brokers = zk.get_broker_ids()
    if broker_id not in running_brokers:
        raise Exception('Broker id {} is not registered ({}), can not restart'.format(broker_id, running_brokers))
    return broker_id


def __prepare_configs():
    config = load_config()
    _LOG.info('Using config: {}'.format(config))
    amazon = Amazon()
    zookeeper = load_exhibitor_proxy(AWSExhibitorAddressProvider(amazon, config.zk_stack_name), config.zk_prefix)
    return config, amazon, zookeeper


logging.basicConfig(level=getattr(logging, 'INFO', None))


@click.group()
def cli():
    pass


@cli.command('restart', help='Restart kafka instance')
@click.option('--broker', type=click.STRING,
              help='Broker id to restart. By default current broker id is restarted')
def restart_broker(broker: str):
    config, amazon, zookeeper = __prepare_configs()
    broker_id = __get_opt_broker_id(broker, config, zookeeper, amazon)
    RemoteCommandExecutorCheck.register_restart(zookeeper, broker_id)


@cli.command('rebalance', help='Run rebalance process on one of brokers')
@click.option('--broker', type=click.STRING,
              help="Broker instance on which to perform rebalance. By default, any free broker will start it")
def rebalance_partitions(broker: str):
    config, amazon, zookeeper = __prepare_configs()
    broker_id = __get_opt_broker_id(broker, config, zookeeper, amazon) if broker else None
    RemoteCommandExecutorCheck.register_rebalance(zookeeper, broker_id)


@cli.command('migrate', help='Replace one broker with another for all partitions')
@click.option('--from', type=click.STRING, help='List of brokers to migrate from (separated with ",")')
@click.option('--to', type=click.STRING, help='List of brokers to migrate to (separated with ",")')
@click.option('--shrink', is_flag=True, default=False, show_default=True,
              help='Whether or not to shrink replaced broker ids form partition assignment')
def migrate_broker(from_: str, to: str, shrink: bool):
    _, __, zookeeper = __prepare_configs()
    RemoteCommandExecutorCheck.register_migration(zookeeper, from_.split(','), to.split(','), shrink)


@cli.command('swap_fat_slim', help='Move one partition from fat broker to slim one')
@click.option('--threshold', type=click.INT, default="100000", show_default=True, help="Threshold in kb to run swap")
def swap_partitions(threshold: int):
    _, __, zookeeper = __prepare_configs()
    RemoteCommandExecutorCheck.register_fatboy_slim(zookeeper, threshold_kb=threshold)


@cli.command('stats', help='Display statistics about brokers')
def show_stats():
    config, amazon, zookeeper = __prepare_configs()
    slim_broker_id, fat_broker_id, calculated_gap, stats_data = load_swap_data(zookeeper, config.health_port, 0)
    print('Broker list (broker_id, ip_address, free_space_kb, used_space_kb):')
    for broker_id, data in stats_data.items():
        data_stats = data.get('disk')
        print('{}\t{}\t{}\t{}'.format(broker_id, data.get('host'), data_stats.get('free_kb'), data.get('used_kb')))
    print('Calculated gap between {}(fat) and {}(slim) is {} kb'.format(fat_broker_id, slim_broker_id, calculated_gap))


if __name__ == '__main__':
    cli()
