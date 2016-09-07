import logging

import click

from bubuku.config import load_config, KafkaProperties, Config
from bubuku.env_provider import EnvProvider
from bubuku.features.remote_exec import RemoteCommandExecutorCheck
from bubuku.zookeeper import load_exhibitor_proxy, BukuExhibitor

_LOG = logging.getLogger('bubuku.cli')


def _print_table(table: list, print_function=None):
    if not print_function:
        print_function = print
    names = sorted(set([v for v in sum([list(k.keys()) for k in table], [])]))
    lengths = {n: len(n) for n in names}
    for d in table:
        for k, v in d.items():
            if lengths[k] < len(str(v)):
                lengths[k] = len(str(v))
    format_string = '  '.join(['{!s:' + str(lengths[n]) + 's}' for n in names])
    print_function(format_string.format(*names))
    for item in table:
        print_function(format_string.format(*[item.get(n, '') for n in names]))


def __validate_not_empty(ctx, param, value):
    if not value:
        raise click.BadParameter('Parameter must have value')


def __get_opt_broker_id(broker_id: str, config: Config, zk: BukuExhibitor, env_provider: EnvProvider) -> str:
    if not broker_id:
        kafka_properties = KafkaProperties(config.kafka_settings_template, '/tmp/tmp.props'.format(config.kafka_dir))
        broker_id_manager = env_provider.create_broker_id_manager(zk, kafka_properties)
        broker_id = broker_id_manager.detect_broker_id()
        _LOG.info('Will use broker_id {}'.format(broker_id))
    running_brokers = zk.get_broker_ids()
    if broker_id not in running_brokers:
        raise Exception('Broker id {} is not registered ({}), can not restart'.format(broker_id, running_brokers))
    return broker_id


def __prepare_configs():
    config = load_config()
    _LOG.info('Using config: {}'.format(config))
    env_provider = EnvProvider.create_env_provider(config)
    return config, env_provider


logging.basicConfig(level=getattr(logging, 'INFO', None))


@click.group()
def cli():
    pass


@cli.command('restart', help='Restart kafka instance')
@click.option('--broker', type=click.STRING,
              help='Broker id to restart. By default current broker id is restarted')
def restart_broker(broker: str):
    config, env_provider = __prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        broker_id = __get_opt_broker_id(broker, config, zookeeper, env_provider)
        RemoteCommandExecutorCheck.register_restart(zookeeper, broker_id)


@cli.command('rebalance', help='Run rebalance process on one of brokers')
@click.option('--broker', type=click.STRING,
              help="Broker instance on which to perform rebalance. By default, any free broker will start it")
def rebalance_partitions(broker: str):
    config, env_provider = __prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        broker_id = __get_opt_broker_id(broker, config, zookeeper, env_provider) if broker else None
        RemoteCommandExecutorCheck.register_rebalance(zookeeper, broker_id)


@cli.command('migrate', help='Replace one broker with another for all partitions')
@click.option('--from', 'from_', type=click.STRING, callback=__validate_not_empty,
              help='List of brokers to migrate from (separated with ",")')
@click.option('--to', type=click.STRING, callback=__validate_not_empty,
              help='List of brokers to migrate to (separated with ",")')
@click.option('--shrink', is_flag=True, default=False, show_default=True,
              help='Whether or not to shrink replaced broker ids form partition assignment')
@click.option('--broker', type=click.STRING, help='Optional broker id to execute check on')
def migrate_broker(from_: str, to: str, shrink: bool, broker: str):
    config, env_provider = __prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        broker_id = __get_opt_broker_id(broker, config, zookeeper, env_provider) if broker else None
        RemoteCommandExecutorCheck.register_migration(zookeeper, from_.split(','), to.split(','), shrink, broker_id)


@cli.command('swap_fat_slim', help='Move one partition from fat broker to slim one')
@click.option('--threshold', type=click.INT, default="100000", show_default=True, help="Threshold in kb to run swap")
def swap_partitions(threshold: int):
    config, env_provider = __prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        RemoteCommandExecutorCheck.register_fatboy_slim(zookeeper, threshold_kb=threshold)


@cli.command('stats', help='Display statistics about brokers')
def show_stats():
    config, env_provider = __prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        disk_stats = zookeeper.get_disk_stats()
        table = []
        for broker_id in zookeeper.get_broker_ids():
            disk = disk_stats.get(broker_id, {}).get('disk') if disk_stats else {}
            table.append({
                'Broker Id': broker_id,
                'Address': zookeeper.get_broker_address(broker_id),
                'Free kb': disk.get('free_kb'),
                'Used kb': disk.get('used_kb')
            })
        _print_table(table)


if __name__ == '__main__':
    cli()
