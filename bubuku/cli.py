#!/usr/bin/env python3
import json
import logging
import time

import click
import requests
from requests import Response

from bubuku.features.remote_exec import RemoteCommandExecutorCheck
from bubuku.utils import get_opt_broker_id, prepare_configs, is_cluster_healthy, get_max_bytes_in
from bubuku.zookeeper import load_exhibitor_proxy, BukuExhibitor, RebalanceThrottleManager

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
    return value


def __check_all_broker_ids_exist(broker_ids: list, zk: BukuExhibitor):
    registered_brokers = zk.get_broker_ids()
    unknown_brokers = [broker_id for broker_id in broker_ids if broker_id not in registered_brokers]
    if len(unknown_brokers) == 1:
        raise Exception('1 broker id is not valid: {}'.format(unknown_brokers[0]))
    if len(unknown_brokers) > 1:
        raise Exception('{} broker ids are not valid: {}'.format(len(unknown_brokers), ",".join(unknown_brokers)))


logging.basicConfig(level=getattr(logging, 'INFO', None))


@click.group()
def cli():
    logo = """
        ____        __          __        
       / __ )__  __/ /_  __  __/ /____  __
      / __  / / / / __ \/ / / / //_/ / / /
     / /_/ / /_/ / /_/ / /_/ / ,< / /_/ / 
    /_____/\__,_/_.___/\__,_/_/|_|\__,_/  
    """
    print(logo)
    print('Start, monitor and rebalance kafka cluster in AWS setup')
    print()
    pass


@cli.command('preferred-replica-election',
             help='Do preferred replica election, as command line tool from kafka have a number of limitations. '
                  'Only partitions, that are improperly allocated will be affected. In case if size of resulting json '
                  'is too big, it will be split into several parts, and they will be executed one after another.')
@click.option('--dry-run', is_flag=True, help="Do not apply the changes. Instead just prepare json file(s)")
@click.option('--max-json-size', type=click.INT, default=512000, 
              help="Maximum size of json data in bytes to write to zk", show_default=True)
def trigger_preferred_replica_election(dry_run: bool, max_json_size: int):
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        partitions_state = {}
        for topic, partition, state in zookeeper.load_partition_states():
            partitions_state[(topic, partition)] = state

        wrong_assignment = []
        for topic, partition, replica_list in zookeeper.load_partition_assignment():
            key = (topic, partition)
            if not replica_list:
                _LOG.warning('Replica list is not defined for %s', key)
                continue
            if key not in partitions_state:
                _LOG.warning("Topic partition %s is not found in active states list. will skip it", key)
                continue
            leader = partitions_state[key].get('leader')
            if leader is None:
                _LOG.warning('Current leader is not defined for ')
                continue
            expected_leader = replica_list[0]
            if leader != expected_leader:
                _LOG.info("Found incorrect assignment: %s, leader is %d, but should be the first one in %s",
                          key, leader, replica_list)
                wrong_assignment.append(key)
        while wrong_assignment:
            items_to_take = len(wrong_assignment)
            change_applied = False
            while not change_applied:
                json_element = {
                    "version": 1,
                    "partitions": [{'topic': v[0], 'partition': int(v[1])} for v in wrong_assignment[:items_to_take]]
                }

                res = json.dumps(json_element).encode('utf-8')
                if len(res) > max_json_size:
                    new_items_to_take = int(items_to_take / 2)
                    _LOG.info("Not fitting to %d bytes with %d items, will try %d items",
                              max_json_size, items_to_take, new_items_to_take)
                    items_to_take = new_items_to_take
                    if items_to_take <= 0:
                        _LOG.error("Incorrect configuration - even one key is not fitting to proposed size %d. "
                                   "Stop playing and do the job!", max_json_size)
                        exit(1)
                    continue
                if dry_run:
                    print(res.decode('utf-8'))
                else:
                    _LOG.info("Applying %s", res.decode('utf-8'))
                    zookeeper.exhibitor.create('/admin/preferred_replica_election', res)

                    while zookeeper.exhibitor.is_node_present('/admin/preferred_replica_election'):
                        _LOG.info("Waiting for node to disappear")
                        time.sleep(1)
                change_applied = True
                del wrong_assignment[:items_to_take]

        _LOG.info("Done with assignment")


@cli.command('restart', help='Restart kafka instance')
@click.option('--broker', type=click.STRING,
              help='Broker id to restart. By default current broker id is restarted')
def restart_broker(broker: str):
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        broker_id = get_opt_broker_id(broker, config, zookeeper, env_provider)
        RemoteCommandExecutorCheck.register_restart(zookeeper, broker_id)


@cli.command('rolling-restart', help='Rolling restart of Kafka cluster')
@click.option('--image-tag', type=click.STRING, help='Docker image to run Kafka broker')
@click.option('--instance-type', type=click.STRING, default='m4.4xlarge',
              help='AWS instance type to run Kafka broker on')
@click.option('--scalyr-key', type=click.STRING, help='Scalyr account key')
@click.option('--scalyr-region', type=click.STRING, help='Scalyr region to use')
@click.option('--kms-key-id', type=click.STRING, help='Kms key id to decrypt data with')
@click.option('--cool-down', type=click.INT, default=20, show_default=True,
              help='Number of seconds to wait before passing the restart task to another broker, after cluster is '
                   'stable')
def rolling_restart_broker(image_tag: str, instance_type: str, scalyr_key: str, scalyr_region: str, kms_key_id: str,
                           cool_down: int):
    if not is_cluster_healthy():
        print('Cluster is not healthy, try again later :)')
        return

    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        broker_id = get_opt_broker_id(None, config, zookeeper, env_provider)
        RemoteCommandExecutorCheck.register_rolling_restart(zookeeper, broker_id, image_tag, instance_type, scalyr_key,
                                                            scalyr_region, kms_key_id, cool_down)


@cli.command('rebalance', help='Run rebalance process on one of brokers. If rack-awareness is enabled, replicas will '
                               'only be move to other brokers in the same rack')
@click.option('--broker', type=click.STRING,
              help="Broker instance on which to perform rebalance. By default, any free broker will start it")
@click.option('--empty_brokers', type=click.STRING,
              help="Comma-separated list of brokers to empty. All partitions will be moved to other brokers")
@click.option('--exclude_topics', type=click.STRING, help="Comma-separated list of topics to exclude from rebalance")
@click.option('--bin-packing', is_flag=True, help="Use bean packing approach instead of one way processing")
@click.option('--parallelism', type=click.INT, default=1, show_default=True,
              help="Amount of partitions to move in a single rebalance step")
@click.option('--throttle', type=click.INT, default=100000000, help="Upper bound on bandwidth (in bytes/sec) used for "
                                                                    "rebalance")
@click.option('--remove-throttle', is_flag=True, help="Don't trigger rebalance but remove throttling "
                                                      "configuration from all the brokers and topics")
def rebalance_partitions(broker: str, empty_brokers: str, exclude_topics: str, parallelism: int, bin_packing: bool,
                         throttle: int, remove_throttle: bool):
    if throttle and throttle < get_max_bytes_in():
        print('Throttle value must be set above the max BytesIn for the replication to progress. '
              'The current max BytesIn is {}'.format(get_max_bytes_in()))
        exit(1)
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        if remove_throttle:
            return RebalanceThrottleManager.remove_all_throttle_configurations(zookeeper)
        empty_brokers_list = [] if empty_brokers is None else empty_brokers.split(',')
        exclude_topics_list = [] if exclude_topics is None else exclude_topics.split(',')
        __check_all_broker_ids_exist(empty_brokers_list, zookeeper)
        broker_id = get_opt_broker_id(broker, config, zookeeper, env_provider) if broker else None
        RemoteCommandExecutorCheck.register_rebalance(zookeeper, broker_id, empty_brokers_list,
                                                      exclude_topics_list, parallelism, bin_packing, throttle)


@cli.command('migrate', help='Replace one broker with another for all partitions')
@click.option('--from', 'from_', type=click.STRING, callback=__validate_not_empty,
              help='List of brokers to migrate from (separated with ",")')
@click.option('--to', type=click.STRING, callback=__validate_not_empty,
              help='List of brokers to migrate to (separated with ",")')
@click.option('--shrink', is_flag=True, default=False, show_default=True,
              help='Whether or not to shrink replaced broker ids form partition assignment')
@click.option('--broker', type=click.STRING, help='Optional broker id to execute check on')
@click.option('--throttle', type=click.INT, default=100000000, help="Upper bound on bandwidth (in bytes/sec) used for "
                                                                    "reassigning partitions")
@click.option('--parallelism', type=click.INT, show_default=True, default=1,
              help="Amount of partitions to move in a single migration step")
@click.option('--remove-throttle', is_flag=True, help="Don't trigger rebalance but remove throttling "
                                                      "configuration from all the brokers and topics")
def migrate_broker(from_: str, to: str, shrink: bool, broker: str, throttle: int, parallelism: int,
                   remove_throttle: bool):
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        if remove_throttle:
            return RebalanceThrottleManager.remove_all_throttle_configurations(zookeeper)
        broker_id = get_opt_broker_id(broker, config, zookeeper, env_provider) if broker else None
        RemoteCommandExecutorCheck.register_migration(zookeeper, from_.split(','), to.split(','), shrink, broker_id,
                                                      throttle, parallelism)


@cli.command('swap_fat_slim', help='Move one partition from fat broker to slim one')
@click.option('--threshold', type=click.INT, default="100000", show_default=True, help="Threshold in kb to run swap")
def swap_partitions(threshold: int):
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        RemoteCommandExecutorCheck.register_fatboy_slim(zookeeper, threshold_kb=threshold)


@cli.group(name='actions', help='Work with running actions')
def actions():
    pass


@actions.command('list', help='List all the actions on broker(s)')
@click.option('--broker', type=click.STRING,
              help='Broker id to list actions on. By default all brokers are enumerated')
def list_actions(broker: str):
    table = []
    config, env_provider = prepare_configs()

    for broker_id, address in _list_broker_addresses(config, env_provider, broker):
        try:
            response = requests.get('http://{}:{}/api/controller/queue'.format(address, config.health_port))
        except Exception as e:
            print('Failed to query information on {} ({})'.format(broker_id, address))
            _LOG.error('Failed to query information on {} ({})'.format(broker_id, address), exc_info=e)
            continue
        line = {
            '_broker_id': broker_id,
            '_broker_address': address,
        }
        if response.status_code != 200:
            line['error'] = _extract_error(response)
            table.append(line)
        else:
            changes = response.json()
            if not changes:
                line.update({
                    'type': None,
                    'description': None,
                    'running': None
                })
                table.append(line)
            else:
                for change in changes:
                    line_copy = dict(line)
                    line_copy.update(change)
                    table.append(line_copy)
    if not table:
        print('No brokers found')
    else:
        _print_table(table)


@actions.command('delete', help='Remove all actions of specified type on broker(s)')
@click.option('--action', type=click.STRING,
              help='Action to delete')
@click.option('--broker', type=click.STRING,
              help='Broker id to delete actions on. By default actions are deleted on all brokers')
def delete_actions(action: str, broker: str):
    if not action:
        print('No action specified. Please specify it')
    config, env_provider = prepare_configs()

    for broker_id, address in _list_broker_addresses(config, env_provider, broker):
        try:
            response = requests.delete(
                'http://{}:{}/api/controller/queue/{}'.format(address, config.health_port, action))
        except Exception as e:
            print('Failed to query information on {} ({})'.format(broker_id, address))
            _LOG.error('Failed to query information on {} ({})'.format(broker_id, address), exc_info=e)
            continue
        if response.status_code not in (200, 204):
            print('Failed to delete action from {} ({}): {}'.format(broker, address, _extract_error(response)))
        else:
            print('Removed action {} from {} ({})'.format(action, broker_id, address))


def _extract_error(response: Response):
    try:
        return response.json()['message']
    except Exception as e:
        _LOG.error('Failed to parse response message', exc_info=e)
        return response.text()


def _list_broker_addresses(config, env_provider, broker):
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        for broker_id in zookeeper.get_broker_ids():
            if broker and broker != broker_id:
                continue
            yield broker_id, zookeeper.get_broker_address(broker_id)


@cli.command('stats', help='Display statistics about brokers')
def show_stats():
    config, env_provider = prepare_configs()
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


@cli.group(name='validate', help='Validates internal structures of kafka/zk')
def validate():
    pass


@validate.command('replication', help='Returns all partitions whose ISR size differs from the replication factor or '
                                      'have not registered broker ids')
@click.option('--factor', type=click.INT, default=3, show_default=True, help='Replication factor')
def validate_replication(factor: int):
    config, env_provider = prepare_configs()
    with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zookeeper:
        brokers = {int(x) for x in zookeeper.get_broker_ids()}
        table = []
        for topic_name, partition, state in zookeeper.load_partition_states():
            if len(state['isr']) != factor or not set(state['isr']).issubset(brokers):
                table.append({
                    'Partition': partition,
                    'Topic': topic_name,
                    'State': state
                })
        if table:
            _LOG.info('Invalid topics:')
            _print_table(table)
        else:
            print('All replica lists look valid')


if __name__ == '__main__':
    cli()
