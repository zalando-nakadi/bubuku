import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
from bubuku.features.migrate import MigrationChange
from bubuku.features.rebalance.change import OptimizedRebalanceChange
from bubuku.features.restart_on_zk_change import RestartBrokerChange
from bubuku.features.swap_partitions import SwapPartitionsChange, load_swap_data
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.remote_exec')


class RemoteCommandExecutorCheck(Check):
    def __init__(self, zk: BukuExhibitor, broker_manager: BrokerManager, api_port):
        super().__init__(check_interval_s=30)
        self.zk = zk
        self.broker_manager = broker_manager
        self.api_port = api_port

    def check(self) -> Change:
        with self.zk.lock():
            data = self.zk.take_action(self.broker_manager.id_manager.get_broker_id())
        if not data:
            return None
        if 'name' not in data:
            _LOG.error('Action name can not be restored from {}, skipping'.format(data))
            return None
        try:
            if data['name'] == 'restart':
                return RestartBrokerChange(self.zk, self.broker_manager, lambda: False)
            elif data['name'] == 'rebalance':
                return OptimizedRebalanceChange(self.zk,
                                                self.zk.get_broker_ids(),
                                                data['empty_brokers'],
                                                data['exclude_topics'],
                                                int(data.get('parallelism', 1)))
            elif data['name'] == 'migrate':
                return MigrationChange(self.zk, data['from'], data['to'], data['shrink'],
                                       int(data.get('parallelism', '1')))
            elif data['name'] == 'fatboyslim':
                return SwapPartitionsChange(self.zk,
                                            lambda x: load_swap_data(x, self.api_port, int(data['threshold_kb'])))
            else:
                _LOG.error('Action {} not supported'.format(data))
        except Exception as e:
            _LOG.error('Failed to create action from {}'.format(data), exc_info=e)
        return None

    def __str__(self):
        return 'RemoteCommandExecutorCheck'

    @staticmethod
    def register_restart(zk: BukuExhibitor, broker_id: str):
        with zk.lock():
            zk.register_action(
                {'name': 'restart'},
                broker_id=broker_id)

    @staticmethod
    def register_rebalance(zk: BukuExhibitor, broker_id: str, empty_brokers: list, exclude_topics: list,
                           parallelism: int):
        if parallelism <= 0:
            raise Exception('Parallelism for rebalance should be greater than 0')
        action = {'name': 'rebalance',
                  'empty_brokers': empty_brokers,
                  'exclude_topics': exclude_topics,
                  'parallelism': int(parallelism)}
        with zk.lock():
            if broker_id:
                zk.register_action(action, broker_id=broker_id)
            else:
                zk.register_action(action)

    @staticmethod
    def register_migration(zk: BukuExhibitor, brokers_from: list, brokers_to: list, shrink: bool, broker_id: str,
                           parallelism: int):
        if len(brokers_from) != len(brokers_to):
            raise Exception('Brokers list {} and {} must have the same size'.format(brokers_from, brokers_to))
        if any(b in brokers_from for b in brokers_to) or any(b in brokers_to for b in brokers_from):
            raise Exception('Broker lists can not hold same broker ids')

        if len(set(brokers_from)) != len(brokers_from):
            raise Exception('Can not use same broker ids for source_list {}'.format(brokers_from))
        if len(set(brokers_to)) != len(brokers_to):
            raise Exception('Can not use same broker ids for source_list {}'.format(brokers_from))

        active_ids = zk.get_broker_ids()
        if any(b not in active_ids for b in brokers_from) or any(b not in active_ids for b in brokers_to):
            raise Exception('Brokers dead from: {} to: {} alive:{}'.format(brokers_from, brokers_to, active_ids))

        if broker_id and str(broker_id) not in active_ids:
            raise Exception('Broker id to run change on ({}) is not in active list {}'.format(
                broker_id, active_ids))
        if parallelism <= 0:
            raise Exception('Parallelism for migration should be greater than 0')

        with zk.lock():
            action = {'name': 'migrate', 'from': brokers_from, 'to': brokers_to, 'shrink': bool(shrink),
                      'parallelism': int(parallelism)}
            if broker_id:
                zk.register_action(action, str(broker_id))
            else:
                zk.register_action(action)

    @staticmethod
    def register_fatboy_slim(zk: BukuExhibitor, threshold_kb: int):
        if zk.is_rebalancing():
            _LOG.warning('Rebalance is already in progress, may be it will take time for this command to start '
                         'processing')
        with zk.lock():
            zk.register_action({'name': 'fatboyslim', 'threshold_kb': threshold_kb})
