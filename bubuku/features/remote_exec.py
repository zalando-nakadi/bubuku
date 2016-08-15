import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
from bubuku.features.migrate import MigrationChange
from bubuku.features.rebalance import RebalanceChange
from bubuku.features.restart_on_zk_change import RestartBrokerChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.remote_exec')


class RemoteCommandExecutorCheck(Check):
    def __init__(self, zk: BukuExhibitor, broker_manager: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker_manager = broker_manager

    def check(self) -> Change:
        with self.zk.lock():
            data = self.zk.take_action(self.broker_manager.id_manager.get_broker_id())
        if not data:
            return None
        if 'name' not in data:
            _LOG.error('Action name can not be restored from {}, skipping'.format(data))
            return None
        if data['name'] == 'restart':
            return RestartBrokerChange(self.zk, self.broker_manager, lambda: False)
        elif data['name'] == 'rebalance':
            return RebalanceChange(self.zk, self.zk.get_broker_ids())
        elif data['name'] == 'migrate':
            return MigrationChange(data['from'], data['to'], data['shrink'])
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
    def register_rebalance(zk: BukuExhibitor, broker_id: str):
        with zk.lock():
            if broker_id:
                zk.register_action({'name': 'rebalance'}, broker_id=broker_id)
            else:
                zk.register_action({'name': 'rebalance'})

    @staticmethod
    def register_migration(zk: BukuExhibitor, brokers_from: list, brokers_to: list, shrink: bool):
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

        with zk.lock():
            zk.register_action({'name': 'migrate', 'from': brokers_from, 'to': brokers_to, 'shrink': bool(shrink)})
