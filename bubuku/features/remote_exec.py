import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
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
        if data['name'] == 'rebalance':
            return RebalanceChange(self.zk, self.zk.get_broker_ids())
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
