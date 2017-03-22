import logging
from bubuku.broker import BrokerManager
from bubuku.controller import Check
from bubuku.features.rebalance.change import OptimizedRebalanceChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rebalance')


class RebalanceOnStartCheck(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.executed = False

    def check(self):
        if self.executed:
            return None
        if not self.broker.is_running_and_registered():
            return None
        _LOG.info("Rebalance on start, triggering rebalance")
        self.executed = True
        return OptimizedRebalanceChange(self.zk, self.zk.get_broker_ids(), [], [])

    def __str__(self):
        return 'RebalanceOnStartCheck (executed={})'.format(self.executed)


class RebalanceOnBrokerListCheck(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.old_broker_list = []

    def check(self):
        if not self.broker.is_running_and_registered():
            return None
        new_list = self.zk.get_broker_ids()
        if not new_list == self.old_broker_list:
            _LOG.info('Broker list changed from {} to {}, triggering rebalance'.format(self.old_broker_list, new_list))
            self.old_broker_list = new_list
            return OptimizedRebalanceChange(self.zk, new_list, [], [])
        return None

    def __str__(self):
        return 'RebalanceOnBrokerListChange, cached list: {}'.format(self.old_broker_list)
