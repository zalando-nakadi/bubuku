import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.features.restart_if_dead')


class StartBrokerChange(Change):
    def __init__(self, broker: BrokerManager, zk: Exhibitor):
        self.broker = broker
        self.zk = zk

    def get_name(self) -> str:
        return 'start'

    def can_run(self, current_actions):
        # Only one restart allowed.
        return all([action not in current_actions for action in ['start', 'restart', 'stop']])

    def run(self, current_actions) -> bool:
        if self.broker.is_running_and_registered():
            return False
        _LOG.info('Waiting for complete death')
        self.broker.stop_kafka_process()
        _LOG.info('Starting up again')
        self.broker.start_kafka_process(self.zk.exhibitor.zookeeper_hosts + self.zk.prefix)
        return False


class CheckBrokerStopped(Check):
    def __init__(self, broker: BrokerManager, zk: Exhibitor):
        self.broker = broker
        self.zk = zk

    def check(self) -> Change:
        if self.broker.is_running_and_registered():
            return None
        _LOG.info('Oops! Broker is dead, triggering restart')
        return StartBrokerChange(self.broker, self.zk)
