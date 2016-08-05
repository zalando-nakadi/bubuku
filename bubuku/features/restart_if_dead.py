import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.restart_if_dead')


class StartBrokerChange(Change):
    def __init__(self, broker: BrokerManager, zk: BukuExhibitor):
        self.broker = broker
        self.zk = zk
        self.stopped = False

    def get_name(self) -> str:
        return 'start'

    def can_run(self, current_actions):
        # Only one restart allowed.
        return all([action not in current_actions for action in ['start', 'restart', 'stop']])

    def run(self, current_actions) -> bool:
        if self.broker.is_running_and_registered():
            return False
        _LOG.info('Waiting for complete death')
        if not self.stopped:
            self.broker.stop_kafka_process()
            self.stopped = True

        _LOG.info('Starting up again')
        try:
            self.broker.start_kafka_process(self.zk.get_conn_str())
        except Exception as ex:
            _LOG.warn('Failed to start kafka process', exc_info=ex)
            return True
        return False

    def __str__(self):
        return 'StartBrokerChange({})'.format(self.get_name())


class CheckBrokerStopped(Check):
    def __init__(self, broker: BrokerManager, zk: BukuExhibitor):
        super().__init__()
        self.broker = broker
        self.zk = zk

    def check(self) -> Change:
        if self.broker.is_running_and_registered():
            return None
        _LOG.info('Oops! Broker is dead, triggering restart')
        return StartBrokerChange(self.broker, self.zk)

    def __str__(self):
        return 'CheckBrokerStopped'
