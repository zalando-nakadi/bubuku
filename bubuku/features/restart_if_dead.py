import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.restart_if_dead')


class StartBrokerChange(Change):
    def __init__(self, broker: BrokerManager, zk: BukuExhibitor, processed_callback):
        self.broker = broker
        self.zk = zk
        self.stopped = False
        self.processed_callback = processed_callback

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

    def on_remove(self):
        if self.processed_callback:
            self.processed_callback()

    def __str__(self):
        return 'StartBrokerChange({})'.format(self.get_name())


class CheckBrokerStopped(Check):
    def __init__(self, broker: BrokerManager, zk: BukuExhibitor):
        super().__init__()
        self.broker = broker
        self.zk = zk
        self.need_check = True

    def check(self) -> Change:
        if not self.need_check:
            return None
        if self.broker.is_running_and_registered():
            return None
        _LOG.info('Oops! Broker is dead, triggering restart')
        self.need_check = False
        return StartBrokerChange(self.broker, self.zk, self.on_check_removed)

    def on_check_removed(self):
        self.need_check = True

    def __str__(self):
        return 'CheckBrokerStopped'
