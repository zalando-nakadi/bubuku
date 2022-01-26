import logging

from time import sleep
from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.features.restart_on_zk_change import RestartBrokerChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.restart_if_dead')


class CheckBrokerStopped(Check):
    def __init__(self, broker: BrokerManager, zk: BukuExhibitor):
        super().__init__()
        self.broker = broker
        self.zk = zk
        self.need_check = True

    def check(self) -> Change:
        if not self.need_check:
            return None
        if self.is_running_and_registered(tries=3):
            return None

        _LOG.info('Oops! Broker is dead, triggering restart')
        self.need_check = False

        # Do not start if broker is running and registered
        def _cancel_if():
            return self.is_running_and_registered()

        return RestartBrokerChange(self.zk, self.broker, _cancel_if, self.on_check_removed)

    def is_running_and_registered(self, tries=1):
        if not self.broker.process.is_running():
            return False
        # Retry with a sleep of 2 seconds in between
        for x in range(0, tries):
            if (x > 0):
                sleep(2)
            _LOG.info(
                'Broker is not registered in zookeeper, {} attempt to retry'.format(x + 1))
            if self.broker.id_manager.is_registered():
                return True
        return False

    def on_check_removed(self):
        self.need_check = True

    def __str__(self):
        return 'CheckBrokerStopped'
