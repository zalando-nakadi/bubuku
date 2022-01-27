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
        self.check_sleep_time_ms = 500
        # Attempt to check if broker is not registered in zookeeper for twice as long as the zookeeper session timeout
        # with 500ms interval in between attempts.
        # Zookeeper client will attempt to create a new session after it is lost after the timeout of the configured value
        # of zookeeper.connection.timeout.ms.
        self.is_broker_registered_attempts = round(((self.broker.kafka_properties.get_property(
            "zookeeper.connection.timeout.ms") or 6000) / self.check_sleep_time_ms) * 2)

    def check(self) -> Change:
        if not self.need_check:
            return None
        if self.is_running_and_registered(attempts=self.is_broker_registered_attempts):
            return None

        _LOG.info('Oops! Broker is dead, triggering restart')
        self.need_check = False

        # Do not start if broker is running and registered
        def _cancel_if():
            return self.is_running_and_registered()

        return RestartBrokerChange(self.zk, self.broker, _cancel_if, self.on_check_removed)

    def is_running_and_registered(self, attempts=1):
        for x in range(0, attempts):
            if (x > 0):
                sleep(self.check_sleep_time_ms / 1000)
            # The process might have died already, so no reason to attempt to wait for zookeeper session to restore
            if not self.broker.is_running():
                return False
            if self.broker.is_registered_in_zookeeper():
                return True
            _LOG.info(
                'Broker is not registered in zookeeper, {} attempt to retry'.format(x + 1))
        return False

    def on_check_removed(self):
        self.need_check = True

    def __str__(self):
        return 'CheckBrokerStopped'
