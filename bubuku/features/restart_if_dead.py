import logging

from datetime import datetime, timedelta
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
        self.last_zk_session_failed_check = None

    def check(self) -> Change:
        if not self.need_check:
            return None
        if not self.should_restart():
            return None

        _LOG.warning('Oops! Broker is dead, triggering restart')
        self.need_check = False

        # Do not start if broker is running and registered
        def _cancel_if():
            return self.broker.is_running() and self.broker.is_registered_in_zookeeper()

        return RestartBrokerChange(self.zk, self.broker, _cancel_if, self.on_change_executed)

    # Attempt to verify that broker is not registered in zookeeper for twice as long as the zookeeper session timeout.
    # Allow zookeeper client to try to restore the session before killing tha kafka process as soon as zookeeper session is dead.
    def should_restart(self):
        current_time = datetime.now()
        if not self.broker.is_running():
            return True
        if not self.broker.is_registered_in_zookeeper():
            _LOG.warning('Broker is not regiestered in Zookeeper')
            if not self.last_zk_session_failed_check:
                self.last_zk_session_failed_check = current_time
            time_to_restart_at = self.last_zk_session_failed_check + timedelta(milliseconds=self.broker.get_zookeeper_session_timeout() * 2)
            if current_time > time_to_restart_at:
                return True
        return False

    def on_change_executed(self):
        self.need_check = True
        self.last_zk_session_failed_check = None

    def __str__(self):
        return 'CheckBrokerStopped'
