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
        self.last_zk_session_check = datetime.now()

    def check(self) -> Change:
        if not self.need_check:
            return None
        if self.is_running_and_registered(should_retry=True):
            return None

        _LOG.warning('Oops! Broker is dead, triggering restart')
        self.need_check = False

        # Do not start if broker is running and registered
        def _cancel_if():
            return self.is_running_and_registered()

        return RestartBrokerChange(self.zk, self.broker, _cancel_if, self.on_check_removed)

    # Attempt to verify that broker is not registered in zookeeper for twice as long as the zookeeper session timeout.
    # Allow zookeeper client to try to restore the session before killing tha kafka process as soon as zookeeper session is dead.
    def is_running_and_registered(self, should_retry=False):
        if not self.broker.is_running():
            return False
        if not self.broker.is_registered_in_zookeeper():
            _LOG.warning('Broker is to not be regiestered in Zookeeper')
            time_to_return = self.last_zk_session_check + timedelta(milliseconds=self.broker.get_zookeeper_session_timeout() * 2)
            if datetime.now() > time_to_return or not should_retry:
                self.last_zk_session_check = datetime.now()
                return False
        else:
            self.last_zk_session_check = datetime.now()
        return True

    def on_check_removed(self):
        self.need_check = True

    def __str__(self):
        return 'CheckBrokerStopped'
