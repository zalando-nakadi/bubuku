import unittest
from unittest.mock import MagicMock
from bubuku.features.restart_if_dead import CheckBrokerStopped
from datetime import datetime, timedelta
from timeout_decorator import timeout

class TestRestartIfDeadCheck(unittest.TestCase):
    @timeout(0.5)
    def test_broker_retries_before_it_restarts(self):
        brokerManager = MagicMock()
        isRegistered = MagicMock(return_value=False)
        attrs = {'is_running.return_value': True,
                 'is_registered_in_zookeeper': isRegistered,
                 'kafka_properties.get_property.return_value': 1}
        brokerManager.configure_mock(**attrs)

        exhibitor = MagicMock()
        checkBrokerStopped = CheckBrokerStopped(brokerManager, exhibitor)
        checkReturnedSomething = None
        while not checkReturnedSomething:
            checkReturnedSomething = checkBrokerStopped.check()
        assert isRegistered.call_count > 0
