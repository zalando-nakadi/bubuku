import unittest
from unittest.mock import MagicMock
from bubuku.features.restart_if_dead import CheckBrokerStopped

class TestRestartIfDeadCheck(unittest.TestCase):

    def test_broker_retries_before_it_restarts(self):
        brokerManager = MagicMock()
        isRegistered = MagicMock(return_value=False)
        attrs = {'is_running.return_value': True,
                 'is_registered_in_zookeeper': isRegistered,
                 'get_zookeeper_session_timeout.return_value': 1}
        brokerManager.configure_mock(**attrs)

        exhibitor = MagicMock()
        checkBrokerStopped = CheckBrokerStopped(brokerManager, exhibitor)
        checkReturnedSomething = None
        while not checkReturnedSomething:
            checkReturnedSomething = checkBrokerStopped.check()
        assert isRegistered.call_count > 0
