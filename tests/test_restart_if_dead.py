import unittest
from unittest.mock import MagicMock
from bubuku.features.restart_if_dead import CheckBrokerStopped


class TestRestartIfDeadCheck(unittest.TestCase):
    def test_broker_retries_before_it_restarts(self):
        brokerManager = MagicMock()
        isRegistered = MagicMock(return_value=False)
        attrs = {'process.is_running.return_value': True,
                 'id_manager.is_registered': isRegistered}
        brokerManager.configure_mock(**attrs)

        exhibitor = MagicMock()

        checkBrokerStopped = CheckBrokerStopped(brokerManager, exhibitor)
        checkBrokerStopped.check()
        
        assert isRegistered.call_count == 3
