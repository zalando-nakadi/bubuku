import unittest
from unittest.mock import MagicMock, Mock

from bubuku.broker import LeaderElectionInProgress
from bubuku.features.restart_on_zk_change import RestartBrokerChange


class RestartTest(unittest.TestCase):
    def test_restart_atomicity(self):
        """
        Because of action locks structure there is need to restart instances atomically. That means, that during restart
        action parts (stop, wait for leader election, start) must be under same lock. That guarantees, that cluster
        instances won't be destroyed at the same time during zk update.
        """
        broker = MagicMock()
        zk = MagicMock()
        change = RestartBrokerChange(zk, broker, None)

        zk.get_conn_str = lambda: 'xxx'

        broker.is_running_and_registered = lambda: True
        stopped = []
        broker.stop_kafka_process = lambda: stopped.append(True)
        assert change.run([])
        assert stopped and stopped[0]

        broker.start_kafka_process = Mock(side_effect=LeaderElectionInProgress())
        for i in range(1, 50):
            assert change.run([])

        started = []
        broker.start_kafka_process = lambda x: started.append(x)
        assert not change.run([])
        assert started and 'xxx' == started[0]
