import unittest
from unittest.mock import MagicMock

from bubuku.broker import BrokerManager, LeaderElectionInProgress, StartupTimeout
from bubuku.process import KafkaProcess
from test_config import build_test_properties

zk_fake_host = 'zk_host:8181/path'


class FakeProcessManager(KafkaProcess):
    def __init__(self):
        super().__init__('')
        self.running = False

    def start(self, settings_file):
        if self.running:
            raise Exception('Start second time')
        self.running = True

    def stop_and_wait(self):
        self.running = False

    def is_running(self) -> bool:
        return self.running


def _prepare_for_start_fail(broker_ids, leader, isr):
    exhibitor = MagicMock()
    exhibitor.get_broker_ids.return_value = broker_ids
    exhibitor.load_partition_states.return_value = [
        ('t0', 0, {'leader': int(leader), 'isr': [int(i) for i in isr]})]

    id_manager = MagicMock()
    id_manager.get_broker_id = lambda: '1'
    kafka_props = build_test_properties()

    broker = BrokerManager(FakeProcessManager(), exhibitor, id_manager, kafka_props,
                           StartupTimeout.build({'type': 'linear'}))

    kafka_props.set_property('unclean.leader.election.enable', 'false')
    return kafka_props, broker


class TestBroker(unittest.TestCase):
    def test_broker_checks_death(self):
        exhibitor = MagicMock()
        states = [2, 2]

        def _load_states(topics=None):
            for idx in range(0, len(states)):
                states[idx] -= 1
            return [
                ('t1', 0, {'leader': states[0], 'isr': [1, 3] if states[0] >= 1 else [3]}),
                ('t2', 0, {'leader': states[1], 'isr': [1, 3] if states[1] >= 1 else [3]})
            ]

        exhibitor.load_partition_states = _load_states

        id_manager = MagicMock()
        id_manager.get_broker_id = lambda: '1'
        kafka_props = build_test_properties()
        kafka_props.set_property('unclean.leader.election.enable', 'true')

        manager = BrokerManager(FakeProcessManager(), exhibitor, id_manager, kafka_props,
                                StartupTimeout.build({'type': 'linear'}))

        assert not manager.has_leadership()

        kafka_props.set_property('unclean.leader.election.enable', 'false')
        assert manager.has_leadership()
        assert not manager.has_leadership()

    def test_broker_start_success_isr(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 1, [3, 4])
        # suppose that leader exists, but isr - not
        broker.start_kafka_process(zk_fake_host)

    def test_broker_start_fail_isr(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 3, [4, 2])
        # suppose that leader is not present
        try:
            broker.start_kafka_process(zk_fake_host)
            assert False, 'broker 2 must be in leaders, it must be impossible to start 1'
        except LeaderElectionInProgress:
            pass

    def test_broker_start_fail_leader(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 3, [1, 5])
        # suppose that broker is free to start
        try:
            broker.start_kafka_process(zk_fake_host)
            assert False, 'Broker must not start in case where it''s possible to change leader'
        except LeaderElectionInProgress:
            pass

    def test_broker_start_success_no_leader_candidate(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 3, [4, 5])
        # suppose that broker is free to start
        broker.start_kafka_process(zk_fake_host)

    def test_broker_start_success_unclean_1(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 1, [1, 2])
        kafka_props.delete_property('unclean.leader.election.enable')
        # suppose that broker is free to start
        broker.start_kafka_process(zk_fake_host)

    def test_broker_start_success_unclean_2(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 1, [1, 2])
        kafka_props.set_property('unclean.leader.election.enable', 'true')
        # suppose that broker is free to start
        broker.start_kafka_process(zk_fake_host)

    def test_broker_start_fail_no_zk_conn(self):
        kafka_props, broker = _prepare_for_start_fail(['1', '2'], 3, [1, 5])
        try:
            broker.start_kafka_process(zk_fake_host)
            assert False, 'Broker must not start in case there is no connection to zk'
        except Exception as e:
            error_msg = str(e)
            assert error_msg != 'No connection to zookeeper'
