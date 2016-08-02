import json
from unittest.mock import MagicMock

from bubuku.broker import BrokerManager, LeaderElectionInProgress
from test_config import build_test_properties


class FakeProcessManager(BrokerManager):
    def _open_process(self):
        return object()


def test_broker_checks_death():
    exhibitor = MagicMock()

    def _get_children(path):
        if path == '/brokers/topics':
            return ['t1', 't2']
        if path == '/brokers/topics/t1/partitions':
            return ['0']
        if path == '/brokers/topics/t2/partitions':
            return ['1']

    states = [2, 2]

    def _get(path):
        if path == '/brokers/topics/t1/partitions/0/state':
            topic = 1
        elif path == '/brokers/topics/t2/partitions/1/state':
            topic = 2
        else:
            raise NotImplementedError('Not implemented for path {}'.format(path))
        curstate = states[topic - 1]
        states[topic - 1] -= 1
        return json.dumps(
            {"leader": 1 if curstate == 2 else 3, "isr": [1, 3] if curstate >= 1 else [3]}
        ).encode('utf-8'), 'xxx'

    exhibitor.get_children = _get_children
    exhibitor.get = _get

    id_manager = MagicMock()
    id_manager.get_broker_id = lambda: '1'
    kafka_props = build_test_properties()
    kafka_props.set_property('unclean.leader.election.enable', 'true')

    manager = FakeProcessManager('kafka_dir', exhibitor, id_manager, kafka_props)

    assert not manager.has_leadership()

    kafka_props.set_property('unclean.leader.election.enable', 'false')
    for i in range(0, 2):
        assert manager.has_leadership(), 'For iteration {}'.format(i)
    assert not manager.has_leadership()


def __prepare_for_start_fail(broker_ids, leader, isr):
    exhibitor = MagicMock()

    def _get_children(path):
        if path == '/brokers/ids':
            return broker_ids
        elif path == '/brokers/topics':
            return ['t1']
        elif path == '/brokers/topics/t1/partitions':
            return ['0']
        else:
            raise NotImplementedError('Not implemented for path {}'.format(path))

    def _get(path):
        if path == '/brokers/topics/t1/partitions/0/state':
            return json.dumps({
                'leader': leader,
                'isr': isr
            }).encode('utf-8'), 'zzz'
        else:
            raise NotImplementedError('Not implemented for path {}'.format(path))

    exhibitor.get_children = _get_children
    exhibitor.get = _get
    id_manager = MagicMock()
    id_manager.get_broker_id = lambda: '1'
    kafka_props = build_test_properties()

    broker = FakeProcessManager('kafka_dir', exhibitor, id_manager, kafka_props)

    kafka_props.set_property('unclean.leader.election.enable', 'false')
    return kafka_props, broker


def test_broker_start_fail_leaders():
    kafka_props, broker = __prepare_for_start_fail(['1', '2'], 1, [3, 4])
    # suppose that broker is in leaders
    try:
        broker.start_kafka_process('')
        assert False, 'broker 1 must be in leaders, it must be impossible to start it'
    except LeaderElectionInProgress:
        pass


def test_broker_start_fail_isr():
    kafka_props, broker = __prepare_for_start_fail(['1', '2'], 3, [4, 2])
    # suppose that broker in isr
    try:
        broker.start_kafka_process('')
        assert False, 'broker 1 must be in leaders, it must be impossible to start it'
    except LeaderElectionInProgress:
        pass


def test_broker_start_success_clean():
    kafka_props, broker = __prepare_for_start_fail(['1', '2'], 3, [4, 5])
    # suppose that broker is free to start
    broker.start_kafka_process('')


def test_broker_start_success_unclean_1():
    kafka_props, broker = __prepare_for_start_fail(['1', '2'], 1, [1, 2])
    kafka_props.delete_property('unclean.leader.election.enable')
    # suppose that broker is free to start
    broker.start_kafka_process('')


def test_broker_start_success_unclean_2():
    kafka_props, broker = __prepare_for_start_fail(['1', '2'], 1, [1, 2])
    kafka_props.set_property('unclean.leader.election.enable', 'true')
    # suppose that broker is free to start
    broker.start_kafka_process('')
