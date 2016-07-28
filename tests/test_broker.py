import json
from unittest.mock import MagicMock

from bubuku.broker import BrokerManager
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

    assert manager.stop_kafka_process()

    kafka_props.set_property('unclean.leader.election.enable', 'false')
    for i in range(0, 4):
        assert not manager.stop_kafka_process(), 'For iteration {}'.format(i)
    assert manager.stop_kafka_process()
