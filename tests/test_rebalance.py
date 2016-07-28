import functools
import json
from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError

from bubuku.features.rebalance import RebalanceChange, RebalanceOnBrokerListChange
from bubuku.zookeeper import Exhibitor


def test_rebalance_can_run():
    o = RebalanceChange(object(), [])

    blocked_actions = ['restart', 'start', 'stop', 'rebalance']

    # Check that can run in exact cases
    for a in blocked_actions:
        assert not o.can_run([a])

    assert o.can_run(['xxx'])
    assert o.can_run([])


def test_rebalance_get_name():
    o = RebalanceChange(object(), [])
    assert o.get_name() == 'rebalance'


def __create_zk_for_topics(topic_data) -> Exhibitor:
    def _get_children(path: str):
        if path == '/brokers/ids':
            return list(set(functools.reduce(lambda x, y: x + y, topic_data.values(), [])))
        if path == '/brokers/topics':
            return list(set([k[0] for k in topic_data.keys()]))
        raise NotImplementedError('get_children {} is not supported'.format(path))

    def _get(path):
        if path.startswith('/brokers/topics/'):
            topic = path[len('/brokers/topics/'):]
            return json.dumps({'partitions': {k[1]: br for k, br in topic_data.items() if k[0] == topic}}).encode(
                'utf-8'), object()
        elif path == '/admin/reassign_partitions':
            raise NoNodeError()
        raise NotImplementedError('get {} is not supported'.format(path))

    def _create(path, value: bytes):
        if path == '/admin/reassign_partitions':
            for item in json.loads(value.decode('utf-8'))['partitions']:
                topic_data[(item['topic'], str(item['partition']))] = [str(x) for x in item['replicas']]
            return
        raise NotImplementedError('set {}, {} is not supported'.format(path, value))

    a = MagicMock()
    a.get_children = _get_children
    a.get = _get
    a.create = _create
    return sorted(list(set(functools.reduce(lambda x, y: x + y, topic_data.values(), [])))), a


def test_rebalance_on_empty1():
    brokers, zk = __create_zk_for_topics({})
    o = RebalanceChange(zk, brokers)
    while o.run([]):
        pass


def __verify_balanced(broker_ids, distribution):
    per_broker_data = {k: {'leaders': 0, 'total': 0} for k in broker_ids}
    for broker_ids in distribution.values():
        per_broker_data[broker_ids[0]]['leaders'] += 1
        for b in broker_ids:
            per_broker_data[b]['total'] += 1
    min_leaders = min(k['leaders'] for k in per_broker_data.values())
    max_leaders = max(k['leaders'] for k in per_broker_data.values())

    assert (max_leaders - min_leaders) <= 1

    min_total = min(k['total'] for k in per_broker_data.values())
    max_total = max(k['total'] for k in per_broker_data.values())

    assert (max_total - min_total) <= 1


def test_rebalance_on_filled1():
    distribution = {
        ('t0', '0'): ['2'],
        ('t0', '1'): ['1'],
        ('t0', '2'): ['1'],
        ('t0', '3'): ['1'],
    }
    brokers, zk = __create_zk_for_topics(distribution)
    o = RebalanceChange(zk, brokers)
    # broker to partitions
    while o.run([]):
        pass

    __verify_balanced(('1', '2'), distribution)


def test_rebalance_on_filled2():
    distribution = {
        ('t0', '0'): ['2', '1'],
        ('t0', '1'): ['1', '2'],
        ('t0', '2'): ['1', '2'],
        ('t0', '3'): ['1', '2'],
        ('t0', '4'): ['1', '2'],
        ('t0', '5'): ['1', '2'],
        ('t0', '6'): ['1', '2'],
    }
    brokers, zk = __create_zk_for_topics(distribution)
    o = RebalanceChange(zk, brokers)
    # broker to partitions
    while o.run([]):
        pass

    __verify_balanced(('1', '2'), distribution)


def test_rebalance_invoked_on_broker_list_change():
    zk = MagicMock()

    zk.get = MagicMock(side_effect=NoNodeError)

    check = RebalanceOnBrokerListChange(zk, MagicMock())
    zk.get_children = lambda x: ['1', '2', '3']

    assert check.check() is not None
    assert check.check() is None
    zk.get_children = lambda x: ['2', '1', '3']
    assert check.check() is None
    zk.get_children = lambda x: ['2', '1', '4']
    assert check.check() is not None
    assert check.check() is None
