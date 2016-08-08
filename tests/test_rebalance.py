import functools
from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError

from bubuku.features.rebalance import RebalanceChange, RebalanceOnBrokerListChange, combine_broker_ids
from bubuku.zookeeper import BukuExhibitor


def test_rebalance_can_run():
    o = RebalanceChange(object(), [])

    blocked_actions = ['restart', 'start', 'stop', 'rebalance', 'swap_partitions']

    # Check that can run in exact cases
    for a in blocked_actions:
        assert not o.can_run([a])

    assert o.can_run(['xxx'])
    assert o.can_run([])


def test_rebalance_get_name():
    o = RebalanceChange(object(), [])
    assert o.get_name() == 'rebalance'


def __create_zk_for_topics(topic_data, broker_ids=None) -> (list, BukuExhibitor):
    buku_proxy = MagicMock()
    buku_proxy.get_broker_ids.return_value = broker_ids if broker_ids else sorted(list(
        set(functools.reduce(lambda x, y: x + y, topic_data.values(), []))))

    def _load_assignment():
        return [(k[0], int(k[1]), [int(p) for p in v]) for k, v in topic_data.items()]

    buku_proxy.load_partition_assignment = _load_assignment
    buku_proxy.is_rebalancing.return_value = False

    def _reassign(topic, partition, replicas):
        topic_data[(topic, str(partition))] = [str(x) for x in replicas]
        return True

    buku_proxy.reallocate_partition = _reassign
    return sorted(list(set(functools.reduce(lambda x, y: x + y, topic_data.values(), [])))), buku_proxy


def test_rebalance_on_empty1():
    brokers, zk = __create_zk_for_topics({})
    o = RebalanceChange(zk, brokers)
    while o.run([]):
        pass


def __verify_balanced(broker_ids, distribution, allowed_delta_leaders=1, allowed_delta_total=1):
    per_broker_data = {k: {'leaders': 0, 'total': 0} for k in broker_ids}
    for broker_ids in distribution.values():
        per_broker_data[broker_ids[0]]['leaders'] += 1
        for b in broker_ids:
            per_broker_data[b]['total'] += 1
    min_leaders = min(k['leaders'] for k in per_broker_data.values())
    max_leaders = max(k['leaders'] for k in per_broker_data.values())

    assert (max_leaders - min_leaders) <= allowed_delta_leaders

    min_total = min(k['total'] for k in per_broker_data.values())
    max_total = max(k['total'] for k in per_broker_data.values())

    assert (max_total - min_total) <= allowed_delta_total


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


def test_rebalance_with_dead_brokers():
    distribution = {
        ('t0', '0'): ['2', '1'],
        ('t0', '1'): ['1', '2'],
        ('t0', '2'): ['1', '2'],
        ('t0', '3'): ['1', '2'],
        ('t0', '4'): ['1', '2'],
        ('t0', '5'): ['1', '2'],
        ('t0', '6'): ['1', '2'],
    }
    _, zk = __create_zk_for_topics(distribution, broker_ids=['1', '3'])
    o = RebalanceChange(zk, ['1', '3'])
    while o.run([]):
        pass
    __verify_balanced(['1', '3'], distribution)


def test_rebalance_with_many_topics():
    distribution = {}
    topic_count = 1000
    partition_count = 8
    for i in range(0, topic_count):
        topic = 't{}'.format(i)
        distribution.update({(topic, str(partition)): ['1', '2', '3'] for partition in range(0, partition_count)})
    _, zk = __create_zk_for_topics(distribution, broker_ids=['1', '2', '3', '4', '5'])

    o = RebalanceChange(zk, ['1', '2', '3', '4', '5'])
    steps = 0
    while o.run([]):
        steps += 1
    __verify_balanced(['1', '2', '3', '4', '5'], distribution, allowed_delta_leaders=5, allowed_delta_total=30)
    # Minimum rebalance steps
    distribution_count = 30
    assert steps == int(1 + topic_count * partition_count * (distribution_count - 1) / distribution_count)


def test_rebalance_invoked_on_broker_list_change():
    zk = MagicMock()

    zk.get = MagicMock(side_effect=NoNodeError)

    check = RebalanceOnBrokerListChange(zk, MagicMock())
    zk.get_broker_ids.return_value = ['1', '2', '3']

    assert check.check() is not None
    assert check.check() is None
    zk.get_broker_ids.return_value = ['1', '2', '3']
    assert check.check() is None
    zk.get_broker_ids.return_value = ['1', '2', '4']
    assert check.check() is not None
    assert check.check() is None


def test_combine_broker_ids():
    assert ['1'] == combine_broker_ids(['1'], 1)
    assert ['1', '2'] == combine_broker_ids(['1', '2'], 1)
    assert ['1', '2'] == combine_broker_ids(['1', '2'], 1)
    assert ['1,2', '2,1'] == combine_broker_ids(['1', '2'], 2)

    assert ['1,2', '1,3', '2,1', '2,3', '3,1', '3,2'] == combine_broker_ids(['1', '2', '3'], 2)
    assert ['1,2,3', '2,1,3', '3,1,2'] == combine_broker_ids(['1', '2', '3'], 3)
    assert [
               '1,2,3', '1,2,4', '1,3,4',
               '2,1,3', '2,1,4', '2,3,4',
               '3,1,2', '3,1,4', '3,2,4',
               '4,1,2', '4,1,3', '4,2,3',
           ] == combine_broker_ids(['1', '2', '3', '4'], 3)
