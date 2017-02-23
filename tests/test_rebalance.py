import functools
import pytest
import unittest
from time import sleep
from unittest.mock import MagicMock

import requests
from kazoo.exceptions import NoNodeError

from bubuku.features.rebalance.change import OptimizedRebalanceChange
from bubuku.features.rebalance.check import RebalanceOnBrokerListCheck
from bubuku.zookeeper import BukuExhibitor, AddressListProvider, _ZookeeperProxy


def _create_zk_for_topics(topic_data, broker_ids=None) -> (list, BukuExhibitor):
    buku_proxy = MagicMock()
    buku_proxy.get_broker_ids.return_value = broker_ids if broker_ids else sorted(list(
        set(functools.reduce(lambda x, y: x + y, topic_data.values(), []))))

    def _load_assignment():
        return [(k[0], int(k[1]), [int(p) for p in v]) for k, v in topic_data.items()]

    def _load_states(topics=None):
        return [(k[0], int(k[1]), {'isr': [int(p) for p in v]}) for k, v in topic_data.items()]

    buku_proxy.load_partition_assignment = _load_assignment
    buku_proxy.load_partition_states = _load_states
    buku_proxy.is_rebalancing.return_value = False

    def _reassign(topic, partition, replicas):
        topic_data[(topic, str(partition))] = [str(x) for x in replicas]
        return True

    buku_proxy.reallocate_partition = _reassign
    return sorted(list(set(functools.reduce(lambda x, y: x + y, topic_data.values(), [])))), buku_proxy


def _verify_balanced(broker_ids, distribution):
    per_broker_data = {k: {'leaders': 0, 'total': 0} for k in broker_ids}
    for broker_ids in distribution.values():
        per_broker_data[broker_ids[0]]['leaders'] += 1
        for b in broker_ids:
            per_broker_data[b]['total'] += 1
            assert len([bb for bb in broker_ids if bb == b]) == 1
    min_leaders = min(k['leaders'] for k in per_broker_data.values())
    max_leaders = max(k['leaders'] for k in per_broker_data.values())

    assert (max_leaders - min_leaders) <= 1

    min_total = min(k['total'] for k in per_broker_data.values())
    max_total = max(k['total'] for k in per_broker_data.values())

    assert (max_total - min_total) <= 1

def _verify_empty_brokers(broker_ids, distribution):
    for brokers in distribution.values():
        for broker in brokers:
            if broker in broker_ids:
                assert False
    assert True

class TestRebalance(unittest.TestCase):
    def test_rebalance_can_run(self):
        o = OptimizedRebalanceChange(object(), [], [], [])

        blocked_actions = ['restart', 'start', 'stop', 'rebalance']

        # Check that can run in exact cases
        for a in blocked_actions:
            assert not o.can_run([a])

        assert o.can_run(['xxx'])
        assert o.can_run([])

    def test_rebalance_get_name(self):
        o = OptimizedRebalanceChange(object(), [], [], [])
        assert o.get_name() == 'rebalance'

    def test_rebalance_on_empty1(self):
        brokers, zk = _create_zk_for_topics({})
        o = OptimizedRebalanceChange(zk, brokers, [], [])
        while o.run([]):
            pass

    def test_rebalance_on_filled1(self):
        distribution = {
            ('t0', '0'): ['2'],
            ('t0', '1'): ['1'],
            ('t0', '2'): ['1'],
            ('t0', '3'): ['1'],
        }
        brokers, zk = _create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, [], [])
        # broker to partitions
        while o.run([]):
            pass

        _verify_balanced(('1', '2'), distribution)

    def test_rebalance_exclude_one_broker(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['2', '3'],
            ('t1', '0'): ['2', '3'],
            ('t1', '1'): ['3', '4'],
        }
        brokers, zk = _create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, ['2'], [])
        while o.run([]):
            pass

        _verify_empty_brokers(('2'), distribution)

    def test_rebalance_exclude_multiple_brokers(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['2', '3'],
            ('t1', '0'): ['2', '3'],
            ('t1', '1'): ['3', '4'],
            ('t1', '2'): ['4', '5'],
            ('t2', '0'): ['3', '4'],
            ('t2', '1'): ['4', '5'],
            ('t2', '2'): ['5', '6'],
        }
        brokers, zk = _create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, ['2','3'], [])
        while o.run([]):
            pass

        _verify_empty_brokers(('2', '3'), distribution)

    def test_rebalance_empty_brokers_and_exclude_topics(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['2', '3'],
            ('t1', '0'): ['2', '3'],
            ('t1', '1'): ['3', '4'],
            ('t1', '2'): ['4', '5'],
            ('t2', '0'): ['3', '4'],
            ('t2', '1'): ['4', '5'],
            ('t2', '2'): ['5', '6'],
        }
        brokers, zk = _create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, ['2','3'], ['t1'])
        while o.run([]):
            pass

        assert distribution[('t1', '0')] == ['2', '3']
        assert distribution[('t1', '1')] == ['3', '4']
        assert distribution[('t1', '2')] == ['4', '5']

        distribution.pop(('t1', '0'))
        distribution.pop(('t1', '1'))
        distribution.pop(('t1', '2'))

        print(distribution.values())
        brokers = [item for sublist in distribution.values() for item in sublist]
        assert '2' not in brokers
        assert '3' not in brokers

    def test_rebalance_on_filled2(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['1', '2'],
            ('t0', '3'): ['1', '2'],
            ('t0', '4'): ['1', '2'],
            ('t0', '5'): ['1', '2'],
            ('t0', '6'): ['1', '2'],
        }
        brokers, zk = _create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, [], [])
        # broker to partitions
        while o.run([]):
            pass

        _verify_balanced(('1', '2'), distribution)

    def test_rebalance_with_dead_brokers(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['1', '2'],
            ('t0', '3'): ['1', '2'],
            ('t0', '4'): ['1', '2'],
            ('t0', '5'): ['1', '2'],
            ('t0', '6'): ['1', '2'],
        }
        _, zk = _create_zk_for_topics(distribution, broker_ids=['1', '3'])
        o = OptimizedRebalanceChange(zk, ['1', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '3'], distribution)

    def test_rebalance_fail_with_not_enough_replicas(self):
        distribution = {
            ('t0', '0'): ['2', '1', '3'],
            ('t0', '1'): ['1', '2'],
        }

        _, zk = _create_zk_for_topics(distribution, broker_ids=['1', '3'])
        o = OptimizedRebalanceChange(zk, ['1', '3'], [], [])
        try:
            while o.run([]):
                pass
            assert False, "Balancing can not work with low replication factor"
        except Exception:
            pass

    def test_rebalance_recovered_with_additional_copy1(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['3', '4']
        }
        _, zk = _create_zk_for_topics(distribution, ['1', '2', '3'])
        o = OptimizedRebalanceChange(zk, ['1', '2', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '2', '3'], distribution)

    def test_rebalance_recovered_with_additional_copy2(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['3', '4']
        }
        _, zk = _create_zk_for_topics(distribution, ['1', '2', '4'])
        o = OptimizedRebalanceChange(zk, ['1', '2', '4'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '2', '4'], distribution)

    def test_rebalance_with_many_topics(self):
        distribution = {}
        topic_count = 1000
        partition_count = 8
        for i in range(0, topic_count):
            topic = 't{}'.format(i)
            distribution.update({(topic, str(partition)): ['1', '2', '3'] for partition in range(0, partition_count)})
        _, zk = _create_zk_for_topics(distribution, broker_ids=['1', '2', '3', '4', '5'])

        o = OptimizedRebalanceChange(zk, ['1', '2', '3', '4', '5'], [], [])
        steps = 0
        while o.run([]):
            steps += 1
        _verify_balanced(['1', '2', '3', '4', '5'], distribution)
        # Minimum rebalance steps
        # distribution_count = 30
        # assert steps == int(1 + topic_count * partition_count * (distribution_count - 1) / distribution_count)

    def test_rebalance_invoked_on_broker_list_change(self):
        zk = MagicMock()

        zk.get = MagicMock(side_effect=NoNodeError)

        check = RebalanceOnBrokerListCheck(zk, MagicMock())
        zk.get_broker_ids.return_value = ['1', '2', '3']

        assert check.check() is not None
        assert check.check() is None
        zk.get_broker_ids.return_value = ['1', '2', '3']
        assert check.check() is None
        zk.get_broker_ids.return_value = ['1', '2', '4']
        assert check.check() is not None
        assert check.check() is None

    def test_leader_partition_limit(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['1', '2'],
            ('t1', '2'): ['1', '2'],
        }
        _, zk = _create_zk_for_topics(distribution, ['2', '3'])
        o = OptimizedRebalanceChange(zk, ['2', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['2', '3'], distribution)
