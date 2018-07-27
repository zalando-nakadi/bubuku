import functools
import unittest
from typing import Dict
from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError

from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.features.rebalance.change import OptimizedRebalanceChange
from bubuku.features.rebalance.change_simple import SimpleRebalancer
from bubuku.features.rebalance.check import RebalanceOnBrokerListCheck
from bubuku.zookeeper import BukuExhibitor


def _verify_balanced(broker_ids, distribution, delta=1):
    per_broker_data = {k: {'leaders': 0, 'total': 0} for k in broker_ids}
    for broker_ids in distribution.values():
        per_broker_data[broker_ids[0]]['leaders'] += 1
        for b in broker_ids:
            per_broker_data[b]['total'] += 1
            assert len([bb for bb in broker_ids if bb == b]) == 1
    min_leaders = min(k['leaders'] for k in per_broker_data.values())
    max_leaders = max(k['leaders'] for k in per_broker_data.values())

    assert (max_leaders - min_leaders) <= delta

    min_total = min(k['total'] for k in per_broker_data.values())
    max_total = max(k['total'] for k in per_broker_data.values())

    assert (max_total - min_total) <= delta


def _verify_rack_aware(initial_distribution, final_distribution, racks):
    for (topic, partition) in initial_distribution.keys():
        final_assignment = final_distribution[(topic, partition)]
        final_racks = _brokers_to_racks(final_assignment, racks)
        if len(racks) > len(final_assignment):
            assert (len(final_assignment) == len(set(final_racks)))
        else:
            assert (len(set(final_assignment)) == len(racks))


def _brokers_to_racks(brokers: list, racks: Dict[int, str]):
    return [racks[int(broker)] for broker in brokers]


def _verify_empty_brokers(broker_ids, distribution):
    for brokers in distribution.values():
        for broker in brokers:
            if broker in broker_ids:
                assert False
    assert True


class TestRebalanceCheck(unittest.TestCase):

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


class TestBaseRebalance(unittest.TestCase):
    __test__ = False

    _correct_rack_assignment = True

    def createChange(self, zk, broker_ids, empty_brokers, exclude_topics, parallelism=1) -> BaseRebalanceChange:
        pass

    def _create_zk_for_topics(self, topic_data, broker_ids=None, racks=None) -> (list, BukuExhibitor):
        buku_proxy = MagicMock()
        actual_broker_ids = broker_ids if broker_ids else sorted(list(
            set(functools.reduce(lambda x, y: x + y, topic_data.values(), []))))
        buku_proxy.get_broker_ids.return_value = actual_broker_ids
        brokers = broker_ids if broker_ids else sorted(list(
            set(functools.reduce(lambda x, y: x + y, topic_data.values(), []))))
        if self._correct_rack_assignment:
            buku_proxy.get_broker_racks.return_value = {k: v for k, v in racks.items() if str(k) in actual_broker_ids} \
                if racks else {int(broker_id): None for broker_id in brokers}
        else:
            buku_proxy.get_broker_racks.return_value = racks if racks else {int(broker_id): None for broker_id in
                                                                            brokers}

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

        def _reassign_many(items):
            for item in items:
                _reassign(*item)
            return True

        buku_proxy.reallocate_partition = _reassign
        buku_proxy.reallocate_partitions = _reassign_many
        return sorted(list(set(functools.reduce(lambda x, y: x + y, topic_data.values(), [])))), buku_proxy

    def test_rebalance_can_run(self):
        brokers, zk = self._create_zk_for_topics({})
        o = self.createChange(zk, [], [], [])

        blocked_actions = ['restart', 'start', 'stop', 'rebalance']

        # Check that can run in exact cases
        for a in blocked_actions:
            assert not o.can_run([a])

        assert o.can_run(['xxx'])
        assert o.can_run([])

    def test_rebalance_get_name(self):
        brokers, zk = self._create_zk_for_topics({})
        o = self.createChange(zk, [], [], [])
        assert o.get_name() == 'rebalance'

    def test_rebalance_on_empty1(self):
        brokers, zk = self._create_zk_for_topics({})
        o = self.createChange(zk, brokers, [], [])
        while o.run([]):
            pass

    def test_rebalance_on_filled1(self):
        distribution = {
            ('t0', '0'): ['2'],
            ('t0', '1'): ['1'],
            ('t0', '2'): ['1'],
            ('t0', '3'): ['1'],
        }
        brokers, zk = self._create_zk_for_topics(distribution)
        o = self.createChange(zk, brokers, [], [])
        # broker to partitions
        while o.run([]):
            pass

        _verify_balanced(('1', '2'), distribution)

    def test_rebalance_with_racks(self):
        distribution = {
            ('t0', '0'): ['3', '1'],
            ('t0', '1'): ['1', '5'],
            ('t0', '2'): ['5', '3'],
            ('t1', '0'): ['5', '3'],
            ('t1', '1'): ['3', '1'],
            ('t2', '0'): ['1', '5'],
            ('t2', '1'): ['2', '4'],
            ('t2', '2'): ['2', '6'],
            ('t2', '3'): ['4', '2'],
            ('t2', '4'): ['6', '2'],
            ('t2', '5'): ['3', '6'],
            ('t2', '6'): ['6', '3'],
        }

        initial_distribution = dict(distribution)

        racks = {
            1: 'r1',
            2: 'r1',
            3: 'r2',
            4: 'r2',
            5: 'r3',
            6: 'r3'
        }

        brokers, zk = self._create_zk_for_topics(distribution, ['1', '2', '3', '4', '5', '6'], racks)
        o = self.createChange(zk, brokers, [], [])
        while o.run([]):
            pass

        _verify_balanced(('1', '2', '3', '4', '5', '6'), distribution, 2)
        _verify_rack_aware(initial_distribution, distribution, racks)

    def test_rebalance_empty_one_broker(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['2', '3'],
            ('t1', '0'): ['2', '3'],
            ('t1', '1'): ['3', '4'],
        }
        brokers, zk = self._create_zk_for_topics(distribution)
        o = self.createChange(zk, brokers, ['2'], [])
        while o.run([]):
            pass

        _verify_empty_brokers(('2'), distribution)

    def test_rebalance_empty_multiple_brokers(self):
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
        brokers, zk = self._create_zk_for_topics(distribution)
        o = self.createChange(zk, brokers, ['2', '3'], [])
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
        brokers, zk = self._create_zk_for_topics(distribution)
        o = OptimizedRebalanceChange(zk, brokers, ['2', '3'], ['t1'])
        while o.run([]):
            pass

        assert distribution[('t1', '0')] == ['2', '3']
        assert distribution[('t1', '1')] == ['3', '4']
        assert distribution[('t1', '2')] == ['4', '5']

        distribution.pop(('t1', '0'))
        distribution.pop(('t1', '1'))
        distribution.pop(('t1', '2'))

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
        brokers, zk = self._create_zk_for_topics(distribution)
        o = self.createChange(zk, brokers, [], [])
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
        _, zk = self._create_zk_for_topics(distribution, broker_ids=['1', '3'], racks={1: None, 2: None, 3: None})
        o = self.createChange(zk, ['1', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '3'], distribution)

    def test_rebalance_fail_with_not_enough_replicas(self):
        distribution = {
            ('t0', '0'): ['2', '1', '3'],
            ('t0', '1'): ['1', '2'],
        }

        _, zk = self._create_zk_for_topics(distribution, broker_ids=['1', '3'])
        o = self.createChange(zk, ['1', '3'], [], [])
        try:
            while o.run([]):
                pass
            assert False, "Balancing can not work with low replication factor"
        except Exception:
            pass

    def test_rebalance_recovered_with_additional_copy2(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['3', '4']
        }
        _, zk = self._create_zk_for_topics(distribution, ['1', '2', '4'], racks={1: None, 2: None, 3: None, 4: None})
        o = self.createChange(zk, ['1', '2', '4'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '2', '4'], distribution)

    def test_rebalance_with_many_topics(self):
        distribution = {}
        topic_count = 1000
        partition_count = 21
        broker_ids = [str(i) for i in range(1, 22)]
        for i in range(0, topic_count):
            topic = 't{}'.format(i)
            distribution.update({(topic, str(partition)): ['1', '2', '3'] for partition in range(0, partition_count)})
        _, zk = self._create_zk_for_topics(distribution, broker_ids=broker_ids)

        o = self.createChange(zk, broker_ids, [], [], parallelism=1000)
        steps = 0
        while o.run([]):
            print(str(o))
            steps += 1
        _verify_balanced(broker_ids, distribution, 1)

    def test_leader_partition_limit(self):
        distribution = {
            ('t0', '0'): ['1', '2'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['1', '2'],
            ('t1', '2'): ['1', '2'],
        }
        _, zk = self._create_zk_for_topics(distribution, ['2', '3'], racks={1: None, 2: None, 3: None})
        o = self.createChange(zk, ['2', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['2', '3'], distribution)


class OptimizedRebalanceTest(TestBaseRebalance):
    __test__ = True
    _correct_rack_assignment = False

    def createChange(self, zk, broker_ids, empty_brokers, exclude_topics, parallelism=1):
        return OptimizedRebalanceChange(zk, broker_ids, empty_brokers, exclude_topics, parallelism)

    def test_rebalance_recovered_with_additional_copy1(self):
        distribution = {
            ('t0', '0'): ['2', '1'],
            ('t0', '1'): ['1', '2'],
            ('t0', '2'): ['3', '4']
        }
        _, zk = self._create_zk_for_topics(distribution, ['1', '2', '3'], racks={1: None, 2: None, 3: None, 4: None})
        o = self.createChange(zk, ['1', '2', '3'], [], [])
        while o.run([]):
            pass
        _verify_balanced(['1', '2', '3'], distribution)


class SimpleRebalanceTest(TestBaseRebalance):
    __test__ = True

    def createChange(self, zk, broker_ids, empty_brokers, exclude_topics, parallelism=1):
        return SimpleRebalancer(zk,
                                broker_ids=broker_ids,
                                empty_brokers=empty_brokers,
                                exclude_topics=exclude_topics,
                                parallelism=parallelism)

    def test_rebalance_with_racks_different_nr_partitions_per_rack(self):
        distribution = {
            ('t0', '0'): ['3', '1'],
            ('t0', '1'): ['6', '5'],
            ('t0', '2'): ['5', '3'],
            ('t1', '0'): ['5', '6'],
            ('t1', '1'): ['6', '5'],
            ('t2', '0'): ['6', '4'],
            ('t2', '1'): ['2', '6'],
            ('t2', '2'): ['2', '6'],
            ('t2', '3'): ['4', '2'],
            ('t2', '4'): ['6', '5'],
            ('t2', '5'): ['4', '6'],
            ('t2', '6'): ['6', '4'],
        }

        initial_distribution = dict(distribution)

        racks = {
            1: 'r1',
            2: 'r1',
            3: 'r2',
            4: 'r2',
            5: 'r3',
            6: 'r3'
        }

        brokers, zk = self._create_zk_for_topics(distribution, ['1', '2', '3', '4', '5', '6'], racks)
        o = self.createChange(zk, brokers, [], [])
        while o.run([]):
            print(o)
            pass

        _verify_rack_aware(initial_distribution, distribution, racks)
