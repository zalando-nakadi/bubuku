import unittest
from unittest.mock import MagicMock
from bubuku.features.swap_partitions import CheckBrokersDiskImbalance, SwapPartitionsChange


class TestPartitionsSwap(unittest.TestCase):
    test_size_stats = {
        "broker1": {"disk": {"free_kb": 20000, "used_kb": 20000}, "topics": {
            "t1": {"p1": 3434, "p2": 200},
            "t2": {"p1": 1000},
            "t3": {"p1": 300}
        }},
        "broker2": {"disk": {"free_kb": 25000, "used_kb": 15000}, "topics": {
            "t1": {"p2": 200},
            "t2": {"p1": 1000, "p2": 100},
            "t3": {"p2": 2000}
        }},
        "broker3": {"disk": {"free_kb": 30000, "used_kb": 10000}, "topics": {
            "t1": {"p1": 3434},
            "t2": {"p2": 100},
            "t3": {"p1": 300, "p2": 2000}
        }}
    }

    test_assignment = [
        ("t1", "p1", ["broker3", "broker1"]),
        ("t1", "p2", ["broker1", "broker2"]),
        ("t2", "p1", ["broker2", "broker1"]),
        ("t2", "p2", ["broker2", "broker3"]),
        ("t3", "p1", ["broker3", "broker1"]),
        ("t3", "p2", ["broker2", "broker3"]),
    ]

    def test_check_requires_swap_partitionschange(self):
        zk = MagicMock()
        zk.get_disk_stats.return_value = self.test_size_stats

        check_imbalance = CheckBrokersDiskImbalance(zk, self.__mock_broker(), 3000)
        change = check_imbalance.check()

        assert change
        assert change.fat_broker_id == "broker1"
        assert change.slim_broker_id == "broker3"
        assert change.gap == 10000
        assert change.size_stats == self.test_size_stats

    def test_check_requires_not_swap_partitions_change(self):
        zk = MagicMock()
        zk.get_disk_stats.return_value = self.test_size_stats

        check_imbalance = CheckBrokersDiskImbalance(zk, self.__mock_broker(), 15000)
        change = check_imbalance.check()

        # change should not be created as the gap between brokers is less than threshold
        assert not change

    def test_swap_partitions_change_performed(self):
        zk = MagicMock()
        zk.load_partition_assignment.return_value = self.test_assignment

        swap_change = SwapPartitionsChange(zk, "broker1", "broker3", 10000, self.test_size_stats)
        result = swap_change.run([])

        assert not result
        zk.reallocate_partitions.assert_called_with(
            [('t2', 'p2', ['broker2', 'broker1']), ('t2', 'p1', ['broker2', 'broker3'])])

    def test_swap_partitions_change_not_performed(self):
        zk = MagicMock()
        zk.load_partition_assignment.return_value = self.test_assignment

        swap_change = SwapPartitionsChange(zk, "broker1", "broker3", 80, self.test_size_stats)
        result = swap_change.run([])

        # change should not trigger partitions swap as there is no possible
        # partitions swap that will decrease the gap between brokers
        assert not result
        zk.reallocate_partitions.assert_not_called()

    def test_swap_partitions_change_postponed(self):
        zk = MagicMock()
        zk.load_partition_assignment.return_value = self.test_assignment
        zk.reallocate_partitions.return_value = False

        swap_change = SwapPartitionsChange(zk, "broker1", "broker3", 10000, self.test_size_stats)
        result = swap_change.run([])

        # if the write to ZK wasn't possible for some reason, the change should
        # return True and repeat write to ZK during next trigger by controller
        assert result
        assert swap_change.to_move == [('t2', 'p2', ['broker2', 'broker1']), ('t2', 'p1', ['broker2', 'broker3'])]

    def test_swap_partitions_change_performed_existing(self):
        zk = MagicMock()
        swap_change = SwapPartitionsChange(zk, None, None, None, None)

        dummy_move_list = ["dummy"]
        swap_change.to_move = ["dummy"]
        result = swap_change.run([])

        # if there already was a pair of partitions to swap in to_move
        # property - SwapPartitionsChange should just execute this swap
        assert not result
        zk.reallocate_partitions.assert_called_with(dummy_move_list)
        zk.load_partition_assignment.assert_not_called()

    def __mock_broker(self) -> MagicMock:
        broker = MagicMock()
        broker.is_running_and_registered.return_value = True
        return broker
