import unittest
from unittest.mock import MagicMock

from bubuku.features.swap_partitions import CheckBrokersDiskImbalance, SwapPartitionsChange, load_swap_data


class TestPartitionsSwap(unittest.TestCase):
    test_size_stats = {
        "111": {"disk": {"free_kb": 20000, "used_kb": 20000}, "topics": {
            "t1": {"1": 3434, "2": 200},
            "t2": {"1": 1000},
            "t3": {"1": 300}
        }},
        "222": {"disk": {"free_kb": 25000, "used_kb": 15000}, "topics": {
            "t1": {"2": 200},
            "t2": {"1": 1000, "2": 100},
            "t3": {"2": 2000}
        }},
        "333": {"disk": {"free_kb": 30000, "used_kb": 10000}, "topics": {
            "t1": {"1": 3434},
            "t2": {"2": 100},
            "t3": {"1": 300, "2": 2000}
        }}
    }

    test_assignment = [
        ("t1", 1, [333, 111]),
        ("t1", 2, [111, 222]),
        ("t2", 1, [111, 222]),
        ("t2", 2, [222, 333]),
        ("t3", 1, [333, 111]),
        ("t3", 2, [222, 333]),
    ]

    def setUp(self):
        self.zk = self.__mock_zk()
        self.broker = self.__mock_broker()

    def test_check_requires_swap_partitions_change(self):
        check_imbalance = CheckBrokersDiskImbalance(self.zk, self.broker, 3000, -1)
        change = check_imbalance.check()

        assert change

    def test_check_requires_not_swap_partitions_change(self):
        check_imbalance = CheckBrokersDiskImbalance(self.zk, self.broker, 15000, -1)
        change = check_imbalance.check()

        # change should not be created as the gap between brokers is less than threshold
        assert not change

    def test_swap_partitions_change_performed(self):
        def _swap_data_provider(zk):
            return load_swap_data(zk, -1, 10000)

        swap_change = SwapPartitionsChange(self.zk, _swap_data_provider)
        result = swap_change.run([])

        assert not result
        self.zk.reallocate_partitions.assert_called_with([('t2', 2, [111, 222]), ('t2', 1, [222, 333])])

    def test_swap_partitions_change_not_performed(self):
        swap_change = SwapPartitionsChange(self.zk, lambda x: load_swap_data(x, -1, 80))
        result = swap_change.run([])

        # change should not trigger partitions swap as there is no possible
        # partitions swap that will decrease the gap between brokers
        assert not result
        self.zk.reallocate_partitions.assert_not_called()

    def test_swap_partitions_change_postponed(self):
        self.zk.reallocate_partitions.return_value = False

        swap_change = SwapPartitionsChange(self.zk, lambda x: load_swap_data(x, -1, 10000))
        result = swap_change.run([])

        # if the write to ZK wasn't possible for some reason, the change should
        # return True and repeat write to ZK during next trigger by controller
        assert result
        assert swap_change.to_move == [('t2', 2, [111, 222]), ('t2', 1, [222, 333])]

    def test_swap_partitions_change_postponed_when_rebalancing(self):
        self.zk.is_rebalancing.return_value = True

        swap_change = SwapPartitionsChange(self.zk, None)
        result = swap_change.run([])

        # if there was a rebalance node in ZK - the change should be postponed
        assert result
        assert not swap_change.to_move

    def test_swap_partitions_change_performed_existing(self):
        swap_change = SwapPartitionsChange(self.zk, None)
        dummy_move_list = ["dummy"]
        swap_change.to_move = ["dummy"]
        result = swap_change.run([])

        # if there already was a pair of partitions to swap in to_move
        # property - SwapPartitionsChange should just execute this swap
        assert not result
        self.zk.reallocate_partitions.assert_called_with(dummy_move_list)
        self.zk.load_partition_assignment.assert_not_called()

    def __mock_broker(self) -> MagicMock:
        broker = MagicMock()
        broker.is_running_and_registered.return_value = True
        return broker

    def __mock_zk(self) -> MagicMock:
        zk = MagicMock()
        zk.is_rebalancing.return_value = False
        zk.load_partition_assignment.return_value = self.test_assignment
        zk.get_disk_stats.return_value = self.test_size_stats
        return zk
