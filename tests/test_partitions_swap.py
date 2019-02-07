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
        ("t1", 1, [111, 333]),
        ("t1", 2, [111, 222]),
        ("t2", 1, [222, 111]),
        ("t2", 2, [222, 333]),
        ("t3", 1, [333, 111]),
        ("t3", 2, [333, 222]),
    ]

    test_broker_racks_unaware = {
        111: None,
        222: None,
        333: None
    }

    test_size_stats_nine = {
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
        }},
        "444": {"disk": {"free_kb": 21000, "used_kb": 19000}, "topics": {
            "t4": {"1": 3434, "2": 200},
            "t5": {"1": 1000},
            "t6": {"1": 300}
        }},
        "555": {"disk": {"free_kb": 10000, "used_kb": 30000}, "topics": {
            "t4": {"2": 200},
            "t5": {"1": 1000, "2": 100},
            "t6": {"2": 2000}
        }},
        "666": {"disk": {"free_kb": 22000, "used_kb": 18000}, "topics": {
            "t4": {"1": 3434},
            "t5": {"2": 100},
            "t6": {"1": 300, "2": 2000}
        }},
        "777": {"disk": {"free_kb": 23000, "used_kb": 17000}, "topics": {
            "t7": {"1": 3434, "2": 200},
            "t8": {"1": 1000},
            "t9": {"1": 300}
        }},
        "888": {"disk": {"free_kb": 24000, "used_kb": 16000}, "topics": {
            "t7": {"2": 200},
            "t8": {"1": 1000, "2": 100},
            "t9": {"2": 2000}
        }},
        "999": {"disk": {"free_kb": 26000, "used_kb": 14000}, "topics": {
            "t7": {"1": 3434},
            "t8": {"2": 100},
            "t9": {"1": 300, "2": 2000}
        }}
    }

    test_assignment_nine = [
        ("t1", 1, [111, 333]),
        ("t1", 2, [111, 222]),
        ("t2", 1, [222, 111]),
        ("t2", 2, [222, 333]),
        ("t3", 1, [333, 111]),
        ("t3", 2, [333, 222]),
        ("t4", 1, [444, 666]),
        ("t4", 2, [444, 555]),
        ("t5", 1, [555, 444]),
        ("t5", 2, [555, 666]),
        ("t6", 1, [666, 444]),
        ("t6", 2, [666, 555]),
        ("t7", 1, [777, 999]),
        ("t7", 2, [777, 888]),
        ("t8", 1, [888, 777]),
        ("t8", 2, [888, 999]),
        ("t9", 1, [999, 777]),
        ("t9", 2, [999, 888]),
    ]

    test_broker_racks_aware = {
        111: "eu-central-1a",
        222: "eu-central-1b",
        333: "eu-central-1c",
        444: "eu-central-1a",
        555: "eu-central-1b",
        666: "eu-central-1c",
        777: "eu-central-1a",
        888: "eu-central-1b",
        999: "eu-central-1c",
    }

    def setUp(self):
        self.zk = self.__mock_zk()
        self.broker = self.__mock_broker()

    def test_check_requires_swap_partitions_change(self):
        check_imbalance = CheckBrokersDiskImbalance(self.zk, self.broker, 3000, -1)
        change = check_imbalance.check()

        assert change

    def test_self_fat_slim_brokers_rack_aware(self):
        zk = self.__mock_zk_rack()

        slim, fat, gap, stats = load_swap_data(zk, -1, 100)
        assert fat == 555
        assert slim == 222

    def test_check_requires_swap_partitions_change_rack_aware(self):
        self.zk = self.__mock_zk_rack()
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
        self.zk.reallocate_partitions.assert_called_with([('t2', 2, [222, 111]), ('t2', 1, [222, 333])])

    def test_swap_partitions_change_not_performed(self):
        swap_change = SwapPartitionsChange(self.zk, lambda x: load_swap_data(x, -1, 10001))
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
        assert swap_change.to_move == [('t2', 2, [222, 111]), ('t2', 1, [222, 333])]

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
        zk.get_broker_racks.return_value = self.test_broker_racks_unaware
        return zk

    def __mock_zk_rack(self) -> MagicMock:
        zk = MagicMock()
        zk.is_rebalancing.return_value = False
        zk.load_partition_assignment.return_value = self.test_assignment_nine
        zk.get_disk_stats.return_value = self.test_size_stats_nine
        zk.get_broker_racks.return_value = self.test_broker_racks_aware
        return zk
