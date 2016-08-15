import unittest
from unittest.mock import MagicMock

from bubuku.features.migrate import MigrationChange


class TestMigrate(unittest.TestCase):
    def test_migration_all_steps(self):
        partitions = {
            ('test', 0): [1, 2, 3],
            ('test', 1): [2, 3, 1],
            ('test1', 0): [3, 2, 1],
        }
        zk = MagicMock()
        zk.is_rebalancing = lambda: False
        zk.load_partition_assignment = lambda: [(k[0], k[1], v) for k, v in partitions.items()]
        result = {}

        def _reallocate_partition(t, p, r):
            result.update({(t, p): r})
            return True

        zk.reallocate_partition = _reallocate_partition
        zk.get_broker_ids = lambda: [1, 2, 3, 4, 5, 6]

        change = MigrationChange(zk, [1, 2, 3], [4, 5, 6], False)
        while change.run([]):
            pass
        expected = {
            ('test', 0): [1, 2, 3, 4, 5, 6],
            ('test', 1): [2, 3, 1, 5, 6, 4],
            ('test1', 0): [3, 2, 1, 6, 5, 4],
        }
        assert expected == result

        zk.load_partition_assignment = lambda: [(k[0], k[1], v) for k, v in expected.items()]
        result.clear()

        change = MigrationChange(zk, [1, 2, 3], [4, 5, 6], True)
        while change.run([]):
            pass

        expected = {
            ('test', 0): [4, 5, 6],
            ('test', 1): [5, 6, 4],
            ('test1', 0): [6, 5, 4],
        }

        assert expected == result

    def test_replica_generation_no_shrink(self):
        change = MigrationChange(MagicMock(), [1, 2, 3], [4, 5, 6], False)

        assert [4, 5, 6] == change._replace_replicas([4, 5, 6])
        assert [1, 2, 3, 4, 5, 6] == change._replace_replicas([1, 2, 3])
        assert [1, 2, 6, 4, 5] == change._replace_replicas([1, 2, 6])
        assert [1, 6, 2, 4, 5] == change._replace_replicas([1, 6, 2])
        assert [1, 6, 3, 4] == change._replace_replicas([1, 6, 3])

    def test_replica_generation_shrink(self):
        change = MigrationChange(MagicMock(), [1, 2, 3], [4, 5, 6], True)

        assert [4, 5, 6] == change._replace_replicas([1, 2, 3])
        assert [4, 5, 6] == change._replace_replicas([4, 2, 6])
        assert [8, 5, 10] == change._replace_replicas([8, 2, 10])
        assert [4, 8, 5] == change._replace_replicas([1, 8, 2])
        assert [4, 5, 6] == change._replace_replicas([1, 2, 3, 4, 5, 6])
