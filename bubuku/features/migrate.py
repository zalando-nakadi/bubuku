import logging

from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.migrate')


class MigrationChange(BaseRebalanceChange):
    def __init__(self, zk: BukuExhibitor, from_: list, to: list, shrink: bool, parallelism: int = 1):
        self.zk = zk
        self.migration = {int(from_[i]): int(to[i]) for i in range(0, len(from_))}
        self.shrink = shrink
        self.data_to_migrate = None
        self.parallelism = parallelism

    def run(self, current_actions) -> bool:
        if self.should_be_paused(current_actions):
            return True
        if self.zk.is_rebalancing():
            return True
        active_ids = [int(k) for k in self.zk.get_broker_ids()]
        if any(b not in active_ids for b in self.migration.keys()):
            _LOG.error('Source brokers {} are not in active list {}. Stopping.'.format(
                self.migration.keys(), active_ids))
            return False
        if any(b not in active_ids for b in self.migration.values()):
            _LOG.error('Target brokers {} are not in active list {}. Stopping.'.format(
                self.migration.values(), active_ids))
            return False
        if self.data_to_migrate is None:
            _LOG.info('Loading partition assignment')
            self.data_to_migrate = [data for data in self.zk.load_partition_assignment()]
            _LOG.info('Load {} partitions'.format(len(self.data_to_migrate)))
            return True

        items_to_migrate = []
        while self.data_to_migrate and len(items_to_migrate) < self.parallelism:
            topic, partition, replicas = self.data_to_migrate.pop()
            replaced_replicas = self._replace_replicas(replicas)
            if replaced_replicas == replicas:
                continue
            items_to_migrate.append((topic, partition, replicas, replaced_replicas))
        if not items_to_migrate:
            return False
        if not self.zk.reallocate_partitions([(t, p, rr) for t, p, _, rr in items_to_migrate]):
            for topic, partition, replicas, _ in items_to_migrate:
                self.data_to_migrate.append((topic, partition, replicas))
        return True

    def __str__(self):
        return 'Migration links {}, shrink: {}, data_to_move: {}, parallelism: {}'.format(
            self.migration,
            self.shrink,
            len(self.data_to_migrate) if self.data_to_migrate is not None else 'Unknown',
            self.parallelism,
        )

    def _replace_replicas(self, replicas):
        replacement = [self.migration[k] for k in replicas if k in self.migration]
        if self.shrink:
            result = []
            for v in replicas:
                to_use = self.migration.get(v, v)
                if to_use not in result:
                    result.append(to_use)
            return result
        else:
            return replicas + [k for k in replacement if k not in replicas]
