import logging

from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.swap_leadership')


class SwapLeadershipChange(BaseRebalanceChange):
    def __init__(self, zk: BukuExhibitor, loaded_id: int, empty_id: int):
        self.zk = zk
        self.loaded_id = loaded_id
        self.empty_id = empty_id

    def run(self, current_actions) -> bool:
        # Stop rebalance if someone is restarting
        if self.should_be_paused(current_actions):
            _LOG.warning("Rebalance paused, because other blocking events running: {}".format(current_actions))
            return True
        if self.zk.is_rebalancing():
            return True

        list_1 = []
        list_2 = []

        for topic, partition, replicas in self.zk.load_partition_assignment():
            if replicas[0] == self.loaded_id and self.empty_id in replicas:
                list_1.append((topic, partition, tuple(replicas)))
            elif replicas[0] == self.empty_id and self.loaded_id in replicas:
                list_2.append((topic, partition, tuple(replicas)))

        sizes = self.zk.get_disk_stats()
        sizes_1 = sizes[str(self.loaded_id)]['topics']
        sizes_2 = sizes[str(self.empty_id)]['topics']
        list_1 = sorted(list_1, key=lambda v: sizes_1[v[0]][str(v[1])], reverse=True)
        list_2 = sorted(list_2, key=lambda v: sizes_2[v[0]][str(v[1])])

        if list_1 and list_2:
            data = [(topic, partition, self.swap_replicas(replicas))
                    for topic, partition, replicas in [list_1[0], list_2[0]]]
            self.zk.reallocate_partitions(data)
        else:
            _LOG.warning("Failed to find direct candidate for reallocation")
        return False

    def swap_replicas(self, replicas):
        def __swap_v(v):
            if v == self.loaded_id:
                return self.empty_id
            elif v == self.empty_id:
                return self.loaded_id
            return v

        return [__swap_v(v) for v in replicas]
