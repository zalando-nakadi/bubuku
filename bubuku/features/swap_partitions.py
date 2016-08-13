import logging
from collections import namedtuple
from operator import attrgetter

from bubuku.broker import BrokerManager
from bubuku.controller import Check
from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.swap_partitions')

TpData = namedtuple('TpData', ('topic', 'partition', 'size', 'replicas'))


class SwapPartitionsChange(BaseRebalanceChange):
    def __init__(self, zk: BukuExhibitor, fat_broker_id: int, slim_broker_id: int, gap: int, size_stats: dict):
        self.zk = zk
        self.fat_broker_id = fat_broker_id
        self.slim_broker_id = slim_broker_id
        self.gap = gap
        self.size_stats = size_stats
        self.to_move = None

    def run(self, current_actions):
        if self.should_be_paused(current_actions):
            _LOG.info("Pausing swap partitions change as there are conflicting actions: {}".format(current_actions))
            return True
        try:
            _LOG.info("Running swap partitions change: {}".format(self))
            return self.__run_internal()
        except Exception:
            _LOG.warn("Error occurred when performing partitions swap change", exc_info=True)
            return False

    def __run_internal(self):
        # if there is already a swap which was postponed - just execute it
        if self.to_move:
            return not self.__perform_swap(self.to_move)

        # if there's a rebalance currently running - postpone current change
        if self.zk.is_rebalancing():
            return True

        # merge topics size stats to a single dict
        topics_stats = {}
        for _, broker_stats in self.size_stats.items():
            for topic in broker_stats["topics"].keys():
                if topic not in topics_stats:
                    topics_stats[topic] = {}
                topics_stats[topic].update(broker_stats["topics"][topic])

        # find partitions that are candidates to be swapped between "fat" and "slim" brokers
        swap_partition_candidates = self.__find_all_swap_candidates(self.fat_broker_id, self.slim_broker_id,
                                                                    topics_stats)

        # smallest partition from slim broker is the one we move to fat broker
        slim_broker_smallest_partition = min(swap_partition_candidates[self.slim_broker_id], key=attrgetter("size"))
        if not slim_broker_smallest_partition:
            _LOG.info("No partitions on slim broker(id: {}) found to swap".format(self.slim_broker_id))
            return False
        _LOG.info("Slim broker(id: {}) partition to swap: {}"
                  .format(self.slim_broker_id, slim_broker_smallest_partition))

        # find the best fitting fat broker partition to move to slim broker
        # (should be as much as possible closing the gap between brokers)
        fat_broker_swap_candidates = swap_partition_candidates[self.fat_broker_id]
        matching_swap_partition = self.__find_best_swap_candidate(fat_broker_swap_candidates, self.gap,
                                                                  slim_broker_smallest_partition.size)

        # if there is no possible swap that will decrease the gap - just do nothing
        if not matching_swap_partition:
            _LOG.info("No candidate from fat broker(id:{}) found to swap".format(self.fat_broker_id))
            return False
        _LOG.info("Fat broker(id: {}) partition to swap: {}".format(self.fat_broker_id, matching_swap_partition))

        # write rebalance-json to ZK; Kafka will read it and perform the partitions swap
        self.to_move = self.__create_rebalance_list(slim_broker_smallest_partition, self.slim_broker_id,
                                                    matching_swap_partition, self.fat_broker_id)
        scheduled_rebalance = self.__perform_swap(self.to_move)
        if scheduled_rebalance:
            _LOG.info("Swap partitions rebalance node was successfully created in ZK")
        else:
            _LOG.info("Swap partitions is postponed as there was a rebalance node in ZK")
        return not scheduled_rebalance

    def __perform_swap(self, rebalance_list):
        _LOG.info("Writing rebalance-json to ZK for partitions swap: {}".format(rebalance_list))
        return self.zk.reallocate_partitions(rebalance_list)

    def __find_all_swap_candidates(self, fat_broker_id: int, slim_broker_id: int, topics_stats: dict) -> dict:
        partition_assignment = self.zk.load_partition_assignment()
        swap_partition_candidates = {}
        for topic, partition, replicas in partition_assignment:
            if topic not in topics_stats or str(partition) not in topics_stats[topic]:
                continue  # we skip this partition as there is not data size stats for it

            if fat_broker_id in replicas and slim_broker_id in replicas:
                continue  # we skip this partition as it exists on both involved brokers

            for broker_id in [slim_broker_id, fat_broker_id]:
                if broker_id in replicas:
                    if broker_id not in swap_partition_candidates:
                        swap_partition_candidates[broker_id] = []
                    swap_partition_candidates[broker_id].append(
                        TpData(topic, partition, topics_stats[topic][str(partition)], replicas))
        return swap_partition_candidates

    @staticmethod
    def __find_best_swap_candidate(candidates: list, brokers_gap: int, partition_to_swap_size: int) -> TpData:
        candidates.sort(key=attrgetter("size"), reverse=True)
        matching_swap_partition = None
        smallest_new_gap = brokers_gap
        for tp in candidates:
            new_gap = abs(brokers_gap - 2 * abs(tp.size - partition_to_swap_size))
            if new_gap < smallest_new_gap:
                smallest_new_gap = new_gap
                matching_swap_partition = tp
        return matching_swap_partition

    def __create_rebalance_list(self, tp1: TpData, br1: int, tp2: TpData, br2: int) -> list:
        return [
            (tp1.topic, tp1.partition, self.__replace_broker(tp1.replicas, br1, br2, tp2.replicas[0] == br2)),
            (tp2.topic, tp2.partition, self.__replace_broker(tp2.replicas, br2, br1, tp1.replicas[0] == br1))
        ]

    def __replace_broker(self, replicas: list, broker_to_replace: int, replacement: int, was_leader: bool) -> list:
        rps = [x for x in replicas if x != broker_to_replace]
        if was_leader:
            return [replacement] + rps
        else:
            return rps + [replacement]

    def __str__(self):
        return 'SwapPartitions(fat_broker_id: {}, slim_broker_id: {}, gap: {})'.format(
            self.fat_broker_id, self.slim_broker_id, self.gap)


class CheckBrokersDiskImbalance(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, diff_threshold_kb: int):
        super().__init__(check_interval_s=900)
        self.zk = zk
        self.broker = broker
        self.diff_threshold_kb = diff_threshold_kb

    def check(self):
        if self.broker.is_running_and_registered():
            _LOG.info("Starting broker disk imbalance check")
            try:
                return self.create_swap_partition_change()
            except Exception:
                _LOG.warn("Error occurred when performing disk imbalance check", exc_info=True)
        return None

    def __str__(self):
        return 'CheckBrokersDiskImbalance'

    def create_swap_partition_change(self) -> SwapPartitionsChange:
        size_stats = self.zk.get_disk_stats()
        if not size_stats or len(size_stats.keys()) == 0:
            _LOG.info("No size stats available, imbalance check cancelled")
            return None

        # find the most "slim" and the most "fat" brokers
        def free_size_getter(tup):
            return tup[1]["disk"]["free_kb"]

        slim_broker_id = max((item for item in size_stats.items()), key=free_size_getter)[0]
        fat_broker_id = min((item for item in size_stats.items()), key=free_size_getter)[0]
        fat_broker_free_kb = size_stats[fat_broker_id]["disk"]["free_kb"]
        slim_broker_free_kb = size_stats[slim_broker_id]["disk"]["free_kb"]

        # is the gap is big enough to swap partitions?
        gap = slim_broker_free_kb - fat_broker_free_kb
        if gap < self.diff_threshold_kb:
            _LOG.info("Gap between brokers: (id: {}, free_kb: {}) and (id: {}, free_kb: {}) is not enough to "
                      "trigger partitions swap; gap is {} KB".format(slim_broker_id, slim_broker_free_kb, fat_broker_id,
                                                                     fat_broker_free_kb, gap))
            return None
        else:
            _LOG.info("Creating swap partitions change to swap partitions of brokers (id: {}, free_kb: {}) and "
                      "(id: {}, free_kb: {}); gap is {} KB".format(slim_broker_id, slim_broker_free_kb, fat_broker_id,
                                                                   fat_broker_free_kb, gap))
            return SwapPartitionsChange(self.zk, int(fat_broker_id), int(slim_broker_id), gap, size_stats)
