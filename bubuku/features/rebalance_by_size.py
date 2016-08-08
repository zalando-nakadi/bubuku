from collections import namedtuple
import logging
from operator import attrgetter

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
from bubuku.utils import CmdHelper
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rebalance_by_size')

TpData = namedtuple('TpData', ('topic', 'partition', 'size', 'replicas'))


class SwapPartitionsChange(Change):
    def __init__(self, zk: BukuExhibitor, fat_broker_id: str, slim_broker_id: str, gap: int, size_stats: dict):
        self.zk = zk
        self.fat_broker_id = fat_broker_id
        self.slim_broker_id = slim_broker_id
        self.gap = gap
        self.size_stats = size_stats
        self.to_move = None

    def get_name(self):
        return 'swap_partitions'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'rebalance', 'swap_partitions', 'stop']])

    def run(self, current_actions):
        if self.to_move:
            return not self.__perform_swap(self.to_move)

        # merge topics size stats to a single dict
        topics_stats = {}
        for broker_id, broker_stats in self.size_stats.items():
            for topic in broker_stats["topics"].keys():
                if topic not in topics_stats:
                    topics_stats[topic] = {}
                topics_stats[topic].update(broker_stats["topics"][topic])

        # find partitions that are candidates to be swapped between "fat" and "slim" brokers
        swap_partition_candidates = self.__find_all_swap_candidates(self.fat_broker_id, self.slim_broker_id,
                                                                    topics_stats)

        # smallest partition from slim broker is the one we move to fat broker
        slim_broker_smallest_partition = min(swap_partition_candidates[self.slim_broker_id], key=attrgetter("size"))

        # find the best fitting fat broker partition to move to slim broker
        # (should be as much as possible closing the gap between brokers)
        fat_broker_swap_candidates = swap_partition_candidates[self.fat_broker_id]
        matching_swap_partition = self.__find_best_swap_candidate(fat_broker_swap_candidates, self.gap,
                                                                  slim_broker_smallest_partition.size)
        if not matching_swap_partition:
            return False

        self.to_move = self.__create_swap_partitions_json(slim_broker_smallest_partition, self.slim_broker_id,
                                                          matching_swap_partition, self.fat_broker_id)
        return not self.__perform_swap(self.to_move)

    def __perform_swap(self, swap_json):
        return self.zk.reallocate_partitions(swap_json)

    def __find_all_swap_candidates(self, fat_broker_id: str, slim_broker_id: str, topics_stats: dict) -> dict:
        partition_assignment = self.zk.load_partition_assignment()
        swap_partition_candidates = {}
        for topic, partition, replicas in partition_assignment:
            if topic not in topics_stats or partition not in topics_stats[topic]:
                continue  # we skip this partition as there is not data size stats for it
            if fat_broker_id in replicas and slim_broker_id in replicas:
                continue  # we skip this partition as it exists on both involved brokers
            for broker_id in [slim_broker_id, fat_broker_id]:
                if broker_id in replicas:
                    if broker_id not in swap_partition_candidates:
                        swap_partition_candidates[broker_id] = []
                    swap_partition_candidates[broker_id].append(
                        TpData(topic, partition, topics_stats[topic][partition], replicas))

        return swap_partition_candidates

    @staticmethod
    def __find_best_swap_candidate(candidates: list, brokers_gap: int, partition_to_swap_size: int) -> TpData:
        candidates.sort(key=attrgetter("size"), reverse=True)
        matching_swap_partition = None
        smallest_new_gap = brokers_gap
        for tp in candidates:
            new_gap = brokers_gap - 2 * abs(tp.size - partition_to_swap_size)
            if abs(new_gap) < smallest_new_gap:
                smallest_new_gap = new_gap
                matching_swap_partition = tp
        return matching_swap_partition

    @staticmethod
    def __create_swap_partitions_json(tp1: TpData, broker1: str, tp2: TpData, broker2: str) -> list:
        return [
            (tp1.topic, tp1.partition, [broker2 if r == broker1 else r for r in tp1.replicas]),
            (tp2.topic, tp2.partition, [broker1 if r == broker2 else r for r in tp2.replicas])
        ]


class RebalanceBySize(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, free_space_diff_threshold_kb: int):
        super().__init__(check_interval_s=900)
        self.zk = zk
        self.broker = broker
        self.free_space_diff_threshold_kb = free_space_diff_threshold_kb

    def check(self):
        if self.broker.is_running_and_registered():
            return self.create_swap_partition_change()
        return None

    def create_swap_partition_change(self) -> SwapPartitionsChange:
        size_stats = self.zk.get_disk_stats()

        if len(size_stats.keys()) == 0:
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
        if gap < self.free_space_diff_threshold_kb:
            return None

        return SwapPartitionsChange(self.zk, fat_broker_id, slim_broker_id, gap, size_stats)


class GenerateDataSizeStatistics(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, cmd_helper: CmdHelper, kafka_log_dirs):
        super().__init__(check_interval_s=600)
        self.zk = zk
        self.broker = broker
        self.cmd_helper = cmd_helper
        self.kafka_log_dirs = kafka_log_dirs

    def check(self):
        if self.broker.is_running_and_registered():
            _LOG.info("Generating data size statistics")
            try:
                self.__generate_stats()
                _LOG.info("Data size statistics successfully written to zk")
            except Exception:
                _LOG.warn("Error occurred when collecting size statistics", exc_info=True)
        return None

    def __generate_stats(self):
        topics_stats = self.__get_topics_stats()
        disk_stats = self.__get_disk_stats()
        stats = {"disk": disk_stats, "topics": topics_stats}
        self.zk.update_disk_stats(self.broker.id_manager.get_broker_id(), stats)

    def __get_topics_stats(self):
        topics_stats = {}
        for log_dir in self.kafka_log_dirs:
            topic_dirs = self.cmd_helper.cmd_run("du -k -d 1 {}".format(log_dir)).split("\n")
            for topic_dir in topic_dirs:
                dir_stats = self.__parse_dir_stats(topic_dir, log_dir)
                if dir_stats:
                    topic, partition, size_kb = dir_stats
                    if topic not in topics_stats:
                        topics_stats[topic] = {}
                    topics_stats[topic][partition] = int(size_kb)
        return topics_stats

    @staticmethod
    def __parse_dir_stats(topic_dir, log_dir):
        """
        Parses topic-partition size stats from "du" tool single line output
        :param topic_dir: the string to be parsed; example: "45983\t/tmp/kafka-logs/my-kafka-topic-0"
        :param log_dir: the kafka log directory name itself
        :return: tuple (topic, partition, size) or None if the topic_dir has incorrect format
        """
        dir_data = topic_dir.split("\t")
        if len(dir_data) == 2 and dir_data[1] != log_dir:
            size_kb, dir_name = tuple(dir_data)
            tp_name = dir_name.split("/")[-1]
            tp_parts = tp_name.rsplit("-", 1)
            if len(tp_parts) == 2:
                topic, partition = tuple(tp_parts)
                return topic, partition, size_kb
        return None

    def __get_disk_stats(self):
        disks = self.cmd_helper.cmd_run("df -k | tail -n +2 |  awk '{ print $3, $4 }'").split("\n")
        total_used = total_free = 0
        for disk in disks:
            parts = disk.split(" ")
            if len(parts) == 2:
                used, free = tuple(parts)
                total_used += int(used)
                total_free += int(free)
        return {"used_kb": total_used, "free_kb": total_free}
