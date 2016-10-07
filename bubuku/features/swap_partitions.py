import logging
from collections import namedtuple
from operator import attrgetter

import requests

from bubuku.broker import BrokerManager
from bubuku.controller import Check
from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.swap_partitions')

TpData = namedtuple('_TpData', ('topic', 'partition', 'size', 'replicas'))


class SwapPartitionsChange(BaseRebalanceChange):
    def __init__(self, zk: BukuExhibitor, swap_data_provider):
        self.zk = zk
        self.to_move = None
        self.swap_data_provider = swap_data_provider

    def run(self, current_actions):
        if self.should_be_paused(current_actions):
            _LOG.info("Pausing swap partitions change as there are conflicting actions: {}".format(current_actions))
            return True
        # if there's a rebalance currently running - postpone current change
        if self.zk.is_rebalancing():
            return True

        if self.to_move is None:
            slim_broker_id, fat_broker_id, gap, size_stats = self.swap_data_provider(self.zk)
            if slim_broker_id is None:
                _LOG.info('Can not find slim broker and fat broker during reassignment. Probably gap changed')
                return False
            # merge topics size stats to a single dict
            topics_stats = {}
            for broker_stats in size_stats.values():
                for topic in broker_stats["topics"].keys():
                    if topic not in topics_stats:
                        topics_stats[topic] = {}
                    topics_stats[topic].update(broker_stats["topics"][topic])

            # find partitions that are candidates to be swapped between "fat" and "slim" brokers
            swap_partition_candidates = self.__find_all_swap_candidates(fat_broker_id, slim_broker_id, topics_stats)

            # smallest partition from slim broker is the one we move to fat broker
            slim_broker_smallest_partition = min(swap_partition_candidates[slim_broker_id], key=attrgetter("size"))
            if not slim_broker_smallest_partition:
                _LOG.info("No partitions on slim broker(id: {}) found to swap".format(slim_broker_id))
                return False
            _LOG.info("Slim broker(id: {}) partition to swap: {}".format(
                slim_broker_id, slim_broker_smallest_partition))

            # find the best fitting fat broker partition to move to slim broker
            # (should be as much as possible closing the gap between brokers)
            fat_broker_swap_candidates = swap_partition_candidates[fat_broker_id]
            matching_swap_partition = self.__find_best_swap_candidate(fat_broker_swap_candidates, gap,
                                                                      slim_broker_smallest_partition.size)

            # if there is no possible swap that will decrease the gap - just do nothing
            if not matching_swap_partition:
                _LOG.info("No candidate from fat broker(id:{}) found to swap".format(fat_broker_id))
                return False
            _LOG.info("Fat broker(id: {}) partition to swap: {}".format(fat_broker_id, matching_swap_partition))
            # write rebalance-json to ZK; Kafka will read it and perform the partitions swap
            self.to_move = self.__create_rebalance_list(slim_broker_smallest_partition, slim_broker_id,
                                                        matching_swap_partition, fat_broker_id)

        # if there is already a swap which was postponed - just execute it
        return not self.__perform_swap(self.to_move)

    def __perform_swap(self, rebalance_list):
        _LOG.info("Writing rebalance-json to ZK for partitions swap: {}".format(rebalance_list))
        return self.zk.reallocate_partitions(rebalance_list)

    def __find_all_swap_candidates(self, fat_broker_id: int, slim_broker_id: int, topics_stats: dict) -> dict:
        swap_partition_candidates = {fat_broker_id: [], slim_broker_id: []}
        for topic, partition, replicas in self.zk.load_partition_assignment():
            if topic not in topics_stats or str(partition) not in topics_stats[topic]:
                continue  # we skip this partition as there is not data size stats for it

            if replicas[0] in (fat_broker_id, slim_broker_id):
                continue  # Skip leadership transfer

            if fat_broker_id in replicas and slim_broker_id in replicas:
                continue  # we skip this partition as it exists on both involved brokers

            for broker_id in [slim_broker_id, fat_broker_id]:
                if broker_id in replicas:
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
        return 'SwapPartitions'


def _load_disk_stats(zk: BukuExhibitor, api_port: int):
    size_stats = zk.get_disk_stats()
    if len(size_stats) < 2:
        _LOG.info("No size stats available, imbalance check cancelled")
        return None
    result = {}
    for broker_id, value in size_stats.items():
        try:
            if api_port != -1:  # For unit tests only
                host = zk.get_broker_address(broker_id)
                tmp = requests.get(
                    'http://{}:{}/api/disk_stats'.format(host, api_port),
                    timeout=5).json()
                if any(a not in tmp for a in ['free_kb', 'used_kb']):
                    continue
                value['disk'] = tmp
                value['host'] = host
            result[broker_id] = value
        except Exception as e:
            _LOG.error('Failed to load disk stats for broker {}. Skipping it'.format(broker_id), exc_info=e)

    return result


def load_swap_data(zk: BukuExhibitor, api_port: int, gap: int) -> (str, str, int, dict):
    """
    Finds brokers that could be used for gap of size gap
    :param zk: Bubuku exhibitor
    :param api_port: bubuku api port
    :param gap: gap in kb to get information for
    :return: (slim_broker_id, fat_broker_id, calculated_gap, size_stats) or (None, None, calculated_gap, size_stats)
    """
    size_stats = _load_disk_stats(zk, api_port)
    if not size_stats or len(size_stats) < 2:
        return None, None, None, size_stats
    sorted_stats = sorted(size_stats.items(), key=lambda tup: tup[1]["disk"]["free_kb"])
    calculated_gap = sorted_stats[-1][1]['disk']['free_kb'] - sorted_stats[0][1]['disk']['free_kb']
    _LOG.info('Gap between {} and {} is {}, need to fix: {}'.format(
        sorted_stats[0][0], sorted_stats[-1][0], calculated_gap, calculated_gap > gap))
    if calculated_gap >= gap:
        return int(sorted_stats[-1][0]), int(sorted_stats[0][0]), calculated_gap, size_stats
    return None, None, calculated_gap, size_stats


class CheckBrokersDiskImbalance(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, diff_threshold_kb: int, api_port: int):
        super().__init__(check_interval_s=900)
        self.zk = zk
        self.api_port = api_port
        self.broker = broker
        self.diff_threshold_kb = diff_threshold_kb

    def check(self):
        if self.broker.is_running_and_registered():
            _LOG.info("Starting broker disk imbalance check")
            try:
                slim_broker_id, fat_broker_id, gap, size_stats = load_swap_data(
                    self.zk, self.api_port, self.diff_threshold_kb)
                if slim_broker_id is not None:  # All or nothing
                    return SwapPartitionsChange(
                        self.zk,
                        lambda x: load_swap_data(x, self.api_port, self.diff_threshold_kb))
            except Exception as e:
                _LOG.warn("Error occurred when performing disk imbalance check", exc_info=e)
        return None

    def __str__(self):
        return 'CheckBrokersDiskImbalance'
