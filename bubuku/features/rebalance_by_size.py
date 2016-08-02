import json
import logging

from kazoo.exceptions import NodeExistsError

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
from bubuku.utils import CmdHelper
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.features.rebalance_by_size')


class RebalanceBySizeChange(Change):
    def __init__(self, zk: Exhibitor):
        self.zk = zk

    def get_name(self):
        return 'rebalance_by_size'

    def can_run(self, current_actions):
        raise NotImplementedError('Not implemented')  # todo: not implemented yet

    def run(self, current_actions):
        raise NotImplementedError('Not implemented')  # todo: not implemented yet

    def can_run_at_exit(self):
        raise NotImplementedError('Not implemented')  # todo: not implemented yet


class RebalanceBySize(Check):
    check_interval_s = 600  # 10 min

    def __init__(self, zk: Exhibitor, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker

    def check(self):
        if self.broker.is_running_and_registered() and self.__is_data_imbalanced():
            _LOG.info("Starting rebalance by size")
            return RebalanceBySizeChange(self.zk)
        return None

    def __is_data_imbalanced(self) -> bool:
        return False  # todo: not implemented yet


class GenerateDataSizeStatistics():
    check_interval_s = 1800  # 30 min

    def __init__(self, zk: Exhibitor, broker: BrokerManager, cmd_helper: CmdHelper, kafka_log_dirs):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.cmd_helper = cmd_helper
        self.kafka_log_dirs = kafka_log_dirs

    def check(self):
        if self.broker.is_running_and_registered():
            _LOG.info("Generating data size statistics")
            try:
                self.__generate_stats()
            except Exception:
                _LOG.warn("Error occurred when collecting size statistics", exc_info=True)
        return None

    def __generate_stats(self):
        topic_stats = self.__get_topics_stats()
        disk_stats = self.__get_disk_stats()
        stats = {"disk": disk_stats, "topics": topic_stats}
        self.__write_stat_to_zk(stats)

    def __get_topics_stats(self):
        topic_stats = {}
        for log_dir in self.kafka_log_dirs.split(","):
            topic_dirs = self.cmd_helper.cmd_run("du -k -d 1 {}".format(log_dir)).split("\n")
            for topic_dir in topic_dirs:
                self.__parse_dir_stats(topic_dir, log_dir, topic_stats)
        return topic_stats

    @staticmethod
    def __parse_dir_stats(dir, log_dir, topic_stats):
        """
        Parses the "du" tool single line output and writes result to topic_stats json
        :param dir: the string to be parsed; example: "45983\t/tmp/kafka-logs/my-kafka-topic-0"
        :param log_dir: the kafka log directory name itself
        :param topic_stats: the json which will be populated
        """
        dir_data = dir.split("\t")
        if len(dir_data) == 2 and dir_data[1] != log_dir:
            size_kb, dir_name = tuple(dir_data)
            tp_name = dir_name.split("/")[-1]
            tp_parts = tp_name.rsplit("-", 1)
            if len(tp_parts) == 2:
                topic, partition = tuple(tp_parts)
                if topic not in topic_stats:
                    topic_stats[topic] = {}
                topic_stats[topic][partition] = int(size_kb)

    def __get_disk_stats(self):
        disks = self.cmd_helper.cmd_run("df -k | tail -n +2 |  awk '{ print $3, $4 }'").split("\n")
        total_used = total_free = 0
        for disk in disks:
            parts = disk.split(" ")
            if len(parts) == 2:
                used, free = tuple(parts)
                total_used += int(used)
                total_free += int(free)
        return {"used": total_used, "free": total_free}

    def __write_stat_to_zk(self, stats):
        broker_id = self.broker.id_manager.get_broker_id()
        data = json.dumps(stats, sort_keys=True, separators=(',', ':')).encode("utf-8")
        path = "/bubuku/size_stats/{}".format(broker_id)
        try:
            self.zk.create(path, data, makepath=True)
        except NodeExistsError:
            self.zk.set(path, data)
