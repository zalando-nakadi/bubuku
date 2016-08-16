import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Check
from bubuku.utils import CmdHelper
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.data_size_stats')


class GenerateDataSizeStatistics(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, cmd_helper: CmdHelper, kafka_log_dirs: list):
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

    def __str__(self):
        return 'GenerateDataSizeStatistics'

    def __generate_stats(self):
        topics_stats = self.__get_topics_stats()
        used_kb, free_kb = self.cmd_helper.get_disk_stats()
        stats = {"disk": {'used_kb': used_kb, 'free_kb': free_kb}, "topics": topics_stats}
        self.zk.update_disk_stats(self.broker.id_manager.get_broker_id(), stats)

    def __get_topics_stats(self):
        topics_stats = {}
        for log_dir in self.kafka_log_dirs:
            _LOG.info("Processing log dir: {}".format(log_dir))
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
