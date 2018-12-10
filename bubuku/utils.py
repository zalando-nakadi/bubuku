import logging
import subprocess

from bubuku.config import Config, KafkaProperties, load_config
from bubuku.env_provider import EnvProvider
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.utils')


class CmdHelper(object):
    def get_disk_stats(self) -> (int, int):
        """
        Returns total disk stats,
        :return: used_kb, free_kb
        """
        disks = self.cmd_run("df -k | tail -n +2 |  awk '{ print $3, $4 }'").split("\n")
        total_used = total_free = 0
        for disk in disks:
            parts = disk.split(" ")
            if len(parts) == 2:
                used, free = tuple(parts)
                total_used += int(used)
                total_free += int(free)
        return total_used, total_free

    def cmd_run(self, cmd):
        output = subprocess.check_output(cmd, shell=True)
        return output.decode("utf-8")


def get_opt_broker_id(broker_id: str, config: Config, zk: BukuExhibitor, env_provider: EnvProvider) -> str:
    if not broker_id:
        kafka_properties = KafkaProperties(config.kafka_settings_template, '/tmp/tmp.props'.format(config.kafka_dir))
        broker_id_manager = env_provider.create_broker_id_manager(zk, kafka_properties)
        broker_id = broker_id_manager.get_broker_id()
        _LOG.info('Will use broker_id {}'.format(broker_id))
    running_brokers = zk.get_broker_ids()
    if broker_id not in running_brokers:
        raise Exception('Broker id {} is not registered ({})'.format(broker_id, running_brokers))
    return broker_id


def prepare_configs():
    config = load_config()
    _LOG.info('Using config: {}'.format(config))
    env_provider = EnvProvider.create_env_provider(config)
    return config, env_provider
