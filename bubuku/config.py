import logging
import os
from collections import namedtuple

_LOG = logging.getLogger('bubuku.properties')

Config = namedtuple('Config', ('kafka_dir', 'kafka_settings_template', 'zk_stack_name',
                               'zk_prefix', 'id_policy', 'features', 'health_port'))


class KafkaProperties(object):
    def __init__(self, template: str, kafka_settings: str):
        self.lines = []
        self.settings_file = kafka_settings
        _LOG.info('Loading template properties from {}'.format(self.settings_file))
        with open(template, 'r') as f:
            for l in f.readlines():
                self.lines.append(_make_clean_line(l))

    def get_property(self, name: str) -> str:
        idx = self._get_property_idx(name)
        if idx is not None:
            return self.lines[idx].split('=', 1)[1]
        return None

    def _get_property_idx(self, name: str):
        search = '{}='.format(name)
        for idx in range(0, len(self.lines)):
            if self.lines[idx].startswith(search):
                return idx
        return None

    def delete_property(self, name):
        idx = self._get_property_idx(name)
        if idx is not None:
            del self.lines[idx]

    def set_property(self, name, value):
        idx = self._get_property_idx(name)
        if idx is not None:
            self.lines[idx] = '{}={}'.format(name, value)
        else:
            self.lines.append('{}={}'.format(name, value))

    def dump(self):
        _LOG.info('Dumping kafka properties to {}'.format(self.settings_file))
        with open(self.settings_file, mode='w') as f:
            for l in self.lines:
                f.write('{}\n'.format(l))


def load_config() -> Config:
    zk_prefix = os.getenv('ZOOKEEPER_PREFIX', '/')

    features_str = os.getenv('BUKU_FEATURES', '').lower()
    features = {key: {} for key in features_str.split(',')} if features_str else {}
    if "balance_data_size" in features:
        features["balance_data_size"]["diff_threshold_mb"] = int(os.getenv('FREE_SPACE_DIFF_THRESHOLD_MB', '50000'))

    return Config(
        kafka_dir=os.getenv('KAFKA_DIR'),
        kafka_settings_template=os.getenv('KAFKA_SETTINGS'),
        zk_stack_name=os.getenv('ZOOKEEPER_STACK_NAME'),
        zk_prefix=zk_prefix if zk_prefix.startswith('/') or not zk_prefix else '/{}'.format(zk_prefix),
        id_policy=os.getenv('BROKER_ID_POLICY', 'ip').lower(),
        features=features,
        health_port=int(os.getenv('HEALTH_PORT', '8888'))
    )


def _make_clean_line(l: str) -> str:
    result = l.strip()
    if result.startswith('#') or not result:
        return result
    n, v = result.split('=', 1)
    return '{}={}'.format(n.strip(), v)
