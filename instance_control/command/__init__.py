from subprocess import call

from instance_control import config


class Command(object):
    def __init__(self, cluster_name: str, cluster_config_path: str):
        super().__init__()
        self.cluster_config_path = cluster_config_path
        self.cluster_name = cluster_name
        self.cluster_config = config.read_cluster_config(cluster_name, cluster_config_path)

    def init(self):
        raise NotImplementedError('Not implemented yet')

    def start(self):
        raise NotImplementedError('Not implemented yet')

    def run(self):
        self.init()
        call(["zaws", "login", self.cluster_config['account']])
        self.start()
