import logging

from instance_control import node
from instance_control import piu
from instance_control.aws import AWSResources
from instance_control.command import Command

_LOG = logging.getLogger('bubuku.cluster.command.terminate')


class TerminateCommand(Command):
    def __init__(self, cluster_config_path: str, ip: str, user: str, odd: str):
        super().__init__(cluster_config_path)
        self.ip = ip
        self.user = user
        self.odd = odd

    def execute(self):
        aws_ = AWSResources(region=self.cluster_config['region'])
        instance = node.get_instance_by_ip(aws_.ec2_resource, self.cluster_config['cluster_name'], self.ip)
        piu.stop_taupage(self.ip, self.user, self.odd, self.cluster_config['region'])
        node.terminate(aws_, self.cluster_config['cluster_name'], instance)
