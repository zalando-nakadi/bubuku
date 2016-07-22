import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.features.restart_on_zk')


class RestartBrokerChange(Change):
    def __init__(self, zk_hosts: str, zk: Exhibitor, broker: BrokerManager):
        self.zk_hosts = zk_hosts
        self.zk = zk
        self.broker = broker

    def get_name(self):
        return 'restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'stop']])

    def run(self, current_actions):
        current_hosts = self.zk.exhibitor.zookeeper_hosts
        if current_hosts != self.zk_hosts:
            _LOG.warning('ZK address changed again, from {} to {}'.format(self.zk_hosts, current_hosts))
            return False

        new_zk_connect = self.zk_hosts + self.zk.prefix
        if self.broker.get_zk_connect_string() != new_zk_connect:
            _LOG.info('Executing restart to update ZK to {}'.format(new_zk_connect))
            self.broker.stop_kafka_process()
            self.broker.start_kafka_process(new_zk_connect)
        else:
            _LOG.info('ZK connect string already configured correctly {}, will not restart'.format(new_zk_connect))
        return False


class CheckExhibitorAddressChanged(Check):
    def __init__(self, zk: Exhibitor, broker: BrokerManager):
        self.zk = zk
        self.broker = broker
        self.zk_address = None

    def check(self) -> Change:
        new_addresses = self.zk.exhibitor.zookeeper_hosts
        if new_addresses != self.zk_address:
            _LOG.info('ZK addresses changed from {} to {}, triggering restart'.format(self.zk_address, new_addresses))
            self.zk_address = new_addresses
            return RestartBrokerChange(new_addresses, self.zk, self.broker)
