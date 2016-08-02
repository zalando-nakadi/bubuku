import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.features.restart_on_zk')

_STAGE_STOP = 'stop'
_STAGE_CHECK_LEADERSHIP = 'check_leader'
_STAGE_START = 'start'


class RestartBrokerOnZkChange(Change):
    def __init__(self, zk_hosts: str, zk: Exhibitor, broker: BrokerManager):
        self.zk_hosts = zk_hosts
        self.zk = zk
        self.broker = broker
        self.stage = _STAGE_STOP

    def get_name(self):
        return 'restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'stop']])

    def run(self, current_actions):
        if self.stage == _STAGE_STOP:
            current_hosts = self.zk.exhibitor.zookeeper_hosts
            if current_hosts != self.zk_hosts:
                _LOG.warning('ZK address changed again, from {} to {}'.format(self.zk_hosts, current_hosts))
                return False
            self.broker.stop_kafka_process()
            self.stage = _STAGE_CHECK_LEADERSHIP
            return True
        elif self.stage == _STAGE_CHECK_LEADERSHIP:
            if not self.broker.has_leadership():
                self.stage = _STAGE_START
            return True
        elif self.stage == _STAGE_START:
            # Yep, use latest data
            self.broker.start_kafka_process(self.zk.exhibitor.zookeeper_hosts + self.zk.prefix)
            return False
        else:
            _LOG.error('Stage {} is not supported'.format(self.stage))
        return False

    def __str__(self):
        return 'RestartOnZkChange ({}), stage={}, new_hosts={}'.format(self.get_name(), self.stage, self.zk_hosts)


class CheckExhibitorAddressChanged(Check):
    def __init__(self, zk: Exhibitor, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.zk_address = None

    def check(self) -> Change:
        new_addresses = self.zk.exhibitor.zookeeper_hosts
        if new_addresses != self.zk_address:
            _LOG.info('ZK addresses changed from {} to {}, triggering restart'.format(self.zk_address, new_addresses))
            self.zk_address = new_addresses
            return RestartBrokerOnZkChange(new_addresses, self.zk, self.broker)

    def __str__(self):
        return 'CheckExhibitorAddressChanged, current={}'.format(self.zk_address)
