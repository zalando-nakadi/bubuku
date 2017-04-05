import logging

from bubuku.broker import BrokerManager
from bubuku.controller import Change, Check
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.restart_on_zk')

_STAGE_STOP = 'stop'
_STAGE_START = 'start'
_STAGE_WAIT_FOR_ISR = 'wait_for_isr'


class RestartBrokerChange(Change):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager, break_condition, processed_callback=None):
        self.zk = zk
        self.broker = broker
        self.break_condition = break_condition
        self.stage = _STAGE_STOP
        self.processed_callback = processed_callback

    def get_name(self):
        return 'restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'stop']])

    def run(self, current_actions):
        if self.stage == _STAGE_STOP:
            if self.break_condition and self.break_condition():
                return False
            self.broker.stop_kafka_process()
            self.stage = _STAGE_START
            return True
        elif self.stage == _STAGE_START:
            # Yep, use latest data
            zk_conn_str = self.zk.get_conn_str()
            try:
                self.broker.start_kafka_process(zk_conn_str)
            except Exception as e:
                _LOG.error('Failed to start kafka process against {}'.format(zk_conn_str), exc_info=e)
                return True
            self.stage = _STAGE_WAIT_FOR_ISR
            return True
        elif self.stage == _STAGE_WAIT_FOR_ISR:
            if not self.broker.is_running_and_registered():
                _LOG.warning("Broker is considered to be dead right after restart, won't wait for isr")
                return False
            unjoined = self.broker.get_disjoined_isr_topic_partitions()
            if unjoined:
                _LOG.info("Still waiting to join to isr list for topic partitions: {}".format(unjoined))
                return True
            return False
        else:
            _LOG.error('Stage {} is not supported'.format(self.stage))
        return False

    def on_remove(self):
        if self.processed_callback:
            self.processed_callback()

    def __str__(self):
        return 'RestartOnZkChange ({}), stage={}'.format(self.get_name(), self.stage)


class CheckExhibitorAddressChanged(Check):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.conn_str = None

    def check(self) -> Change:
        new_conn_str = self.zk.get_conn_str()
        if new_conn_str != self.conn_str:
            def _cancel_if():
                current_conn_str = self.zk.get_conn_str()
                if current_conn_str != new_conn_str:
                    _LOG.warning('ZK address changed again, from {} to {}'.format(new_conn_str, current_conn_str))
                    return True
                if current_conn_str == self.broker.get_zk_connect_string():
                    _LOG.warning('Broker already have latest version of zk address: '.format(current_conn_str))
                    return True
                return False

            _LOG.info('ZK addresses changed from {} to {}, triggering restart'.format(self.conn_str, new_conn_str))
            self.conn_str = new_conn_str
            return RestartBrokerChange(self.zk, self.broker, _cancel_if)

    def __str__(self):
        return 'CheckExhibitorAddressChanged, current={}'.format(self.conn_str)
