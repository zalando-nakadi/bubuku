import json
import logging
from time import time, sleep

from bubuku.config import KafkaProperties
from bubuku.id_generator import BrokerIdGenerator
from bubuku.process import KafkaProcess
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.broker')


class LeaderElectionInProgress(Exception):
    pass


class StartupTimeout(object):
    def is_timed_out(self, seconds: float) -> bool:
        raise Exception('Not supported')

    def on_timeout_fail(self):
        raise Exception('Not supported')

    @staticmethod
    def build(props: dict):
        type_ = props.get('type', 'linear')
        if type_ == 'linear':
            return LinearTimeout(float(props.get('initial', 300)), float(props.get('step', '60')))
        elif type_ == 'progressive':
            return ProgressiveTimeout(float(props.get('initial', 300)), float(props.get('step', '0.5')))
        elif type_ == 'none':
            return NoTimeout()
        else:
            raise NotImplementedError('Startup timeout type {} is not valid'.format(type_))


class ProgressiveTimeout(StartupTimeout):
    def __init__(self, initial: float, scale: float):
        self.initial = initial
        self.timeout = initial
        self.scale = scale

    def is_timed_out(self, seconds: float) -> bool:
        return seconds > self.timeout

    def on_timeout_fail(self):
        self.timeout = self.timeout * (1 + self.scale)

    def __str__(self):
        return 'Progressive, initial={}, scale={}, current={}'.format(self.initial, self.scale, self.timeout)


class LinearTimeout(StartupTimeout):
    def __init__(self, initial: float, step: float):
        self.initial = initial
        self.timeout = initial
        self.step = step

    def is_timed_out(self, seconds: float) -> bool:
        return seconds > self.timeout

    def on_timeout_fail(self):
        self.timeout += self.step

    def __str__(self):
        return 'Linear, initial={}, step={}, current={}'.format(self.initial, self.step, self.timeout)


class NoTimeout(StartupTimeout):
    def is_timed_out(self, seconds: float) -> bool:
        return False

    def on_timeout_fail(self):
        pass


class BrokerManager(object):
    def __init__(self, process: KafkaProcess, exhibitor: BukuExhibitor,
                 id_manager: BrokerIdGenerator, kafka_properties: KafkaProperties, timeout: StartupTimeout):
        self.id_manager = id_manager
        self.exhibitor = exhibitor
        self.kafka_properties = kafka_properties
        self.process = process
        self.timeout = timeout

    def is_running_and_registered(self):
        if not self.process.is_running():
            return False
        return self.id_manager.is_registered()

    def stop_kafka_process(self):
        if self.process.is_running():
            self.process.stop_and_wait()
        self._wait_for_zk_absence()

    def _is_clean_election(self):
        value = self.kafka_properties.get_property('unclean.leader.election.enable')
        return value == 'false'

    def has_leadership(self):
        """
        Says if this broker is still a leader for partitions
        :return: True, if broker is a leader for some partitions.
        """
        broker_id = self.id_manager.get_broker_id()
        if not broker_id:
            return False
        return not self._is_leadership_transferred(dead_broker_ids=[broker_id])

    def _wait_for_zk_absence(self):
        try:
            while self.id_manager.is_registered():
                sleep(1)
        except Exception as e:
            _LOG.error('Failed to wait until broker id absence in zk', exc_info=e)

    def get_zk_connect_string(self):
        return self.kafka_properties.get_property('zookeeper.connect')

    def start_kafka_process(self, zookeeper_address):
        """
        Starts kafka using zookeeper address provided.
        :param zookeeper_address: Address to use for kafka
        :raise LeaderElectionInProgress: raised when broker can not be started because leader election is in progress
        """
        if not self.process.is_running():
            if not self._is_leadership_transferred(active_broker_ids=self.exhibitor.get_broker_ids()):
                raise LeaderElectionInProgress()

            _LOG.info('Using ZK address: {}'.format(zookeeper_address))
            self.kafka_properties.set_property('zookeeper.connect', zookeeper_address)

            self.kafka_properties.dump()

            _LOG.info('Staring kafka process')
            self.process.start(self.kafka_properties.settings_file)

            _LOG.info('Waiting for kafka to start with timeout {}'.format(self.timeout))
            start = time()
            while self.process.is_running():
                if self.id_manager.is_registered():
                    break
                if self.timeout.is_timed_out(time() - start):
                    self.timeout.on_timeout_fail()
                    break
                sleep(1)
            if not self.process.is_running() or not self.id_manager.is_registered():
                _LOG.error(
                    'Failed to wait for broker to start up, probably will kill, next timeout is'.format(self.timeout))

    def _is_leadership_transferred(self, active_broker_ids=None, dead_broker_ids=None):
        _LOG.info('Checking if leadership is transferred: active_broker_ids={}, dead_broker_ids={}'.format(
            active_broker_ids, dead_broker_ids))
        if self._is_clean_election():
            for topic, partition, state in self.exhibitor.load_partition_states():
                leader = str(state['leader'])
                if active_broker_ids and leader not in active_broker_ids:
                    if any(str(x) in active_broker_ids for x in state.get('isr', [])):
                        _LOG.warning(
                            'Leadership is not transferred for {} {} ({}, brokers: {})'.format(
                                topic, partition, json.dumps(state), active_broker_ids))
                        return False
                    else:
                        _LOG.warning('No single isr available for {}, {}, state: {}, skipping check for that'.format(
                            topic, partition, json.dumps(state)))
                if dead_broker_ids and leader in dead_broker_ids:
                    _LOG.warning('Leadership is not transferred for {} {}, {} (dead list: {})'.format(
                        topic, partition, json.dumps(state), dead_broker_ids))
                    return False

        return True
