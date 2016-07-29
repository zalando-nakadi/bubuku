import json
import logging
import subprocess

from bubuku.config import KafkaProperties
from bubuku.id_generator import BrokerIdGenerator
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.broker')


class LeaderElectionInProgress(Exception):
    pass


class BrokerManager(object):
    def __init__(self, kafka_dir: str, exhibitor: Exhibitor, id_manager: BrokerIdGenerator,
                 kafka_properties: KafkaProperties):
        self.kafka_dir = kafka_dir
        self.id_manager = id_manager
        self.exhibitor = exhibitor
        self.kafka_properties = kafka_properties
        self.process = None
        self.wait_timeout = 5 * 60

    def is_running_and_registered(self):
        if not self.process:
            return False
        return self.id_manager.is_registered()

    def stop_kafka_process(self):
        self._terminate_process()
        self._wait_for_zk_absence()

    def _is_clean_election(self):
        value = self.kafka_properties.get_property('unclean.leader.election.enable')
        return value == 'false'

    def has_leadership(self):
        """
        Says if this broker is still a leader for partitions or in ISR list for some partitions
        :return: True, if broker is a leader or have isr.
        """
        # Only wait when unclean leader election is disabled
        if not self._is_clean_election():
            return False
        broker_id = str(self.id_manager.get_broker_id())
        if not broker_id:
            return False
        for topic in self.exhibitor.get_children('/brokers/topics'):
            for partition in self.exhibitor.get_children('/brokers/topics/{}/partitions'.format(topic)):
                state = json.loads(
                    self.exhibitor.get('/brokers/topics/{}/partitions/{}/state'.format(topic, partition))[0].decode(
                        'utf-8'))
                if str(state['leader']) == broker_id:
                    _LOG.warn('Broker {} is still a leader for {} {} ({})'.format(broker_id, topic, partition,
                                                                                  json.dumps(state)))
                    return True
                if any([str(x) == broker_id for x in state['isr']]):
                    _LOG.warn('Broker {} is still is in ISR for {} {} ({})'.format(broker_id, topic, partition,
                                                                                   json.dumps(state)))
                    return True
        return False

    def _terminate_process(self):
        if self.process is not None:
            try:
                self.process.terminate()
                self.process.wait()
            except Exception as e:
                _LOG.error('Failed to wait for termination of kafka process', exc_info=e)
            finally:
                self.process = None

    def _wait_for_zk_absence(self):
        try:
            self.id_manager.wait_for_broker_id_absence()
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
        if not self.process:
            self._verify_leaders_election_progress()

            broker_id = self.id_manager.get_broker_id()
            _LOG.info('Using broker_id {} for kafka'.format(broker_id))
            if broker_id is not None:
                self.kafka_properties.set_property('broker.id', broker_id)
            else:
                self.kafka_properties.delete_property('broker.id')

            _LOG.info('Using ZK address: {}'.format(zookeeper_address))
            self.kafka_properties.set_property('zookeeper.connect', zookeeper_address)

            self.kafka_properties.dump()

            _LOG.info('Staring kafka process')
            self.process = self._open_process()

            _LOG.info('Waiting for kafka to start up with timeout {} seconds'.format(self.wait_timeout))
            if not self.id_manager.wait_for_broker_id_presence(self.wait_timeout):
                self.wait_timeout += 60
                _LOG.error(
                    'Failed to wait for broker to start up, probably will kill, increasing timeout to {} seconds'.format(
                        self.wait_timeout))

    def _verify_leaders_election_progress(self):
        if self._is_clean_election():
            active_brokers = self.exhibitor.get_children('/brokers/ids')

            for topic in self.exhibitor.get_children('/brokers/topics'):
                for partition in self.exhibitor.get_children('/brokers/topics/{}/partitions'.format(topic)):
                    state = json.loads(
                        self.exhibitor.get('/brokers/topics/{}/partitions/{}/state'.format(topic, partition))[0].decode(
                            'utf-8'))
                    if str(state['leader']) in active_brokers:
                        _LOG.warn('Leadership is not transferred for {} {} ({}, brokers: {})'.format(topic, partition,
                                                                                                     json.dumps(state),
                                                                                                     active_brokers))
                        raise LeaderElectionInProgress()
                    if any([str(x) in active_brokers for x in state['isr']]):
                        _LOG.warn('Leadership is not transferred for {} {} ({}, brokers: {})'.format(topic, partition,
                                                                                                     json.dumps(state),
                                                                                                     active_brokers))
                        raise LeaderElectionInProgress()

    def _open_process(self):
        return subprocess.Popen(
            [self.kafka_dir + "/bin/kafka-server-start.sh", self.kafka_properties.settings_file])
