import json
import logging
import subprocess

from bubuku.config import KafkaProperties
from bubuku.id_generator import BrokerIdGenerator
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.broker')


class LeaderElectionInProgress(Exception):
    pass


class KafkaProcessHolder(object):
    def __init__(self):
        self.process = None

    def get(self):
        return self.process

    def set(self, process):
        self.process = process


class StartupTimeout(object):
    def __init__(self, initial_value: float, config: str):
        self.timeout = initial_value
        self.config = config

    def get_timeout(self) -> float:
        return self.timeout

    def on_timeout_fail(self):
        self.timeout += self.get_step()

    def get_step(self) -> float:
        return 0.

    def __str__(self):
        return 'timeout={}, step={}, config={}'.format(self.get_timeout(), self.get_step(), self.config)

    @staticmethod
    def build(props: dict):
        type_ = props.get('type', 'linear')
        result = StartupTimeout(float(props.get('initial', '300')),
                                ','.join('{}={}'.format(k, v) for k, v in props.items()))
        if type_ == 'linear':
            step = float(props.get('step', '60'))
            result.get_step = lambda: step
        elif type_ == 'progressive':
            scale = float(props.get('step', '0.5'))
            result.get_step = lambda: result.timeout * scale
        else:
            raise NotImplementedError('Startup timeout type {} is not valid'.format(type_))
        return result


class BrokerManager(object):
    def __init__(self, process_holder: KafkaProcessHolder, kafka_dir: str, exhibitor: BukuExhibitor,
                 id_manager: BrokerIdGenerator, kafka_properties: KafkaProperties, timeout: StartupTimeout):
        self.kafka_dir = kafka_dir
        self.id_manager = id_manager
        self.exhibitor = exhibitor
        self.kafka_properties = kafka_properties
        self.process_holder = process_holder
        self.timeout = timeout

    def is_running_and_registered(self):
        if not self.process_holder.get():
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
        Says if this broker is still a leader for partitions
        :return: True, if broker is a leader for some partitions.
        """
        broker_id = self.id_manager.get_broker_id()
        if not broker_id:
            return False
        return not self._is_leadership_transferred(dead_broker_ids=[broker_id])

    def _terminate_process(self):
        if self.process_holder.get() is not None:
            try:
                self.process_holder.get().terminate()
                self.process_holder.get().wait()
            except Exception as e:
                _LOG.error('Failed to wait for termination of kafka process', exc_info=e)
            finally:
                self.process_holder.set(None)

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
        if not self.process_holder.get():
            if not self._is_leadership_transferred(active_broker_ids=self.exhibitor.get_broker_ids()):
                raise LeaderElectionInProgress()

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
            self.process_holder.set(self._open_process())

            _LOG.info('Waiting for kafka to start up with timeout {} seconds'.format(self.timeout.get_timeout()))
            if not self.id_manager.wait_for_broker_id_presence(self.timeout.get_timeout()):
                self.timeout.on_timeout_fail()
                _LOG.error(
                    'Failed to wait for broker to start up, probably will kill, next timeout is'.format(
                        self.timeout.get_timeout()))

    def get_disjoined_isr_topic_partitions(self):
        """
        Gets list of (topic,partition) for which this broker should be in isr list, but for some reason it is not there 
        :return: list of tuples (topic:str, partition:int)
        """
        broker_id = int(self.id_manager.get_broker_id())
        checked_topic_partitoins = [
            (topic, partition) for topic, partition, broker_ids in self.exhibitor.load_partition_assignment()
            if broker_id in broker_ids
        ]
        unjoined_topic_partitions = []
        for topic, partition, state in self.exhibitor.load_partition_states():
            if (topic, partition) not in checked_topic_partitoins:
                continue
            if broker_id not in state.get('isr', []):
                unjoined_topic_partitions.append((topic, partition))
        return unjoined_topic_partitions

    def _is_leadership_transferred(self, active_broker_ids=None, dead_broker_ids=None):
        _LOG.info('Checking if leadership is transferred: active_broker_ids={}, dead_broker_ids={}'.format(
            active_broker_ids, dead_broker_ids))
        if self._is_clean_election():
            for topic, partition, state in self.exhibitor.load_partition_states():
                leader = str(state['leader'])
                if active_broker_ids and leader not in active_broker_ids:
                    if any(str(x) in active_broker_ids for x in state.get('isr', [])):
                        _LOG.warn(
                            'Leadership is not transferred for {} {} ({}, brokers: {})'.format(
                                topic, partition, json.dumps(state), active_broker_ids))
                        return False
                    else:
                        _LOG.warn('Shit happens! No single isr available for {}, {}, state: {}, '
                                  'skipping check for that'.format(topic, partition, json.dumps(state)))
                if dead_broker_ids and leader in dead_broker_ids:
                    _LOG.warn('Leadership is not transferred for {} {}, {} (dead list: {})'.format(
                        topic, partition, json.dumps(state), dead_broker_ids))
                    return False

        return True

    def _open_process(self):
        return subprocess.Popen(
            [self.kafka_dir + "/bin/kafka-server-start.sh", self.kafka_properties.settings_file])
