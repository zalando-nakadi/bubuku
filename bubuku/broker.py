import logging
import subprocess

from bubuku.config import KafkaProperties
from bubuku.id_generator import BrokerIdGenerator
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.broker')


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
        if self.process:
            return True
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
        self.process = subprocess.Popen(
            [self.kafka_dir + "/bin/kafka-server-start.sh", self.kafka_properties.settings_file])

        _LOG.info('Waiting for kafka to start up with timeout {} seconds'.format(self.wait_timeout))
        if not self.id_manager.wait_for_broker_id_presence(self.wait_timeout):
            self.wait_timeout += 60
            _LOG.error(
                'Failed to wait for broker to start up, probably will kill, increasing timeout to {} seconds'.format(
                    self.wait_timeout))
