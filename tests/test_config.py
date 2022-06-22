import os
from tempfile import mkstemp

from bubuku.config import KafkaProperties, load_config, _load_timeout_dict

__PROPS = """
log.dirs=/data/kafka-logs
auto.create.topics.enable=false
delete.topic.enable=true
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=100

### from http://kafka.apache.org/documentation.html#prodconfig

# Replication configurations
num.replica.fetchers=4
replica.fetch.max.bytes=1048576
replica.fetch.wait.max.ms=500
replica.high.watermark.checkpoint.interval.ms=5000
replica.socket.timeout.ms=30000
replica.socket.receive.buffer.bytes=65536
replica.lag.time.max.ms=10000
replica.lag.max.messages=4000

controller.socket.timeout.ms=30000
controller.message.queue.size=10

# Log configuration
#num.partitions=8
#message.max.bytes=1000000
#auto.create.topics.enable=true
log.index.interval.bytes=4096
log.index.size.max.bytes=10485760
log.retention.hours=168
log.flush.interval.ms=10000
log.flush.interval.messages=20000
log.flush.scheduler.interval.ms=2000
log.roll.hours=168
log.retention.check.interval.ms=300000
log.segment.bytes=1073741824

# ZK configuration
zookeeper.connection.timeout.ms=6000
zookeeper.sync.time.ms=2000

# Socket server configuration
num.io.threads=8
num.network.threads=8
socket.request.max.bytes=104857600
socket.receive.buffer.bytes=1048576
socket.send.buffer.bytes=1048576
queued.max.requests=16
fetch.purgatory.purge.interval.requests=100
producer.purgatory.purge.interval.requests=100
"""

__FNAME = ''


def build_test_properties():
    __create_kafak_props_file()
    return KafkaProperties(__FNAME, __FNAME)


def __create_kafak_props_file():
    global __FNAME
    if not __FNAME:
        _, __FNAME = mkstemp(text=True)
        with open(__FNAME, 'w') as fd:
            fd.write(__PROPS)


__create_kafak_props_file()


def test_parse_kafka_properties():
    props = build_test_properties()

    assert props.get_property('log.retention.hours') == '168'


def test_update_kafka_properties():
    props = build_test_properties()

    assert '100' == props.get_property('producer.purgatory.purge.interval.requests')

    props.set_property('producer.purgatory.purge.interval.requests', '180')

    assert '180' == props.get_property('producer.purgatory.purge.interval.requests')

    props.dump()

    props2 = build_test_properties()

    assert '180' == props2.get_property('producer.purgatory.purge.interval.requests')


def test_zk_prefix_replacement():
    if os.getenv('ZOOKEEPER_PREFIX', None):
        os.unsetenv('ZOOKEEPER_PREFIX')
    assert load_config().zk_prefix == '/'

    os.environ['ZOOKEEPER_PREFIX'] = '/'
    assert load_config().zk_prefix == '/'

    os.environ['ZOOKEEPER_PREFIX'] = 'test'
    assert load_config().zk_prefix == '/test'

    os.environ['ZOOKEEPER_PREFIX'] = '/test'
    assert load_config().zk_prefix == '/test'


def test_parse_timeout():
    assert {'type': 'linear', 'initial': '300', 'step': '60'} == _load_timeout_dict(
        {'STARTUP_TIMEOUT_TYPE': 'linear', 'STARTUP_TIMEOUT_INITIAL': '300', 'STARTUP_TIMEOUT_STEP': '60'}.get)
    assert {'type': 'linear', 'step': '60'} == _load_timeout_dict(
        {'STARTUP_TIMEOUT_TYPE': 'linear', 'STARTUP_TIMEOUT_STEP': '60'}.get)
    assert {'initial': '300', 'step': '60'} == _load_timeout_dict(
        {'STARTUP_TIMEOUT_INITIAL': '300', 'STARTUP_TIMEOUT_STEP': '60'}.get)
