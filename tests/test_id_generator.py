from bubuku.id_generator import BrokerIDByIp
from test_config import build_test_properties


def _test_max_id_replaced_on_ip(ip):
    kafka_properties = build_test_properties()
    assert kafka_properties.get_property('reserved.broker.max.id') is None
    gen = BrokerIDByIp(None, ip, kafka_properties)

    id_ = int(gen.get_broker_id())
    max_id = int(kafka_properties.get_property('reserved.broker.max.id'))
    # check that it's not negative and it can hold all the information
    assert 3 * 256 * 256 * 256 <= max_id < 127 * 256 * 256 * 256

    assert id_ < max_id


def test_max_id_block_1():
    _test_max_id_replaced_on_ip('10.0.0.0')
    _test_max_id_replaced_on_ip('10.255.255.255')


def test_max_id_block_2():
    _test_max_id_replaced_on_ip('192.168.0.0')
    _test_max_id_replaced_on_ip('192.168.255.255')


def test_max_id_block_3():
    _test_max_id_replaced_on_ip('172.16.0.0')
    _test_max_id_replaced_on_ip('172.31.255.255')
