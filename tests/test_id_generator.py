from bubuku.id_generator import BrokerIDByIp
from test_config import build_test_properties


def test_max_id_replaced_on_ip():
    kafka_properties = build_test_properties()
    assert kafka_properties.get_property('reserved.broker.max.id') is None
    BrokerIDByIp(None, '192.168.1.1', kafka_properties)

    # check that it's not negative and it can hold all the information
    assert 3 * 256 * 256 * 256 <= int(kafka_properties.get_property('reserved.broker.max.id')) < 127 * 256 * 256 * 256
