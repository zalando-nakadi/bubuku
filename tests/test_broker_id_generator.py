import unittest

from bubuku.id_extractor import _search_broker_id


class TestBrokerIdExtractor(unittest.TestCase):
    def test_match_valid(self):
        assert '123534' == _search_broker_id(['broker.id=123534'])
        assert '123534' == _search_broker_id(['\tbroker.id=123534'])
        assert '123534' == _search_broker_id(['\tbroker.id=123534\n'])
        assert '123534' == _search_broker_id(['broker.id=123534 \n\r'])
        assert '123534' == _search_broker_id(['\tbroker.id=123534 \r'])
        assert '123534' == _search_broker_id(['xbroker.id=1', 'broker.id=123534'])
        assert '123534' == _search_broker_id(['broker.id=123534', 'boker.id=123534'])

    def test_match_invalid(self):
        assert _search_broker_id([]) is None
        assert _search_broker_id(['broker_id=123534']) is None
        assert _search_broker_id(['xbroker.id=1', 'broker.id=12f3534']) is None
        assert _search_broker_id(['bruker.id=123534', 'boker.id=123534']) is None
