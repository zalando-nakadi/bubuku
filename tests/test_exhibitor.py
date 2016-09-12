import unittest
from unittest.mock import MagicMock

from bubuku.zookeeper.exhibitor import ExhibitorAddressProvider


class ExhibitorAddressProviderTest(unittest.TestCase):
    def test_get_latest_address(self):
        address_provider = ExhibitorAddressProvider(lambda: ['aws-lb-1', 'aws-lb-2'])
        address_provider._query_exhibitors = lambda _: {'servers': ['aws-lb-1-new'], 'port': 99}

        actual_result = address_provider.get_latest_address()

        assert actual_result == (['aws-lb-1-new'], 99)

    def test_get_latest_address_no_exhibitors(self):
        address_provider = ExhibitorAddressProvider(lambda: ['aws-lb-1', 'aws-lb-2'])
        address_provider._query_exhibitors = lambda _: None

        actual_result = address_provider.get_latest_address()
        assert actual_result is None

    def test_get_latest_address_2(self):
        address_provider = ExhibitorAddressProvider(lambda: ['aws-lb-1', 'aws-lb-2'])
        address_provider._query_exhibitors = MagicMock()
        address_provider._query_exhibitors.side_effect = [None, {'servers': ['aws-lb-1-new'], 'port': 99}]

        actual_result = address_provider.get_latest_address()

        assert address_provider._query_exhibitors.call_count == 2
        assert actual_result == (['aws-lb-1-new'], 99)

    def test_addresses_are_sorted(self):
        address_provider = ExhibitorAddressProvider(lambda: ['aws-lb-1', 'aws-lb-2'])
        address_provider._query_exhibitors = lambda _: {'servers': ['1', '2', '3'], 'port': '1234'}
        tmp_result = address_provider.get_latest_address()

        # Check that two calls in sequence will return the same value
        assert tmp_result == address_provider.get_latest_address()

        # Check sort 1
        address_provider._query_exhibitors = lambda _: {'servers': ['2', '1', '3'], 'port': '1234'}
        assert tmp_result == address_provider.get_latest_address()

        # Check sort again (just to be sure)
        address_provider._query_exhibitors = lambda _: {'servers': ['3', '2', '1'], 'port': '1234'}
        assert tmp_result == address_provider.get_latest_address()

