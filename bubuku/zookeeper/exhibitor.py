import logging
import random

import requests
from requests import RequestException

from bubuku.zookeeper import AddressListProvider

_LOG = logging.getLogger('bubuku.zookeeper.exhibitor')


class ExhibitorAddressProvider(AddressListProvider):
    def __init__(self, initial_list_provider):
        self.initial_list_provider = initial_list_provider
        self.exhibitors = []

    def get_latest_address(self) -> (list, int):
        json_ = self._query_exhibitors(self.exhibitors)
        if not json_:
            self.exhibitors = self.initial_list_provider()
            json_ = self._query_exhibitors(self.exhibitors)
        if isinstance(json_, dict) and 'servers' in json_ and 'port' in json_:
            self.exhibitors = json_['servers']
            return sorted(json_['servers']), int(json_['port'])
        return None

    def _query_exhibitors(self, exhibitors):
        if not exhibitors:
            return None
        random.shuffle(exhibitors)
        for host in exhibitors:
            url = 'http://{}:{}{}'.format(host, 8181, '/exhibitor/v1/cluster/list')
            try:
                response = requests.get(url, timeout=3.1)
                return response.json()
            except RequestException as e:
                _LOG.warn('Failed to query zookeeper list information from {}'.format(url), exc_info=e)
            except ConnectionError as e:
                _LOG.warn('Failed to connect to zookeeper instance {}'.format(url), exc_info=e)
            except Exception as e:
                _LOG.warn('Unknown error connecting to zookeeper instance {}'.format(url), exc_info=e)
        return None
