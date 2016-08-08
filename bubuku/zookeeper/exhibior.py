import logging
import random

import requests
from requests import RequestException

from bubuku.amazon import Amazon
from bubuku.zookeeper import AddressListProvider

_LOG = logging.getLogger('bubuku.zookeeper.exhibitor')


class AWSExhibitorAddressProvider(AddressListProvider):
    def __init__(self, amazon: Amazon, zk_stack_name: str):
        self.master_exhibitors = amazon.get_addresses_by_lb_name(zk_stack_name)
        self.exhibitors = list(self.master_exhibitors)

    def get_latest_address(self) -> (list, int):
        json_ = self._query_exhibitors(self.exhibitors)
        if not json_:
            json_ = self._query_exhibitors(self.master_exhibitors)
        if isinstance(json_, dict) and 'servers' in json_ and 'port' in json_:
            self.exhibitors = json_['servers']
            return json_['servers'], int(json_['port'])
        return None

    def _query_exhibitors(self, exhibitors):
        random.shuffle(exhibitors)
        for host in exhibitors:
            url = 'http://{}:{}{}'.format(host, 8181, '/exhibitor/v1/cluster/list')
            try:
                response = requests.get(url, timeout=3.1)
                return response.json()
            except RequestException as e:
                _LOG.warn('Failed to query zookeeper list information from {}'.format(url), exc_info=e)
        return None
