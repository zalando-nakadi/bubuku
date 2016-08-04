import json
import logging
import random
import time

import requests
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError
from requests.exceptions import RequestException

_LOG = logging.getLogger('bubuku.exhibitor')


class ExhibitorEnsembleProvider:
    TIMEOUT = 3.1

    def __init__(self, hosts, port, uri_path='/exhibitor/v1/cluster/list', poll_interval=300):
        self._exhibitor_port = port
        self._uri_path = uri_path
        self._poll_interval = poll_interval
        self._exhibitors = hosts
        self._master_exhibitors = hosts
        self._zookeeper_hosts = ''
        self._next_poll = None
        while not self.poll():
            _LOG.info('waiting on exhibitor')
            time.sleep(5)

    def poll(self):
        if self._next_poll and self._next_poll > time.time():
            return False

        json = self._query_exhibitors(self._exhibitors)
        if not json:
            json = self._query_exhibitors(self._master_exhibitors)

        if isinstance(json, dict) and 'servers' in json and 'port' in json:
            self._next_poll = time.time() + self._poll_interval
            zookeeper_hosts = ','.join([h + ':' + str(json['port']) for h in sorted(json['servers'])])
            if self._zookeeper_hosts != zookeeper_hosts:
                _LOG.info('ZooKeeper connection string has changed: %s => %s', self._zookeeper_hosts, zookeeper_hosts)
                self._zookeeper_hosts = zookeeper_hosts
                self._exhibitors = json['servers']
                return True
        return False

    def _query_exhibitors(self, exhibitors):
        if exhibitors == [None]:
            return {'servers': ['localhost'], 'port': 2181}
        random.shuffle(exhibitors)
        for host in exhibitors:
            uri = 'http://{}:{}{}'.format(host, self._exhibitor_port, self._uri_path)
            try:
                response = requests.get(uri, timeout=self.TIMEOUT)
                return response.json()
            except RequestException:
                pass
        return None

    @property
    def zookeeper_hosts(self):
        return self._zookeeper_hosts


class Exhibitor:
    def __init__(self, hosts, port, prefix):
        self.prefix = prefix
        self.exhibitor = ExhibitorEnsembleProvider(hosts, port, poll_interval=30)
        self.client = KazooClient(hosts=self.exhibitor.zookeeper_hosts + self.prefix,
                                  command_retry={
                                      'deadline': 10,
                                      'max_delay': 1,
                                      'max_tries': -1},
                                  connection_retry={'max_delay': 1, 'max_tries': -1})
        self.client.add_listener(self.session_listener)
        self.client.start()

    def session_listener(self, state):
        pass

    def get_conn_str(self):
        return self.exhibitor.zookeeper_hosts + self.prefix

    def _poll_exhibitor(self):
        if self.exhibitor.poll():
            self.client.stop()
            self.client.set_hosts(self.get_conn_str())
            self.client.start()

    def get(self, *params):
        self._poll_exhibitor()
        return self.client.retry(self.client.get, *params)

    def set(self, *args, **kwargs):
        self._poll_exhibitor()
        return self.client.retry(self.client.set, *args, **kwargs)

    def create(self, *args, **kwargs):
        self._poll_exhibitor()
        return self.client.retry(self.client.create, *args, **kwargs)

    def delete(self, *args, **kwargs):
        self._poll_exhibitor()
        return self.client.retry(self.client.delete, *args, **kwargs)

    def get_children(self, *params):
        self._poll_exhibitor()
        try:
            return self.client.retry(self.client.get_children, *params)
        except NoNodeError:
            return []

    def take_lock(self, *args, **kwargs):
        while True:
            try:
                self._poll_exhibitor()
                return self.client.Lock(*args, **kwargs)
            except Exception as e:
                _LOG.error('Failed to obtain lock for exhibitor, retrying', exc_info=e)


class BukuProxy(object):
    def __init__(self, exhibitor: Exhibitor):
        self.exhibitor = exhibitor

    def get_broker_ids(self) -> list:
        """
        Gets list of available broker ids
        :return: Sorted list of strings - active broker ids.
        """
        return sorted(self.exhibitor.get('/brokers/ids'))

    def load_partition_assignment(self) -> list:
        """
        Lists all the assignments of partitions to particular broker ids.
        :returns list of tuples (topic_name:str, partition:int, replica_list:list(int)), for ex. "test", 0, [1,2,3]
        """
        result = []
        for topic in self.exhibitor.get_children('/brokers/topics'):
            data = json.loads(self.exhibitor.get("/brokers/topics/" + topic)[0].decode('utf-8'))
            for k, v in data['partitions'].items():
                result.append((topic, int(k), v))
        return result

    def reallocate_partition(self, topic: str, partition: object, replicas: list) -> bool:
        """
        Reallocates partition to replica list
        :param topic: topic to move
        :param partition: partition to move (can be str or int)
        :param replicas: list of replicas to move to
        :return: If reallocation was successful (node for reallocation was created)
        """
        j = {
            "version": "1",
            "partitions": [
                {
                    "topic": topic,
                    "partition": int(partition),
                    "replicas": [int(p) for p in replicas],
                }
            ]
        }
        try:
            data = json.dumps(j)
            self.exhibitor.create("/admin/reassign_partitions", data.encode('utf-8'))
            _LOG.info("Reallocating {}".format(data))
            return True
        except NodeExistsError:
            _LOG.info("Waiting for free reallocation slot, still in progress...")
        return False

    def is_rebalancing(self):
        try:
            rebalance_data = self.exhibitor.get('/admin/reassign_partitions')[0].decode('utf-8')
            _LOG.info('Old rebalance is still in progress: {}, waiting'.format(rebalance_data))
            return True
        except NoNodeError:
            return False


def load_exhibitor(initial_hosts: list, zookeeper_prefix):
    return Exhibitor(initial_hosts, 8181, zookeeper_prefix)
