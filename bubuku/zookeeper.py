import json
import logging
import random
import threading
import time

import requests
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, NodeExistsError, ConnectionLossException
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

        json_ = self._query_exhibitors(self._exhibitors)
        if not json_:
            json_ = self._query_exhibitors(self._master_exhibitors)

        if isinstance(json_, dict) and 'servers' in json_ and 'port' in json_:
            self._next_poll = time.time() + self._poll_interval
            zookeeper_hosts = ','.join([h + ':' + str(json_['port']) for h in sorted(json_['servers'])])
            if self._zookeeper_hosts != zookeeper_hosts:
                _LOG.info('ZooKeeper connection string has changed: %s => %s', self._zookeeper_hosts, zookeeper_hosts)
                self._zookeeper_hosts = zookeeper_hosts
                self._exhibitors = json_['servers']
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


class WaitingCounter(object):
    def __init__(self, limit=100):
        self.limit = limit
        self.counter = 0
        self.cv = threading.Condition()

    def increment(self):
        with self.cv:
            while self.counter >= self.limit:
                self.cv.wait()
            self.counter += 1

    def decrement(self):
        with self.cv:
            self.counter -= 1
            self.cv.notify()


class _Exhibitor:
    def __init__(self, hosts, port, prefix):
        self.prefix = prefix
        self.async_counter = WaitingCounter(limit=100)
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

    def get_async(self, *params):
        # Exhibitor is not polled here and it's totally fine!
        self.async_counter.increment()
        try:
            i_async = self.client.get_async(*params)
            i_async.rawlink(self.async_counter.decrement)
            return i_async
        except Exception as e:
            self.async_counter.decrement()
            raise e

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


class BukuExhibitor(object):
    def __init__(self, exhibitor: _Exhibitor, async=True):
        self.exhibitor = exhibitor
        self.async = async
        try:
            self.exhibitor.create('/bubuku/changes', makepath=True)
        except NodeExistsError:
            pass

    def is_broker_registered(self, broker_id):
        try:
            _, stat = self.exhibitor.get('/brokers/ids/{}'.format(broker_id))
            return stat is not None
        except NoNodeError:
            return False

    def get_broker_ids(self) -> list:
        """
        Gets list of available broker ids
        :return: Sorted list of strings - active broker ids.
        """
        return sorted(self.exhibitor.get_children('/brokers/ids'))

    def load_partition_assignment(self) -> list:
        """
        Lists all the assignments of partitions to particular broker ids.
        :returns generator of tuples (topic_name:str, partition:int, replica_list:list(int)), for ex. "test", 0, [1,2,3]
        """
        if self.async:
            results = [(topic, self.exhibitor.get_async('/brokers/topics/{}'.format(topic))) for topic in
                       self.exhibitor.get_children('/brokers/topics')]
            for topic, cb in results:
                try:
                    value, stat = cb.get(block=True)
                except ConnectionLossException:
                    value, stat = self.exhibitor.get('/brokers/topics/{}'.format(topic))
                data = json.loads(value.decode('utf-8'))
                for k, v in data['partitions'].items():
                    yield (topic, int(k), v)

        else:
            for topic in self.exhibitor.get_children('/brokers/topics'):
                data = json.loads(self.exhibitor.get('/brokers/topics/{}'.format(topic))[0].decode('utf-8'))
                for k, v in data['partitions'].items():
                    yield (topic, int(k), v)

    def load_partition_states(self) -> list:
        """
        Lists all the current partition states (leaders and isr list)
        :return: generator of tuples
        (topic_name: str, partition: int, state: json from /brokers/topics/{}/partitions/{}/state)
        """
        if self.async:
            asyncs = []
            for topic, partition, _ in self.load_partition_assignment():
                asyncs.append((topic, partition, self.exhibitor.get_async(
                    '/brokers/topics/{}/partitions/{}/state'.format(topic, partition))))
            for topic, partition, async in asyncs:
                try:
                    value, stat = async.get(block=True)
                except ConnectionLossException:
                    value, stat = self.exhibitor.get('/brokers/topics/{}/partitions/{}/state'.format(topic, partition))
                yield (topic, int(partition), json.loads(value.decode('utf-8')))
        else:
            for topic in self.exhibitor.get_children('/brokers/topics'):
                for partition in self.exhibitor.get_children('/brokers/topics/{}/partitions'.format(topic)):
                    state = json.loads(self.exhibitor.get('/brokers/topics/{}/partitions/{}/state'.format(
                        topic, partition))[0].decode('utf-8'))
                    yield (topic, int(partition), state)

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

    def update_disk_stats(self, broker_id: str, data: dict):
        data_bytes = json.dumps(data, sort_keys=True).encode('utf-8')
        path = '/bubuku/size_stats/{}'.format(broker_id)
        try:
            self.exhibitor.create(path, data_bytes, ephemeral=True, makepath=True)
        except NodeExistsError:
            self.exhibitor.set(path, data_bytes)

    def get_conn_str(self):
        """
        Calculates connection string in format usable by kafka
        :return: connection string in form host:port[,host:port[...]]/path
        """
        return self.exhibitor.get_conn_str()

    def is_rebalancing(self):
        try:
            rebalance_data = self.exhibitor.get('/admin/reassign_partitions')[0].decode('utf-8')
            _LOG.info('Old rebalance is still in progress: {}, waiting'.format(rebalance_data))
            return True
        except NoNodeError:
            return False

    def lock(self, lock_data=None):
        return self.exhibitor.take_lock('/bubuku/global_lock', lock_data)

    def get_running_changes(self) -> dict:
        return {
            change: self.exhibitor.get('/bubuku/changes/{}'.format(change))[0].decode('utf-8')
            for change in self.exhibitor.get_children('/bubuku/changes')
            }

    def register_change(self, name, ip):
        _LOG.info('Registering change in zk: {}'.format(name))
        self.exhibitor.create('/bubuku/changes/{}'.format(name), ip.encode('utf-8'), ephemeral=True)

    def unregister_change(self, name):
        _LOG.info('Removing change {} from locks'.format(name))
        self.exhibitor.delete('/bubuku/changes/{}'.format(name), recursive=True)


def load_exhibitor_proxy(initial_hosts: list, zookeeper_prefix) -> BukuExhibitor:
    return BukuExhibitor(_Exhibitor(initial_hosts, 8181, zookeeper_prefix))
