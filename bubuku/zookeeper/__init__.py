import json
import logging
import threading
import time
import uuid
from typing import Dict, List, Iterable, Tuple

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError, ConnectionLossException
from collections import defaultdict
from enum import Enum

_LOG = logging.getLogger('bubuku.exhibitor')


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


class SlowlyUpdatedCache(object):
    def __init__(self, load_func, update_func, refresh_timeout, delay):
        self.load_func = load_func
        self.update_func = update_func
        self.refresh_timeout = refresh_timeout
        self.delay = delay
        self.value = None
        self.last_check = None
        self.next_apply = None
        self.force = True

    def __str__(self):
        return 'SlowCache(refresh={}, delay={}, last_check={}, next_apply={})'.format(
            self.refresh_timeout, self.delay, self.last_check, self.next_apply)

    def touch(self):
        now = time.time()
        if self.last_check is None or (now - self.last_check) > self.refresh_timeout:
            value = None
            if self.force:
                while value is None:
                    value = self._load_value_safe()
                self.force = False
            else:
                value = self._load_value_safe()
            if value is not None:
                if value != self.value:
                    self.value = value
                    self.next_apply = (now + self.delay) if self.last_check is not None else now
                self.last_check = now
        if self.next_apply is not None and self.next_apply - now <= 0:
            self.update_func(self.value)
            self.next_apply = None

    def _load_value_safe(self):
        try:
            return self.load_func()
        except Exception as e:
            _LOG.error('Failed to load value to update', exc_info=e)
        return None


class AddressListProvider(object):
    def get_latest_address(self) -> (list, int):
        """
        Loads current address list from service. Can return None if value can't be refreshed at the moment
        :return: tuple of hosts, port for zookeeper
        """
        raise NotImplementedError


class _ZookeeperProxy(object):
    def __init__(self, address_provider: AddressListProvider, prefix: str):
        self.address_provider = address_provider
        self.async_counter = WaitingCounter(limit=100)
        self.conn_str = None
        self.client = None
        self.prefix = prefix
        self.hosts_cache = SlowlyUpdatedCache(
            self.address_provider.get_latest_address,
            self._update_hosts,
            30,  # Refresh every 30 seconds
            3 * 60)  # Update only after 180 seconds of stability

    def _update_hosts(self, value):
        hosts, port = value
        if hosts:
            self.conn_str = ','.join(['{}:{}'.format(h, port) for h in hosts]) + self.prefix
            if self.client is None:
                self.client = KazooClient(hosts=self.conn_str,
                                          command_retry={'deadline': 120, 'max_delay': 1, 'max_tries': -1},
                                          connection_retry={'max_delay': 1, 'max_tries': -1})
                self.client.add_listener(self.session_listener)
            else:
                self.client.stop()
                self.client.set_hosts(self.conn_str)
            self.client.start()

    def terminate(self):
        if self.client:
            self.client.stop()

    def session_listener(self, state):
        pass

    def get_conn_str(self):
        return self.conn_str

    def get(self, *params):
        self.hosts_cache.touch()
        return self.client.retry(self.client.get, *params)

    def get_async(self, *params):
        # Exhibitor is not polled here and it's totally fine!
        self.async_counter.increment()
        try:
            i_async = self.client.get_async(*params)
            i_async.rawlink(self._decrement)
            return i_async
        except Exception as e:
            self._decrement()
            raise e

    def _decrement(self, *args, **kwargs):
        self.async_counter.decrement()

    def set(self, *args, **kwargs):
        self.hosts_cache.touch()
        return self.client.retry(self.client.set, *args, **kwargs)

    def create(self, *args, **kwargs):
        self.hosts_cache.touch()
        return self.client.retry(self.client.create, *args, **kwargs)

    def delete(self, *args, **kwargs):
        self.hosts_cache.touch()
        try:
            return self.client.retry(self.client.delete, *args, **kwargs)
        except NoNodeError:
            pass

    def get_children(self, *args, **kwargs):
        self.hosts_cache.touch()
        try:
            return self.client.retry(self.client.get_children, *args, **kwargs)
        except NoNodeError:
            return []

    def take_lock(self, *args, **kwargs):
        while True:
            try:
                self.hosts_cache.touch()
                return self.client.Lock(*args, **kwargs)
            except Exception as e:
                _LOG.error('Failed to obtain lock for exhibitor, retrying', exc_info=e)


class ThrottleConfig(object):

    BROKER_FOLLOWER_THROTTLE_RATE = "follower.replication.throttle.rate"
    BROKER_LEADER_THROTTLE_RATE = "leader.replication.throttle.rate"
    TOPIC_LEADER_THROTTLE_REPLICAS = "leader.replication.throttled.replicas"
    TOPIC_FOLLOWER_THROTTLE_REPLICAS = "follower.replication.throttled.replicas"

    @classmethod
    def get_broker_throttle_properties(cls):
        return [cls.BROKER_LEADER_THROTTLE_RATE, cls.BROKER_FOLLOWER_THROTTLE_RATE]

    @classmethod
    def get_topic_throttle_properties(cls):
        return [cls.TOPIC_FOLLOWER_THROTTLE_REPLICAS, cls.TOPIC_LEADER_THROTTLE_REPLICAS]


class BukuExhibitor(object):
    def __init__(self, exhibitor: _ZookeeperProxy, async=True):
        self.exhibitor = exhibitor
        self.async = async
        for node in ('changes', 'actions/global'):
            try:
                self.exhibitor.create('/bubuku/{}'.format(node), makepath=True)
            except NodeExistsError:
                pass

    def __enter__(self):
        _LOG.info('Entered safe exhibitor space')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _LOG.info('Exiting safe exhibitor space')
        self.exhibitor.terminate()

    class ConfigEntityType(Enum):
        BROKER = "brokers"
        TOPIC = "topics"

    def is_broker_registered(self, broker_id):
        try:
            _, stat = self.exhibitor.get('/brokers/ids/{}'.format(broker_id))
            return stat is not None
        except NoNodeError:
            return False

    def get_broker_ids(self) -> List[str]:
        """
        Gets list of available broker ids
        :return: Sorted list of strings - active broker ids.
        """
        return sorted(self.exhibitor.get_children('/brokers/ids'))

    def get_broker_racks(self) -> Dict[int, str]:
        """
        Lists the rack of each broker, if it exists
        :return: a dictionary of tuples (broker_id, rack), where rack can be None
        """
        return {
        int(broker): json.loads(self.exhibitor.get('/brokers/ids/{}'.format(broker))[0].decode('utf-8')).get('rack') for
        broker in self.get_broker_ids()}

    def load_partition_assignment(self, topics=None) -> Iterable[Tuple[str, int, List[int]]]:
        """
        Lists all the assignments of partitions to particular broker ids.
        :param topics Optional list of topics to get data for
        :returns generator of tuples (topic_name:str, partition:int, replica_list:list(int)), for ex. "test", 0, [1,2,3]
        """
        topics_ = self.exhibitor.get_children('/brokers/topics') if topics is None else topics
        if self.async:
            results = [(topic, self.exhibitor.get_async('/brokers/topics/{}'.format(topic))) for topic in topics_]
            for topic, cb in results:
                try:
                    value, stat = cb.get(block=True)
                except ConnectionLossException:
                    value, stat = self.exhibitor.get('/brokers/topics/{}'.format(topic))
                data = json.loads(value.decode('utf-8'))
                for k, v in data['partitions'].items():
                    yield (topic, int(k), v)
        else:
            for topic in topics_:
                data = json.loads(self.exhibitor.get('/brokers/topics/{}'.format(topic))[0].decode('utf-8'))
                for k, v in data['partitions'].items():
                    yield (topic, int(k), v)

    def load_partition_isr(self, partitions: list) -> list:
        """
        Lists all the ISRs of partitions
        :param partitions: List of (topic, partition) tuples
        :return: generator of tuples
        (topic_name, str, partition: int, isr: tuple of int)
        """
        if not partitions:
            return
        if self.async:
            results = [(topic, partition, self.exhibitor.get_async(
                '/brokers/topics/{}/partitions/{}/state'.format(topic, partition))) for topic, partition in partitions]
            for topic, partition, result in results:
                try:
                    value, stat = result.get(block=True)
                except ConnectionLossException:
                    value, stat = self.exhibitor.get(
                        '/brokers/topics/{}/partitions/{}/state'.format(topic, partitions))
                yield (topic, int(partition), json.loads(value.decode('utf-8')).get('isr', []))
        else:
            return ((topic, partition, json.loads(self.exhibitor.get(
                '/brokers/topics/{}/partitions/{}/state'.format(topic, partition)[0])).decode('utf-8').get('isr', []))
                    for topic, partition in partitions)

    def load_partition_states(self, topics=None) -> list:
        """
        Lists all the current partition states (leaders and isr list)
        :return: generator of tuples
        (topic_name: str, partition: int, state: json from /brokers/topics/{}/partitions/{}/state)
        """
        if self.async:
            asyncs = []
            for topic, partition, _ in self.load_partition_assignment(topics):
                asyncs.append((topic, partition, self.exhibitor.get_async(
                    '/brokers/topics/{}/partitions/{}/state'.format(topic, partition))))
            for topic, partition, async in asyncs:
                try:
                    value, stat = async.get(block=True)
                except ConnectionLossException:
                    value, stat = self.exhibitor.get('/brokers/topics/{}/partitions/{}/state'.format(topic, partition))
                yield (topic, int(partition), json.loads(value.decode('utf-8')))
        else:
            topics_ = self.exhibitor.get_children('/brokers/topics') if topics is None else topics
            for topic in topics_:
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
        return self.reallocate_partitions([(topic, partition, replicas)])

    def remove_dynamic_configuration(self, entity_type: str, properties: list, entities=None):
        zk_config_path = "/config/{}".format(entity_type)
        entities = self.exhibitor.get_children(zk_config_path) if not entities else entities
        to_change_entities = set()
        for entity in entities:
            config, stats = self.exhibitor.get("/config/{}/{}".format(entity_type, entity))
            config = json.loads(config.decode('utf-8'))
            to_change = False
            for config_property in properties:
                if config_property in config.get('config', {}):
                    config.get('config').pop(config_property, None)
                    to_change = True
                if to_change:
                    to_change_entities.add((entity, json.dumps(config).encode('utf-8')))
        for entity, updated_config in to_change_entities:
            self.exhibitor.set("/config/{}/{}".format(entity_type, entity), updated_config)
            self._apply_change_notification(entity, entity_type)

    def apply_dynamic_config_changes(self, entity: str, changes: dict, entity_type: str):
        """
        Applies dynamic config changes using zookeeper
        :param entity: id of the entity (broker id or topic name)
        :param changes: dictionary containing property and key values
        :param entity_type: type of the entity (can be either 'brokers' or 'topics')
        """

        zk_config_path = "/config/{}/{}".format(entity_type, entity)
        try:
            config = json.loads(self.exhibitor.get(zk_config_path)[0].decode('utf-8'))
            updated_config = config
            for config_property, value in changes.items():
                updated_config.get('config', {})[config_property] = value
            self.exhibitor.set(zk_config_path, json.dumps(updated_config).encode('utf-8'))
        except NoNodeError:
            updated_config = {
                "version": 1,
                "config": changes
            }
            self.exhibitor.create(zk_config_path, json.dumps(updated_config).encode('utf-8'))
        self._apply_change_notification(entity, entity_type)

    def _apply_change_notification(self, entity: str, entity_type: str):
        notification_entity = {
            "version": 2,
            "entity_path": "{}/{}".format(entity_type, entity)
        }
        self.exhibitor.create("/config/changes/config_change_",
                              json.dumps(notification_entity).encode('utf-8'), sequence=True)

    def throttle_ongoing_rebalance(self, throttle):
        rebalance_json, zk_stats = self.exhibitor.get("/admin/reassign_partitions")
        self.apply_throttle(json.loads(rebalance_json.decode('utf-8')), throttle)

    def apply_throttle(self, data, throttle):
        """
        Applies throttle to the brokers and partitions based on reassignment-json-file data.
        :param data: Dictionary containing the reassignment-json-file data
        :param throttle: throttle to be applied to the brokers and the topics
        """
        self._apply_throttle_to_brokers(*self._add_throttle_replicas_per_topic(data), throttle)

    def _add_throttle_replicas_per_topic(self, data):

        replica_data = defaultdict(lambda: defaultdict(list))
        partitions = []
        for entry in data['partitions']:
            replica_data[entry['topic']][entry['partition']] = entry['replicas']
            partitions.append((entry['topic'], entry['partition']))

        partition_isrs = self.load_partition_isr(partitions)
        topic_changes = defaultdict(lambda: defaultdict(list))
        follower_replicas, leader_replicas = set(), set()

        for topic, partition, isrs in partition_isrs:
            follower_topic_replicas = ["{}:{}".format(partition, replica) for replica in
                                       replica_data[topic][partition] if replica not in isrs]
            leader_topic_replicas = ["{}:{}".format(partition, replica) for replica in isrs]
            leader_replicas = leader_replicas.union(set(isrs))
            follower_replicas = follower_replicas.union(
                set([replica for replica in replica_data[topic][partition] if replica not in isrs]))
            topic_changes[topic][ThrottleConfig.TOPIC_LEADER_THROTTLE_REPLICAS].extend(leader_topic_replicas)
            topic_changes[topic][ThrottleConfig.TOPIC_FOLLOWER_THROTTLE_REPLICAS].extend(follower_topic_replicas)

        for topic, throttle_replicas in topic_changes.items():
            throttle_config_changes = {}
            for _property in throttle_replicas:
                throttle_config_changes[_property] = ','.join(throttle_replicas[_property])
            self.apply_dynamic_config_changes(topic, throttle_config_changes, 'topics')

        return leader_replicas, follower_replicas

    def _apply_throttle_to_brokers(self, leader_replicas, follower_replicas, throttle):
        for replica in leader_replicas:
            self.apply_dynamic_config_changes(
                replica,
                {
                    ThrottleConfig.BROKER_LEADER_THROTTLE_RATE: str(throttle)
                },
                "brokers"
            )
        for replica in follower_replicas:
            self.apply_dynamic_config_changes(
                replica,
                {
                    ThrottleConfig.BROKER_FOLLOWER_THROTTLE_RATE: str(throttle)
                },
                "brokers"
            )

    def remove_throttle_configurations(self):
        self.remove_dynamic_configuration(
            entity_type="brokers", properties=ThrottleConfig.get_broker_throttle_properties())
        self.remove_dynamic_configuration(
            entity_type="topics", properties=ThrottleConfig.get_topic_throttle_properties())

    def reallocate_partitions(self, partitions_data: list, throttle: int = 0) -> bool:
        j = {
            "version": "1",
            "partitions": [
                {
                    "topic": topic,
                    "partition": int(partition),
                    "replicas": [int(p) for p in replicas]
                } for (topic, partition, replicas) in partitions_data]
        }
        try:
            if throttle:
                self.apply_throttle(j, throttle)
            data = json.dumps(j)
            self.exhibitor.create("/admin/reassign_partitions", data.encode('utf-8'))
            _LOG.info("Reallocating {}".format(data))
            return True
        except NodeExistsError:
            _LOG.info("Waiting for free reallocation slot, still in progress...")
        return False

    def update_disk_stats(self, broker_id: str, data: dict):
        data_bytes = json.dumps(data, separators=(',', ':')).encode('utf-8')
        path = '/bubuku/size_stats/{}'.format(broker_id)
        try:
            self.exhibitor.create(path, data_bytes, ephemeral=True, makepath=True)
        except NodeExistsError:
            self.exhibitor.set(path, data_bytes)

    def get_broker_address(self, broker_id):
        try:
            config = json.loads(self.exhibitor.get('/brokers/ids/{}'.format(broker_id))[0].decode('utf-8'))
            return config['host']
        except NoNodeError:
            return None

    def get_disk_stats(self) -> Dict[str, dict]:
        stats = {}
        for broker_id in self.get_broker_ids():
            try:
                broker_stats_data, zk_stat = self.exhibitor.get('/bubuku/size_stats/{}'.format(broker_id))
                broker_stats = json.loads(broker_stats_data.decode("utf-8"))
                stats[broker_id] = broker_stats
            except NoNodeError:
                pass
        return stats

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

    def register_action(self, data: dict, broker_id: str = 'global'):
        registered = False
        while not registered:
            name = '/bubuku/actions/{}/{}'.format(broker_id, uuid.uuid4())
            try:
                self.exhibitor.create(name, json.dumps(data).encode('utf-8'), makepath=True)
                _LOG.info('Action {} registered with name {}'.format(data, name))
                registered = True
            except NodeExistsError:
                pass

    def take_action(self, broker_id):
        paths = ['/bubuku/actions/global']
        if broker_id:
            paths.insert(0, '/bubuku/actions/{}'.format(broker_id))

        for base_path in paths:
            for action in self.exhibitor.get_children(base_path):
                name = '{}/{}'.format(base_path, action)
                try:
                    return json.loads(self.exhibitor.get(name)[0].decode('utf-8'))
                except Exception as e:
                    _LOG.error('Failed to take action {}, but anyway will remove it'.format(name), exc_info=e)
                finally:
                    self.exhibitor.delete(name)
        return None

    def lock(self, lock_data=None):
        return self.exhibitor.take_lock('/bubuku/global_lock', lock_data)

    def get_running_changes(self) -> dict:
        return {
            change: self.exhibitor.get('/bubuku/changes/{}'.format(change))[0].decode('utf-8')
            for change in self.exhibitor.get_children('/bubuku/changes')
        }

    def register_change(self, name, provider_id):
        _LOG.info('Registering change in zk: {}'.format(name))
        self.exhibitor.create('/bubuku/changes/{}'.format(name), provider_id.encode('utf-8'), ephemeral=True)

    def unregister_change(self, name):
        _LOG.info('Removing change {} from locks'.format(name))
        self.exhibitor.delete('/bubuku/changes/{}'.format(name), recursive=True)


def load_exhibitor_proxy(address_provider: AddressListProvider, prefix: str) -> BukuExhibitor:
    proxy = _ZookeeperProxy(address_provider, prefix)
    return BukuExhibitor(proxy)
