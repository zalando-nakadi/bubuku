import logging
from typing import List

from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.zookeeper import BukuExhibitor

FAKE_ZONE = 'fake_zone'

FAKE_BROKER_ID = 'fake_id'


class Partition(object):
    def __init__(self, event_type: str, partition: str, brokers: List['Broker']):
        self.event_type = event_type
        self.partition = partition
        self.brokers = brokers

    def get_key(self):
        return self.event_type, self.partition

    def change_assignment(self, from_: 'Broker', to: 'Broker', leader: bool):
        for idx in range(0, len(self.brokers)):
            if self.brokers[idx] != from_:
                continue
            if (idx == 0 and not leader) or (leader and idx != 0):
                raise Exception('Inconsistency because of poor data structure model')
            from_.remove_partition(leader, self)
            to.add_partition(leader, self)
            self.brokers[idx] = to
            return
        raise Exception('Inconsistency because of poor data structure model2')

    def optimize_leaders(self, only_solve_overload=True):
        leader = self.brokers[0]
        if only_solve_overload and not leader.is_leaders_overloaded():
            return False
        smallest_load = None
        smallest_load_idx = None
        for idx in range(1, len(self.brokers)):
            load_factor = self.brokers[idx].get_load_factor(True, 1)
            if smallest_load is None or smallest_load > load_factor:
                smallest_load = load_factor
                smallest_load_idx = idx
        if smallest_load_idx is None:
            return False
        less_loaded = self.brokers[smallest_load_idx]

        if less_loaded.is_leaders_overloaded(1):
            return False

        if leader.leader_count - less_loaded.leader_count < 2:
            return False

        leader.remove_partition(True, self)
        if not leader.add_partition(False, self):
            raise Exception('Can not add partition {}'.format(self.get_key()))

        less_loaded.remove_partition(False, self)
        if not less_loaded.add_partition(True, self):
            raise Exception('Can not add partition {}'.format(self.get_key()))

        self.brokers[0] = less_loaded
        self.brokers[smallest_load_idx] = leader
        return True


class Broker(object):
    def __init__(self, id_: str, zone: str):
        self.id_ = id_
        self.zone = zone
        self.replica_count = 0
        self.leader_count = 0
        self.expected_replica_count = (0, 0)
        self.expected_leader_count = (0, 0)
        self.partitions = {}
        self.healthy = True

    def get_id(self):
        return self.id_

    def set_partitions_estimation(self, replica_counts, leader_count):
        self.expected_replica_count = replica_counts
        self.expected_leader_count = leader_count

    def get_load_factor(self, for_leader, add=0):
        if for_leader:
            return float(self.leader_count + add) / self.expected_leader_count[1]
        else:
            return float(self.replica_count + add) / self.expected_replica_count[1]

    def is_leaders_overloaded(self, add=0):
        return (self.leader_count + add) > self.expected_leader_count[1]

    def is_overloaded(self, for_leader, add=0):
        if for_leader:
            return (self.leader_count + add) > self.expected_leader_count[1]
        else:
            return (self.replica_count + add) > self.expected_replica_count[1]

    def list_partition_keys(self):
        result = []
        for pkey, leaderArray in self.partitions.items():
            for is_leader in leaderArray:
                result.append((pkey, is_leader))
        return result

    def is_leader_for(self, pkey):
        return any(self.partitions.get(pkey, []))

    def copy_partition_keys(self):
        return [k for k in self.partitions.keys()]

    def iterate(self, leader_only: bool):
        for pkey, leader_array in self.partitions.items():
            if leader_only:
                if any(leader_array):
                    yield pkey
            else:
                yield pkey

    def add_partition(self, leader: bool, partition: Partition):
        key = partition.get_key()
        if key in self.partitions:
            return False
        self._add_partition_int(key, leader)
        return True

    def remove_partition(self, leader: bool, partition: Partition):
        key = partition.get_key()
        if key not in self.partitions:
            raise Exception('Trying to detach partition that was not attached to broker')
        data = self.partitions[key]
        for idx in range(0, len(data)):
            if bool(leader) == bool(data[idx]):
                del data[idx]
                if leader:
                    self.leader_count -= 1
                self.replica_count -= 1
                if not data:
                    del self.partitions[key]
                return
        raise Exception("Well, something bad happen. Partition is removed, but it is not a leader or whatever.")

    def _add_partition_int(self, key, leader):
        if key not in self.partitions:
            self.partitions[key] = []
        self.partitions[key].append(leader)
        if leader:
            self.leader_count += 1
        self.replica_count += 1

    def can_accept_partition(self, partition: Partition):
        for b in partition.brokers:
            if b.id_ == self.id_:
                return False
        return True


class ZoneChecker(object):
    def __init__(self, zones: List[str]):
        self.zones = zones

    def is_allowed_move(self, from_: Broker, to: Broker, partition: Partition):
        current_zone_distribution = {}
        for b in partition.brokers:
            if b.zone not in self.zones:
                continue
            current_zone_distribution[b.zone] = current_zone_distribution.get(b.zone, 0) + 1
        if from_.zone in current_zone_distribution:
            current_zone_distribution[from_.zone] -= 1
        if 0 == current_zone_distribution.get(to.zone, 0):
            return True
        # Allow move only to zone with minimal amount of partitions
        return current_zone_distribution[to.zone] == min(current_zone_distribution.values())


class FakeBroker(Broker):
    def __init__(self):
        super(FakeBroker, self).__init__(FAKE_BROKER_ID, FAKE_ZONE)
        self.healthy = False

    def add_partition(self, leader: bool, partition: Partition):
        self._add_partition_int(partition.get_key(), leader)
        return True


def transfer_partition(partition, leader: bool, from_: Broker, target_brokers_sorted: List[Broker],
                       zone_checker: ZoneChecker, polite=False):
    for target in target_brokers_sorted:
        if not zone_checker.is_allowed_move(from_, target, partition):
            continue
        if polite and target.is_overloaded(leader, 1):
            continue
        if target.can_accept_partition(partition):
            partition.change_assignment(from_, target, leader)
            return True
    return False


def get_division_result_min_max(nom, denom):
    if denom == 0:
        return 0, 0
    return nom // denom, ((nom - 1) // denom) + 1


_LOG = logging.getLogger('simple_rebalance')


class SimpleRebalancer(BaseRebalanceChange):
    _STATE_INIT = 'init'
    _STATE_DISTRIBUTE_AMONG = 'distribute'
    _STATE_EMPTY_FAKE = 'empty_fake'
    _STATE_OPTIMIZE_REPLICAS = 'optimize_replicas'
    _STATE_OPTIMIZE_LEADERS = 'optimize_leaders'
    _STATE_BALANCE = 'balance'

    def __init__(self, zk: BukuExhibitor, broker_ids: list, empty_brokers: list, exclude_topics: list,
                 parallelism: int):
        self.state = self._STATE_INIT
        self.zk = zk
        self.fake = FakeBroker()
        self.active_brokers = {}
        self.partitions = {}
        self.rebalance_queue = {}
        self.parallelism = parallelism
        self.exclude_topics = exclude_topics if exclude_topics else []
        self.empty_brokers = [str(e) for e in empty_brokers] if empty_brokers else []
        self.initial_broker_ids = sorted([str(broker_id) for broker_id in broker_ids])
        self.zone_checker = None

    def register_partition_change(self, partition: Partition):
        self.rebalance_queue[(partition.event_type, partition.partition)] = partition

    def loadDataFromZk(self):
        for broker_id, broker_rack in self.zk.get_broker_racks().items():
            str_broker_id = str(broker_id)
            if str_broker_id in self.empty_brokers:
                continue
            self.active_brokers[str_broker_id] = Broker(str_broker_id, str(broker_rack))

        for topic, partition, broker_ids in self.zk.load_partition_assignment():
            if topic in self.exclude_topics:
                continue
            p = Partition(topic, str(partition), [self.active_brokers.get(str(id_), self.fake) for id_ in broker_ids])
            self.partitions[p.get_key()] = p

    def createInitialDistribution(self):
        total_partitions = 0
        total_leaders = 0
        az_counts = {}
        for broker in self.active_brokers.values():
            az_counts[broker.zone] = az_counts.get(broker.zone, 0) + 1
        self.zone_checker = ZoneChecker([zone for zone in az_counts.keys()])

        for partition in self.partitions.values():
            replication_factor = len(partition.brokers)

            to_fake = []
            for broker_idx in range(0, len(partition.brokers)):
                added = False
                if partition.brokers[broker_idx].get_id() in self.active_brokers:
                    broker = partition.brokers[broker_idx]
                    if self.zone_checker.is_allowed_move(broker, broker, partition):
                        if broker.add_partition(broker_idx == 0, partition):
                            added = True
                if not added:
                    partition.brokers[broker_idx] = self.fake
                    to_fake.append(broker_idx == 0)
                    self.register_partition_change(partition)

            if len(to_fake) == replication_factor:
                _LOG.warning('Partition {} is lost, cause no one have it'.format(partition.get_key))
                continue

            for leader in to_fake:
                self.fake.add_partition(leader, partition)
            total_partitions += replication_factor
            total_leaders += 1

        leaders_min, leaders_max = get_division_result_min_max(total_leaders, len(self.active_brokers))

        per_zone_partitions_min, per_zone_partitions_max = get_division_result_min_max(total_partitions, len(az_counts))

        for zone, count in az_counts.items():
            per_broker_partitions_min, _ = get_division_result_min_max(per_zone_partitions_min, count)
            _, per_broker_partitions_max = get_division_result_min_max(per_zone_partitions_max, count)
            for broker in self.active_brokers.values():
                if broker.zone == zone:
                    broker.set_partitions_estimation(
                        (per_broker_partitions_min, per_broker_partitions_max),
                        (leaders_min, leaders_max),
                    )

    def empty_fake(self):
        leaders_load = sorted(self.active_brokers.values(), key=lambda x: x.get_load_factor(True))
        replica_load = sorted(self.active_brokers.values(), key=lambda x: x.get_load_factor(False))

        for pkey, is_leader in self.fake.list_partition_keys():
            load = leaders_load if is_leader else replica_load
            partition = self.partitions[pkey]
            if not transfer_partition(partition, is_leader, self.fake, load, self.zone_checker, polite=False):
                raise Exception('Nowhere to transfer partition {}'.format(pkey))
            self.register_partition_change(partition)
            if is_leader:
                leaders_load = sorted(leaders_load, key=lambda x: x.get_load_factor(True))
            else:
                replica_load = sorted(replica_load, key=lambda x: x.get_load_factor(True))

    def optimize_replicas(self):
        is_leader = False
        current_load = sorted(self.active_brokers.values(), key=lambda x: x.get_load_factor(is_leader))
        moved = True
        while moved:
            to_empty = current_load[len(current_load) - 1]
            if not to_empty.is_overloaded(is_leader):
                return
            moved = False
            for pkey in to_empty.copy_partition_keys():
                partition = self.partitions[pkey]
                if not transfer_partition(partition, to_empty.is_leader_for(pkey), to_empty, current_load,
                                          self.zone_checker, polite=True):
                    continue
                moved = True
                self.register_partition_change(partition)
                if not to_empty.is_overloaded(is_leader):
                    break
            if moved:
                current_load = sorted(current_load, key=lambda x: x.get_load_factor(is_leader))
            self.print_load()

    def print_load(self):
        print('Current load: {}'.format(['{}/{},{}/{}'.format(b.leader_count, b.expected_leader_count[1],
                                                              b.replica_count, b.expected_replica_count[1]) for b in
                                         self.active_brokers.values()]))

    def optimize_leaders(self):
        # First step, try to balance leaders for partitions that we are already moving
        has_modifications = True
        while has_modifications:
            has_modifications = False
            for p in self.rebalance_queue.values():
                if p.optimize_leaders():
                    has_modifications = True
                    self.print_load()
            if not has_modifications:
                for p in self.rebalance_queue.values():
                    if p.optimize_leaders(only_solve_overload=False):
                        has_modifications = True
                        self.print_load()
            if not has_modifications:
                for p in self.partitions.values():
                    if not p.optimize_leaders():
                        continue
                    self.register_partition_change(p)
                    has_modifications = True
                    self.print_load()
            if not has_modifications:
                for p in self.partitions.values():
                    if not p.optimize_leaders(only_solve_overload=False):
                        continue
                    self.register_partition_change(p)
                    has_modifications = True
                    self.print_load()

    def perform_rebalance(self):
        to_rebalance = [k for k in self.rebalance_queue.keys()]
        if len(to_rebalance) > self.parallelism:
            to_rebalance = to_rebalance[:self.parallelism]
        to_rebalance = [self.rebalance_queue.pop(k) for k in to_rebalance]
        if not to_rebalance:
            return True
        to_rebalance_data = [
            (p.event_type, int(p.partition), [b.id_ for b in p.brokers])
            for p in to_rebalance
        ]
        if not self.zk.reallocate_partitions(to_rebalance_data):
            for partition in to_rebalance:
                self.register_partition_change(partition)
        return False

    def run(self, current_actions) -> bool:
        # Stop rebalance if someone is restarting
        if self.should_be_paused(current_actions):
            _LOG.warning("Rebalance paused, because other blocking events running: {}".format(current_actions))
            return True
        if self.zk.is_rebalancing():
            return True

        new_broker_ids = sorted([str(id_) for id_ in self.zk.get_broker_ids()])
        if new_broker_ids != self.initial_broker_ids:
            _LOG.warning("Rebalance stopped because of broker list change from {} to {}".format(
                self.initial_broker_ids, new_broker_ids))
            return False

        if self.state == SimpleRebalancer._STATE_INIT:
            # Load data from zk
            self.loadDataFromZk()
            if not self.active_brokers:
                _LOG.info("no active brokers, can not do rebalance")
                return False
            self.state = SimpleRebalancer._STATE_DISTRIBUTE_AMONG
        elif self.state == SimpleRebalancer._STATE_DISTRIBUTE_AMONG:
            # Sort and distribute to entities
            self.createInitialDistribution()
            self.state = SimpleRebalancer._STATE_EMPTY_FAKE
        elif self.state == SimpleRebalancer._STATE_EMPTY_FAKE:
            # Move data around from non-existent brokers
            self.empty_fake()
            self.state = SimpleRebalancer._STATE_OPTIMIZE_REPLICAS
        elif self.state == SimpleRebalancer._STATE_OPTIMIZE_REPLICAS:
            # Now try to evenly distribute partitions/leaders among brokers
            self.optimize_replicas()
            self.state = SimpleRebalancer._STATE_OPTIMIZE_LEADERS
        elif self.state == SimpleRebalancer._STATE_OPTIMIZE_LEADERS:
            # Now try to evenly distribute partitions/leaders among brokers
            self.optimize_leaders()
            self.state = SimpleRebalancer._STATE_BALANCE
        elif self.state == SimpleRebalancer._STATE_BALANCE:
            rebalance_finished = self.perform_rebalance()
            return not rebalance_finished
        else:
            _LOG.warning("Stopping rebalance, as state {} is not supported".format(self.state))
            return False
        return True

    def __str__(self):
        return 'SimpleRebalance state={}, queue_size={}, parallelism={}'.format(
            self.state, len(self.rebalance_queue), self.parallelism)
