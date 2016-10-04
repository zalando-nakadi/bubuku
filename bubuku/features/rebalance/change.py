import logging

from bubuku.features.rebalance import BaseRebalanceChange
from bubuku.features.rebalance.broker import BrokerDescription
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rebalance')


def distribute(amount: int, items: list, weight_key):
    if not items:
        return []
    items = sorted(items, key=weight_key, reverse=True)
    ceil_amount = int(amount / len(items))
    amounts = [ceil_amount for _ in range(0, len(items))]
    for idx in range(0, min(amount - sum(amounts), len(items))):
        amounts[idx] += 1
    return amounts


class OptimizedRebalanceChange(BaseRebalanceChange):
    _LOAD_STATE = 'load_state'
    _COMPUTE_LEADERS = 'compute_leaders'
    _COMPUTE_REPLICAS = 'compute_replicas'
    _SORT_ACTIONS = 'sort_actions'
    _BALANCE = 'balance'

    def __init__(self, zk: BukuExhibitor, broker_ids: list):
        self.zk = zk
        self.broker_ids = sorted(int(id_) for id_ in broker_ids)
        self.broker_distribution = None
        self.source_distribution = None
        self.action_queue = None
        self.state = OptimizedRebalanceChange._LOAD_STATE

    def __str__(self):
        return 'OptimizedRebalance state={}, queue_size={}'.format(
            self.state, len(self.action_queue) if self.action_queue is not None else None)

    def run(self, current_actions) -> bool:
        # Stop rebalance if someone is restarting
        if self.should_be_paused(current_actions):
            _LOG.warning("Rebalance paused, because other blocking events running: {}".format(current_actions))
            return True
        if self.zk.is_rebalancing():
            return True
        new_broker_ids = sorted(int(id_) for id_ in self.zk.get_broker_ids())
        if new_broker_ids != self.broker_ids:
            _LOG.warning("Rebalance stopped because of broker list change from {} to {}".format(
                self.broker_ids, new_broker_ids))
            return False
        if self.state == OptimizedRebalanceChange._LOAD_STATE:
            self._load_data()
            self.state = OptimizedRebalanceChange._COMPUTE_LEADERS
        elif self.state == OptimizedRebalanceChange._COMPUTE_LEADERS:
            self.action_queue = self.rebalance_leaders(self.broker_distribution)
            self.state = OptimizedRebalanceChange._COMPUTE_REPLICAS
        elif self.state == OptimizedRebalanceChange._COMPUTE_REPLICAS:
            self._rebalance_replicas()
            self.state = OptimizedRebalanceChange._SORT_ACTIONS
        elif self.state == OptimizedRebalanceChange._SORT_ACTIONS:
            self.action_queue = self._sort_actions()
            self.state = OptimizedRebalanceChange._BALANCE
        elif self.state == OptimizedRebalanceChange._BALANCE:
            return not self._balance()
        return True

    def _balance(self):
        if not self.action_queue:
            return True
        key, replicas = self.action_queue.popitem()
        topic, partition = key
        if not self.zk.reallocate_partition(topic, partition, replicas):
            self.action_queue[key] = replicas
        return False

    def _rebalance_replicas(self):
        # Remove duplicates
        self._remove_replica_copies()
        if not self._rebalance_replicas_template(False):
            # It may happen, that data can not be copied, because there is leader of this topic already there.
            self._rebalance_replicas_template(True)
            if not self._rebalance_replicas_template(False):
                _LOG.error('Failed to rebalance replicas... Reason is unclear, will just stop the process')
                raise Exception('Failed to perform replica rebalance {}, {}, {}'.format(
                    self.broker_distribution, self.broker_ids, self.action_queue))

    def _sort_actions(self):
        result = {}
        for topic_partition, source_broker_id, target_broker_id in self.action_queue:
            # Here comes small trick. Each action includes only one replacement.
            if topic_partition not in result:
                result[topic_partition] = list(self.source_distribution[topic_partition])
            tmp_result = result[topic_partition]
            for i in range(len(tmp_result) - 1, -1, -1):
                if tmp_result[i] == source_broker_id:
                    tmp_result[i] = target_broker_id
                    break
        return result

    def _list_active_brokers_with_skip(self, skip_id: int):
        # Method was created only in optimization purposes
        return [b for b in self.broker_distribution.values() if
                b.broker_id != skip_id and b.broker_id in self.broker_ids]

    def _remove_replica_copies(self):
        for broker in self.broker_distribution.values():
            # List all brokers, without free_replica_slots optimization.
            for topic_partition in broker.list_replica_copies():
                success = self._move_partition_to_replica(
                    broker, topic_partition,
                    [b for b in self._list_active_brokers_with_skip(broker.broker_id) if b.has_free_replica_slots()])
                if not success:
                    self._move_partition_to_replica(
                        broker, topic_partition, self._list_active_brokers_with_skip(broker.broker_id))

    def _move_partition_to_replica(self, broker, topic_partition, broker_iterable):
        for target in broker_iterable:
            if target.accept_replica(broker, topic_partition):
                self.action_queue.append((topic_partition, broker.broker_id, target.broker_id))
                return True
        return False

    def _rebalance_replicas_template(self, force: bool):
        for broker in self.broker_distribution.values():
            to_move = broker.get_replica_overload()
            if to_move <= 0:
                continue
            targets = self._list_active_brokers_with_skip(broker.broker_id)
            if not force:
                targets = [t for t in targets if t.has_free_replica_slots()]
            for _ in range(0, to_move):
                copied = False
                for topic_partition in broker.list_replicas():
                    if self._move_partition_to_replica(broker, topic_partition, targets):
                        copied = True
                        break
                if not copied:
                    return False
                if not force:
                    targets = [t for t in targets if t.has_free_replica_slots()]
        return True

    @staticmethod
    def _build_leader_distribution_weights(brokers: dict):
        candidates = {}
        for source_broker_id, source_broker in brokers.items():
            if not source_broker.have_extra_leaders():
                continue
            for target_broker_id, target_broker in brokers.items():
                if not target_broker.have_less_leaders():
                    continue
                weight_list = []
                for topic in source_broker.topic_cardinality.keys():
                    delta = source_broker.topic_cardinality[topic] - target_broker.topic_cardinality.get(topic, 0)
                    weight_list.append((topic, delta))
                # Now the first element is the first to rebalance.
                if weight_list:
                    candidates[(source_broker_id, target_broker_id)] = sorted(
                        weight_list, key=lambda x: x[1], reverse=True)
        return candidates

    @staticmethod
    def _place_to_cardinalities(array: list, topic: str, cardinality_change: int):
        old_cardinality = None
        for item in [(a, b) for a, b in array if a == topic]:
            old_cardinality = item[1]
            array.remove(item)
        if old_cardinality is None:
            return
        new_cardinality = old_cardinality + cardinality_change
        idx = len(array)
        for i in range(idx - 1, -1, -1):
            if array[i][1] >= new_cardinality:
                idx = i + 1
                break
        array.insert(idx, (topic, new_cardinality))

    @staticmethod
    def rebalance_leaders(brokers: dict) -> list:
        candidates = OptimizedRebalanceChange._build_leader_distribution_weights(brokers)
        queue_list = []
        while any([broker.have_extra_leaders() for broker in brokers.values()]):
            top_candidate = None
            for broker_pair, weight_list in candidates.items():
                if not top_candidate:
                    top_candidate = broker_pair
                else:
                    if weight_list[0][1] > candidates[top_candidate][0][1]:
                        top_candidate = broker_pair
            # Taking best topic to move
            topic, _ = candidates[top_candidate][0]
            # Updating cardinality map
            for broker_pair, weight_list in candidates.items():
                if top_candidate == broker_pair:
                    OptimizedRebalanceChange._place_to_cardinalities(weight_list, topic, -2)
                elif top_candidate[0] == broker_pair[0] or top_candidate[1] == broker_pair[1]:  # src or dst matches
                    OptimizedRebalanceChange._place_to_cardinalities(weight_list, topic, -1)
            # Get broker objects
            source_broker, target_broker = brokers[top_candidate[0]], brokers[top_candidate[1]]

            # Selecting best partition to move (It is better to just swap leadership, instead of copying data)
            selected_partition = None
            source_partitions = source_broker.list_partitions(topic, False)
            target_partitions = target_broker.list_partitions(topic, True)
            for partition in source_partitions:
                if partition in target_partitions:
                    selected_partition = partition
                    break
            # Failed to find optimized partition for swapping, taking any.
            selected_partition = source_partitions[0] if selected_partition is None else selected_partition
            topic_partition = (topic, selected_partition)
            # Update brokers state
            target_broker.accept_leader(source_broker, topic_partition)
            # add transfer to queue
            queue_list.append((topic_partition, source_broker.broker_id, target_broker.broker_id))
            # Remove empty lists
            for bp in [bp for bp, wl in candidates.items() if
                       not brokers[bp[0]].have_extra_leaders() or not brokers[bp[1]].have_less_leaders()]:
                del candidates[bp]
        return queue_list

    def _load_data(self):
        self.broker_distribution = {id_: BrokerDescription(id_) for id_ in self.broker_ids}
        self.source_distribution = {(topic, partition): replicas for topic, partition, replicas in
                                    self.zk.load_partition_assignment()}
        for topic_partition, replicas in self.source_distribution.items():
            if not replicas:
                continue
            first = True
            for replica in replicas:
                if replica not in self.broker_distribution:
                    self.broker_distribution[replica] = BrokerDescription(replica)
                self.broker_distribution[replica].add_partition(first, topic_partition)
                first = False
        active_brokers = [broker for id_, broker in self.broker_distribution.items() if id_ in self.broker_ids]
        total_leaders = sum(b.get_leader_count() for b in self.broker_distribution.values())

        new_leader_count = distribute(total_leaders, active_brokers, BrokerDescription.get_leader_count)
        for i in range(0, len(active_brokers)):
            active_brokers[i].set_leader_expectation(new_leader_count[i])

        # Here is one trick. Across all the code replica list does not contain leaders.
        # But in order to balance correctly (max(diff(replica+leaders) <= 1) we should add leaders and substract it
        # again
        total_replicas = sum(b.get_replica_count() for b in self.broker_distribution.values()) + sum(new_leader_count)
        new_replica_count = distribute(total_replicas, active_brokers, BrokerDescription.get_replica_count)
        for i in range(0, len(active_brokers)):
            active_brokers[i].set_replica_expectation(new_replica_count[i] - new_leader_count[i])
