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


class DistributionMap(object):
    """
    Topic distribution map. Used to correctly balance leadership across brokers. Internal collection of candidates is a
    dict with reflection of (source_broker, target_broker) -> sorted_list_of_topics_and weights.
    """
    __slots__ = [
        '_candidates',
        '_candidates_cardinality'
    ]

    def __init__(self, brokers: iter):
        self._candidates = {}
        self._candidates_cardinality = {broker: broker.calculate_topic_cardinality() for broker in brokers}
        for source_broker in brokers:
            if not source_broker.have_extra_leaders():
                continue
            for target_broker in brokers:
                if not target_broker.have_less_leaders():
                    continue
                weight_list = []
                for topic in self._candidates_cardinality[source_broker].keys():
                    delta = self._candidates_cardinality[source_broker][topic] - \
                            self._candidates_cardinality[target_broker].get(topic, 0)
                    weight_list.append((topic, delta))
                # Now the first element is the first to rebalance.
                if weight_list:
                    self._candidates[(source_broker, target_broker)] = sorted(
                        weight_list, key=lambda x: x[1], reverse=True)

    def take_move_pair(self) -> tuple:
        """
        Selects best source_broker, target_broker, topic to move leadership. Internal data is updated as if change was
        already performed
        :return: tuple source_broker, target_broker, topic_name
        """
        top_candidate = None
        for broker_pair, weight_list in self._candidates.items():
            if not top_candidate:
                top_candidate = broker_pair
            else:
                if weight_list[0][1] > self._candidates[top_candidate][0][1]:
                    top_candidate = broker_pair
                    # Taking best topic to move
        topic, _ = self._candidates[top_candidate][0]
        self._candidates_cardinality[top_candidate[0]][topic] -= 1
        topic_exhausted = self._candidates_cardinality[top_candidate[0]][topic] == 0
        if topic_exhausted:
            for broker_pair, weight_list in self._candidates.items():
                if top_candidate == broker_pair:
                    DistributionMap._rearrange_topic_weights(weight_list, topic, None)
                elif top_candidate[0] == broker_pair[0]:
                    DistributionMap._rearrange_topic_weights(weight_list, topic, None)
                elif top_candidate[1] == broker_pair[1]:
                    DistributionMap._rearrange_topic_weights(weight_list, topic, -1)
        else:
            # Updating cardinality map
            for broker_pair, weight_list in self._candidates.items():
                if top_candidate == broker_pair:
                    DistributionMap._rearrange_topic_weights(weight_list, topic, -2)
                elif top_candidate[0] == broker_pair[0] or top_candidate[1] == broker_pair[1]:  # src or dst matches
                    DistributionMap._rearrange_topic_weights(weight_list, topic, -1)
        # Get broker objects
        return top_candidate[0], top_candidate[1], topic

    @staticmethod
    def _rearrange_topic_weights(array: list, topic: str, cardinality_change):
        old_cardinality = None
        for item in array:
            if item[0] == topic:
                old_cardinality = item[1]
                array.remove(item)
                break
        if old_cardinality is None:
            return
        if cardinality_change is None:
            return
        new_cardinality = old_cardinality + cardinality_change
        idx = 0
        for i in range(len(array) - 1, -1, -1):
            if array[i][1] >= new_cardinality:
                idx = i + 1
                break
        array.insert(idx, (topic, new_cardinality))

    def cleanup(self):
        for bp in [bp for bp, wl in self._candidates.items()
                   if not bp[0].have_extra_leaders() or not bp[1].have_less_leaders()]:
            del self._candidates[bp]


class OptimizedRebalanceChange(BaseRebalanceChange):
    _LOAD_STATE = 'load_state'
    _COMPUTE_LEADERS = 'compute_leaders'
    _COMPUTE_REPLICAS = 'compute_replicas'
    _SORT_ACTIONS = 'sort_actions'
    _BALANCE = 'balance'

    def __init__(self, zk: BukuExhibitor, broker_ids: list, empty_brokers: list, exclude_topics: list,
                 parallelism: int = 1):
        self.zk = zk
        self.all_broker_ids = sorted(int(id_) for id_ in broker_ids)
        self.broker_ids = sorted(int(id_) for id_ in broker_ids if id_ not in empty_brokers)
        self.exclude_topics = exclude_topics
        self.broker_distribution = None
        self.source_distribution = None
        self.action_queue = []
        self.state = OptimizedRebalanceChange._LOAD_STATE
        self.parallelism = parallelism

    def __str__(self):
        return 'OptimizedRebalance state={}, queue_size={}, parallelism={}'.format(
            self.state, len(self.action_queue) if self.action_queue is not None else None, self.parallelism)

    def run(self, current_actions) -> bool:
        # Stop rebalance if someone is restarting
        if self.should_be_paused(current_actions):
            _LOG.warning("Rebalance paused, because other blocking events running: {}".format(current_actions))
            return True
        if self.zk.is_rebalancing():
            return True
        new_broker_ids = sorted(int(id_) for id_ in self.zk.get_broker_ids())
        if new_broker_ids != self.all_broker_ids:
            _LOG.warning("Rebalance stopped because of broker list change from {} to {}".format(
                self.all_broker_ids, new_broker_ids))
            return False
        if self.state == OptimizedRebalanceChange._LOAD_STATE:
            self._load_data()
            self.state = OptimizedRebalanceChange._COMPUTE_LEADERS
        elif self.state == OptimizedRebalanceChange._COMPUTE_LEADERS:
            self._rebalance_leaders()
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
        items = []
        while self.action_queue and len(items) < self.parallelism:
            items.append(self.action_queue.popitem())  # key, partition tuple
        if not items:
            return True
        data_to_rebalance = [(key[0], key[1], replicas) for key, replicas in items]
        if not self.zk.reallocate_partitions(data_to_rebalance):
            for key, replicas in items:
                self.action_queue[key] = replicas
        return False

    def _rebalance_replicas(self):
        """
        Balances replicas distribution.
        """
        # Remove duplicates
        self._remove_replica_copies()
        if not self._rebalance_replicas_template(False):
            # It may happen, that data can not be copied, because there is leader of this topic already there.
            self._rebalance_replicas_template(True)
            if not self._rebalance_replicas_template(False):
                _LOG.error('Failed to rebalance replicas. Probably because of replication factor problems. '
                           'Will just stop the process')
                raise Exception('Failed to perform replica rebalance {}, {}, {}'.format(
                    self.broker_distribution, self.broker_ids, self.action_queue))

    def _sort_actions(self):
        result = {}
        for topic_partition, source_broker_id, target_broker_id in self.action_queue:
            if topic_partition not in result:
                result[topic_partition] = list(self.source_distribution[topic_partition])
            tmp_result = result[topic_partition]
            for i in range(len(tmp_result) - 1, -1, -1):
                # Here comes small trick. Each action includes only one replacement. We are starting replacements from
                # back and perform them only once.
                if tmp_result[i] == source_broker_id:
                    tmp_result[i] = target_broker_id
                    break
        return result

    def _list_active_brokers_with_skip(self, skip_id: int) -> list:
        """
        Lists active brokers without broker with id `skip_id`
        :param skip_id: Broker id to skip
        :return:
        """
        return [b for b in self.broker_distribution.values() if
                b.broker_id != skip_id and b.broker_id in self.broker_ids]

    def _remove_replica_copies(self):
        """
        During leadership rebalance it may happen that leader for replica was placed to broker that is already have
        this partition. Before starting real rebalance it is needed to move such partitions to different brokers, even
        if it will lead to some additional steps in rebalance.
        """
        for broker in self.broker_distribution.values():
            for topic_partition in broker.list_replica_copies():
                targets = [b for b in self._list_active_brokers_with_skip(broker.broker_id)]
                moved_to = broker.move_replica(topic_partition, [t for t in targets if t.has_free_replica_slots()])
                if not moved_to:
                    moved_to = broker.move_replica(topic_partition, targets)
                if not moved_to:
                    raise Exception('Failed to move replica ' + str(topic_partition) + ', not enough replicas')
                self.action_queue.append((topic_partition, broker.broker_id, moved_to.broker_id))

    def _rebalance_replicas_template(self, force: bool):
        for broker in self.broker_distribution.values():
            to_move = broker.get_replica_overload()
            if to_move <= 0:
                continue
            targets = self._list_active_brokers_with_skip(broker.broker_id)
            if not force:
                targets = [t for t in targets if t.has_free_replica_slots()]
            for _ in range(0, to_move):
                target = False
                for topic_partition in broker.list_replicas():
                    target = broker.move_replica(topic_partition, targets)
                    if target:
                        self.action_queue.append((topic_partition, broker.broker_id, target.broker_id))
                        break
                if target is None:
                    return False
                if not force:
                    targets = [t for t in targets if t.has_free_replica_slots()]
        return True

    def _rebalance_leaders(self):
        """
        Balances leadership across active brokers.
        """
        candidates = DistributionMap(self.broker_distribution.values())
        while any([broker.have_extra_leaders() for broker in self.broker_distribution.values()]):
            # Get broker objects
            source_broker, target_broker, topic = candidates.take_move_pair()
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
            self.action_queue.append((topic_partition, source_broker.broker_id, target_broker.broker_id))
            # Remove empty lists
            candidates.cleanup()

    def _load_data(self):
        """
        Loads data from zk and prepares to perform rebalance. Fills in leader and replica count expectations.
        """
        self.broker_distribution = {id_: BrokerDescription(id_) for id_ in self.broker_ids}
        self.source_distribution = {(topic, partition): replicas for topic, partition, replicas in
                                    self.zk.load_partition_assignment() if topic not in self.exclude_topics}
        for topic_partition, replicas in self.source_distribution.items():
            if not replicas:
                continue
            leader = True
            for replica in replicas:
                if replica not in self.broker_distribution:
                    self.broker_distribution[replica] = BrokerDescription(replica)
                if leader:
                    self.broker_distribution[replica].add_leader(topic_partition)
                    leader = False
                else:
                    self.broker_distribution[replica].add_replica(topic_partition)
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
