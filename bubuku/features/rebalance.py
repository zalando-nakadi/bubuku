import json
import logging

from kazoo.exceptions import NodeExistsError, NoNodeError

from bubuku.broker import BrokerManager
from bubuku.controller import Check, Change
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.features.rebalance')


def _optimise_broker_ids(ids: list) -> str:
    if len(ids) > 1:
        ids[1:] = sorted(ids[1:])
    return ','.join(ids)


def combine_broker_ids(ids, length) -> list:
    result = []

    def _combine(start, left, length_):
        if not left or length_ == 0:
            result.append(start)
            return
        for i in range(0, len(left)):
            left_copy = list(left)
            del left_copy[i]
            next_copy = list(start)
            next_copy.append(left[i])
            _combine(next_copy, left_copy, length_ - 1)

    _combine([], ids, length)

    return sorted(set([_optimise_broker_ids(x) for x in result]))


def pop_with_length(arrays, length):
    for d in arrays:
        if len(d) == length:
            def _removal_func():
                del d[0]

            return d[0], _removal_func
    return None, None


class RebalanceChange(Change):
    def __init__(self, zk: Exhibitor, broker_list):
        self.zk = zk
        self.broker_ids = broker_list
        self.stale_data = {}  # partition count to topic data
        self.shuffled_broker_ids = None

    def __str__(self):
        return 'Rebalance({}), stale_count: {}'.format(
            self.get_name(),
            sum([len(v) for v in self.stale_data.values()]))

    def get_name(self) -> str:
        return 'rebalance'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'restart', 'rebalance', 'stop']])

    def load_current_data(self):
        result = []
        for topic in self.zk.get_children('/brokers/topics'):
            data = json.loads(self.zk.get("/brokers/topics/" + topic)[0].decode('utf-8'))
            for k, v in data['partitions'].items():
                result.append({"topic": topic, "partition": int(k), "replicas": v})
        return result

    def take_next(self) -> dict:
        if self.stale_data:
            k = [x for x in self.stale_data.keys()][0]
            v = self.stale_data[k]

            def _removal_func():
                del v[0]
                if not v:
                    del self.stale_data[k]

            return v[0], _removal_func

        if self.shuffled_broker_ids:
            for repl_factor, data in self.shuffled_broker_ids.items():
                min_length = min([len(v) for v in data.values()])
                max_length = max([len(v) for v in data.values()])
                if (max_length - min_length) < 2:
                    continue
                return pop_with_length(data.values(), max_length)
        return None, None

    def run(self, current_actions):
        # Stop rebalance if someone is restarting
        if any([a in current_actions for a in ['restart', 'start', 'stop']]):
            _LOG.warning("Rebalance stopped, because other blocking events running: {}".format(current_actions))
            return False

        try:
            rebalance_data = self.zk.get('/admin/reassign_partitions')[0].decode('utf-8')
            _LOG.info('Old rebalance is still in progress: {}, waiting'.format(rebalance_data))
            return True
        except NoNodeError:
            pass

        new_broker_ids = sorted(self.zk.get_children('/brokers/ids'))

        if new_broker_ids != self.broker_ids:
            _LOG.warning("Rebalance stopped because of broker list change from {} to {}".format(self.broker_ids,
                                                                                                new_broker_ids))
            return False

        # Next actions are split into steps, because they are relatively long-running

        # Load existing data from zookeeper and try to split it for different purposes
        if self.shuffled_broker_ids is None:
            self.shuffled_broker_ids = {}
            for d in self.load_current_data():
                replication_factor = len(d['replicas'])
                if replication_factor > len(self.broker_ids):
                    _LOG.warning(
                        "Will not rebalance partition {} because only {} brokers available".format(d, self.broker_ids))
                    continue
                if replication_factor not in self.shuffled_broker_ids:
                    self.shuffled_broker_ids[replication_factor] = {
                        k: [] for k in combine_broker_ids(self.broker_ids, replication_factor)
                        }
                name = _optimise_broker_ids([str(i) for i in d['replicas']])
                if name not in self.shuffled_broker_ids[replication_factor]:
                    if name not in self.stale_data:
                        self.stale_data[name] = []
                    self.stale_data[name].append(d)
                else:
                    self.shuffled_broker_ids[replication_factor][name].append(d)
            _LOG.info("Shuffled broker ids are: {}".format(json.dumps(self.shuffled_broker_ids, indent=2)))
            return True

        to_move, removal_func = self.take_next()
        while to_move:
            replication_factor = len(to_move['replicas'])
            if replication_factor not in self.shuffled_broker_ids:
                removal_func()
                _LOG.error("Well, it's a BUG! Replication factor {} is not found among {}".format(
                    replication_factor,
                    self.shuffled_broker_ids.keys()))
            else:
                min_length = min([len(v) for v in self.shuffled_broker_ids[replication_factor].values()])
                for k, v in self.shuffled_broker_ids[replication_factor].items():
                    if len(v) == min_length:
                        j = {
                            "version": "1",
                            "partitions": [
                                {
                                    "topic": to_move['topic'],
                                    "partition": to_move['partition'],
                                    "replicas": [int(v) for v in k.split(',')],
                                }
                            ]
                        }
                        if self.reallocate(j):
                            _LOG.info("Current allocation: \n{}".format(self.dump_allocations()))
                            removal_func()
                            v.append(to_move)
                        return True
            to_move, removal_func = self.take_next()

        _LOG.info("Current allocation: \n{}".format(self.dump_allocations()))
        return False

    def reallocate(self, j: dict):
        try:
            data = json.dumps(j)
            self.zk.create("/admin/reassign_partitions", data.encode('utf-8'))
            _LOG.info("Reallocating {}".format(data))
            return True
        except NodeExistsError:
            _LOG.info("Waiting for free reallocation slot, still in progress...")
            return False

    def dump_allocations(self):
        return '\n'.join(
            ['\n'.join(['{}:{}'.format(x, len(y)) for x, y in v.items()]) for v in self.shuffled_broker_ids.values()])


class RebalanceOnStartCheck(Check):
    def __init__(self, zk, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.executed = False

    def check(self):
        if self.executed:
            return None
        if not self.broker.is_running_and_registered():
            return None
        _LOG.info("Rebalance on start, triggering rebalance")
        self.executed = True
        return RebalanceChange(self.zk, sorted(self.zk.get_children('/brokers/ids')))

    def __str__(self):
        return 'RebalanceOnStartCheck (executed={})'.format(self.executed)


class RebalanceOnBrokerListChange(Check):
    def __init__(self, zk, broker: BrokerManager):
        super().__init__()
        self.zk = zk
        self.broker = broker
        self.old_broker_list = []

    def check(self):
        if not self.broker.is_running_and_registered():
            return None
        new_list = sorted(self.zk.get_children('/brokers/ids'))
        if not new_list == self.old_broker_list:
            _LOG.info('Broker list changed from {} to {}, triggering rebalance'.format(self.old_broker_list, new_list))
            self.old_broker_list = new_list
            return RebalanceChange(self.zk, new_list)
        return None

    def __str__(self):
        return 'RebalanceOnBrokerListChange, cached list: {}'.format(self.old_broker_list)
