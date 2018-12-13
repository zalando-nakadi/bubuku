import logging
from time import time

import requests

from bubuku.api import ApiConfig
from bubuku.aws import AWSResources, node, volume
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ec2_node import EC2
from bubuku.controller import Change
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rolling_restart')


class RollingRestartChange(Change):
    def __init__(self, zk: BukuExhibitor, cluster_config: ClusterConfig,
                 broker_id_to_restart: str,
                 image: str,
                 instance_type: str,
                 scalyr_key: str,
                 scalyr_region: str,
                 kms_key_id: str):
        self.zk = zk
        self.cluster_config = cluster_config
        self.cluster_config.set_application_version(image)
        self.cluster_config.set_instance_type(instance_type)
        self.cluster_config.set_scalyr_account_key(scalyr_key)
        self.cluster_config.set_scalyr_region(scalyr_region)
        self.cluster_config.set_kms_key_id(kms_key_id)
        self.state_context = StateContext(self.zk, self.cluster_config, broker_id_to_restart)

    def get_name(self) -> str:
        return 'rolling_restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['start', 'stop', 'restart', 'rebalance']])

    def run(self, current_actions) -> bool:
        if not self._is_cluster_in_good_state():
            _LOG.error('Cluster is not stable skipping restart iteration')
            return True

        return self.state_context.run()

    def _is_cluster_in_good_state(self):
        return True


class StateContext:
    def __init__(self, zk: BukuExhibitor, cluster_config, broker_id_to_restart):
        self.zk = zk
        self.broker_id_to_restart = broker_id_to_restart
        self.broker_ip_to_restart = self.zk.get_broker_address(broker_id_to_restart)
        self.cluster_config = cluster_config
        self.aws = AWSResources(region=self.cluster_config.get_aws_region())
        self.current_state = StopKafka(self)
        self.instance = None
        self.volume = None

    def run(self):
        """
        Runs states one after another. If state is finished, it takes the next one.
        """
        try:
            _LOG.info('Running state {}'.format(self.current_state))
            if self.current_state.run():
                next_state = self.current_state.next()
                _LOG.info('Next state {}'.format(next_state))
                if next_state is None:
                    return False
                self.current_state = next_state
            return True
        except Exception as e:
            _LOG.error('Failed to run state', exc_info=e)
            return True


class State:
    """
    State which can be run as many times as required before it finishes it work. The progress of the state has to be
    recoverable
    """

    def __init__(self, state_context):
        self.state_context = state_context
        self.time_to_check_s = time()

    def run(self) -> bool:
        """
        Runs the state, and if state finishes successfully it returns True, otherwise it returns False, which means
        that state has to be executed again
        """
        pass

    def next(self):
        """
        Return the next state, which has to be executed after the current state
        """
        pass

    def run_with_timeout(self, func, timeout_s=10):
        """
        Runs func() with timeout
        :param func function to execute
        :param timeout_s timeout before executing state next time
        """
        if time() >= self.time_to_check_s:
            self.time_to_check_s = time() + timeout_s
            return func()
        return False


class StopKafka(State):
    def run(self):
        _LOG.info('Stopping broker {} {}'.format(self.state_context.broker_id_to_restart,
                                                 self.state_context.broker_ip_to_restart))
        resp = requests.post(ApiConfig.get_url(self.state_context.broker_ip_to_restart, 'stop'))
        if resp.status_code != 200:
            _LOG.error('Failed to stop Kafka: {} {}', resp.status_code, resp.text)
            return False
        return True

    def next(self):
        return WaitBrokerStopped(self.state_context)


class WaitBrokerStopped(State):
    def run(self):
        def func():
            resp = requests.get(ApiConfig.get_url(self.state_context.broker_ip_to_restart, 'state')).json()
            return resp.get('state') == 'stopped'

        return self.run_with_timeout(func)

    def next(self):
        return DetachVolume(self.state_context)


class DetachVolume(State):
    def run(self):
        self.state_context.instance = node.get_instance_by_ip(self.state_context.aws.ec2_resource,
                                                              self.state_context.cluster_config,
                                                              self.state_context.broker_ip_to_restart)

        vol = volume.detach_volume(self.state_context.aws, self.state_context.instance)
        self.state_context.cluster_config.set_availability_zone(vol.availability_zone)
        self.state_context.volume = vol
        return True

    def next(self):
        return TerminateInstance(self.state_context)


class TerminateInstance(State):
    def run(self):
        def func():
            node.terminate(self.state_context.aws, self.state_context.cluster_config, self.state_context.instance)
            return True

        return self.run_with_timeout(func)

    def next(self):
        return WaitVolumeAvailable(self.state_context)


class WaitVolumeAvailable(State):
    def run(self):
        def func():
            return volume.is_volume_available(self.state_context.volume)

        return self.run_with_timeout(func)

    def next(self):
        return LaunchInstance(self.state_context)


class LaunchInstance(State):
    def run(self):
        ec2 = EC2(self.state_context.aws)
        ec2.create(self.state_context.cluster_config, 1)
        return True

    def next(self):
        return WaitVolumeAttached(self.state_context)


class WaitVolumeAttached(State):
    def run(self):
        def func():
            return volume.clear_volume_tag_if_in_use(self.state_context.volume)

        return self.run_with_timeout(func)

    def next(self):
        return WaitKafkaRunning(self.state_context)


class WaitKafkaRunning(State):
    def run(self):
        def func():
            return self.state_context.zk.is_broker_registered(self.state_context.broker_id_to_restart)

        return self.run_with_timeout(func)

    def next(self):
        return None
