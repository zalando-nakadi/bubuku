import logging
from time import time

from bubuku.aws import AWSResources, node, volume
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ec2_node import EC2
from bubuku.broker import BrokerManager
from bubuku.controller import Change
from bubuku.zookeeper import BukuExhibitor

_LOG = logging.getLogger('bubuku.features.rolling_restart')


class RollingRestartChange(Change):
    def __init__(self, zk: BukuExhibitor, cluster_config: ClusterConfig,
                 restart_assignment,
                 broker_id: str,
                 image: str,
                 instance_type: str,
                 scalyr_key: str,
                 scalyr_region: str,
                 kms_key_id: str):
        self.zk = zk
        self.restart_assignment = restart_assignment
        self.cluster_config = cluster_config
        self.cluster_config.set_application_version(image)
        self.cluster_config.set_instance_type(instance_type)
        self.cluster_config.set_scalyr_account_key(scalyr_key)
        self.cluster_config.set_scalyr_region(scalyr_region)
        self.cluster_config.set_kms_key_id(kms_key_id)
        self.state_context = StateContext(self.zk, self.cluster_config, broker_id, restart_assignment)

    def get_name(self) -> str:
        return 'rolling_restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['stop', 'restart', 'rebalance']])

    def run(self, current_actions) -> bool:
        if not self._is_cluster_in_good_state():
            _LOG.error('Cluster is not stable skipping restart iteration')
            return True

        return self.state_context.run()

    def _is_cluster_in_good_state(self):
        return True


class StartBrokerChange(Change):
    def __init__(self, zk: BukuExhibitor, broker: BrokerManager):
        self.zk = zk
        self.broker = broker

    def get_name(self):
        return 'start'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['restart', 'stop']])

    def run(self, current_actions):
        zk_conn_str = self.zk.get_conn_str()
        try:
            self.broker.start_kafka_process(zk_conn_str)
        except Exception as e:
            _LOG.error('Failed to start kafka process against {}'.format(zk_conn_str), exc_info=e)
            return True
        return False


class StateContext:
    def __init__(self, zk: BukuExhibitor, cluster_config, broker_id, restart_assignment):
        self.zk = zk
        self.broker_id = broker_id
        self.restart_assignment = restart_assignment
        self.broker_ip_to_restart = self.zk.get_broker_address(self.restart_assignment.pop(broker_id))
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

    def run_with_timeout(self, func):
        """
        Runs func() with timeout
        :param func function to execute
        :param timeout_s timeout before executing state next time
        """
        if time() >= self.time_to_check_s:
            self.time_to_check_s = time() + 10
            return func()
        return False


class StopKafka(State):
    def run(self):
        from bubuku.features.remote_exec import RemoteCommandExecutorCheck
        RemoteCommandExecutorCheck.register_stop(self.state_context.zk, self.state_context.broker_id_to_restart)
        return True

    def next(self):
        return WaitBrokerStopped(self.state_context)


class WaitBrokerStopped(State):
    def run(self):
        def func():
            return not self.state_context.zk.is_broker_registered(self.state_context.broker_id_to_restart)

        return self.run_with_timeout(func)

    def next(self):
        return DetachVolume(self.state_context)


class DetachVolume(State):
    def run(self):
        self.state_context.instance = node.get_instance_by_ip(
            self.state_context.aws.ec2_resource,
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
        return StartKafka(self.state_context)


class StartKafka(State):
    def run(self):
        def func():
            from bubuku.features.remote_exec import RemoteCommandExecutorCheck
            RemoteCommandExecutorCheck.register_start(self.state_context.zk, self.state_context.broker_id_to_restart)
            return True

        return self.run_with_timeout(func)

    def next(self):
        return WaitKafkaRunning(self.state_context)


class WaitKafkaRunning(State):
    def run(self):
        def func():
            return self.state_context.zk.is_broker_registered(self.state_context.broker_id_to_restart)

        return self.run_with_timeout(func)

    def next(self):
        return RegisterRollingRestart(self.state_context)


class RegisterRollingRestart(State):
    def run(self):
        if len(self.restart_assignment) == 0:
            _LOG.info('Rolling restart is successfully finished')
        else:
            action = {'name': 'rolling_restart',
                      'restart_assignment': self.restart_assignment,
                      'image': self.state_context.cluster_config.get_application_version(),
                      'instance_type': self.state_context.cluster_config.get_instance_type(),
                      'scalyr_key': self.state_context.cluster_config.get_scalyr_region(),
                      'scalyr_region': self.state_context.cluster_config.get_scalyr_account_key(),
                      'kms_key_id': self.state_context.cluster_config.get_kms_key_id()}
            self.zk.register_action(action, broker_id=self.state_context.broker_id)
        return True

    def next(self):
        return None
