import logging
from time import time

from bubuku import utils
from bubuku.aws import AWSResources
from bubuku.aws.cluster_config import ClusterConfig
from bubuku.aws.ec2_node_launcher import Ec2NodeLauncher
from bubuku.aws.node import Ec2Node
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
                 kms_key_id: str,
                 cool_down: int):
        self.zk = zk
        self.restart_assignment = restart_assignment
        self.broker_id = broker_id
        self.broker_id_to_restart = self.restart_assignment.pop(broker_id)
        self.broker_ip_to_restart = self.zk.get_broker_address(self.broker_id_to_restart)

        self.cluster_config = cluster_config
        self.cluster_config.set_application_version(image)
        self.cluster_config.set_instance_type(instance_type)
        self.cluster_config.set_scalyr_account_key(scalyr_key)
        self.cluster_config.set_scalyr_region(scalyr_region)
        self.cluster_config.set_kms_key_id(kms_key_id)

        self.aws = AWSResources(region=self.cluster_config.get_aws_region())
        self.ec_node = Ec2Node(self.aws, self.cluster_config, self.broker_ip_to_restart)
        self.ec2_node_launcher = Ec2NodeLauncher(self.aws, self.cluster_config)
        self.cluster_config.set_availability_zone(self.ec_node.get_node_availability_zone())

        self.state_context = StateContext(self.zk, self.aws, self.ec_node, self.ec2_node_launcher,
                                          self.broker_id_to_restart, self.restart_assignment,
                                          self.cluster_config, cool_down)

    def get_name(self) -> str:
        return 'rolling_restart'

    def can_run(self, current_actions):
        return all([a not in current_actions for a in ['stop', 'restart', 'rebalance']])

    def run(self, current_actions) -> bool:
        return self.state_context.run()


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
    def __init__(self, zk: BukuExhibitor, aws: AWSResources, ec_node: Ec2Node, ec2_node_launcher: Ec2NodeLauncher,
                 broker_id_to_restart, restart_assignment, cluster_config: ClusterConfig, cool_down: int):
        self.zk = zk
        self.restart_assignment = restart_assignment
        self.cluster_config = cluster_config
        self.aws = aws
        self.ec_node = ec_node
        self.ec2_node_launcher = ec2_node_launcher
        self.broker_id_to_restart = broker_id_to_restart
        self.current_state = StopKafka(self)
        self.new_instance_id = None
        self.cool_down = cool_down

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
        if utils.is_cluster_healthy():
            from bubuku.features.remote_exec import RemoteCommandExecutorCheck
            RemoteCommandExecutorCheck.register_stop(self.state_context.zk, self.state_context.broker_id_to_restart)
            return True
        _LOG.warning('Cluster is not healthy, waiting for it to recover')
        return False

    def next(self):
        return WaitBrokerStopped(self.state_context)

    def __str__(self):
        return 'StopKafka: stopping broker {}'.format(self.state_context.broker_id_to_restart)


class WaitBrokerStopped(State):
    def run(self):
        def func():
            return not self.state_context.zk.is_broker_registered(self.state_context.broker_id_to_restart)

        return self.run_with_timeout(func)

    def next(self):
        return DetachVolume(self.state_context)

    def __str__(self):
        return 'WaitBrokerStopped: waiting for broker {} to stop'.format(self.state_context.broker_id_to_restart)


class DetachVolume(State):
    def run(self):
        self.state_context.ec_node.detach_volume()
        return True

    def next(self):
        return TerminateInstance(self.state_context)

    def __str__(self):
        return 'DetachVolume: detaching volume {} from broker {}'.format(self.state_context.ec_node.get_volume_id(),
                                                                         self.state_context.broker_id_to_restart)


class TerminateInstance(State):
    def run(self):
        self.state_context.ec_node.terminate()
        return True

    def next(self):
        return WaitInstanceTerminated(self.state_context)

    def __str__(self):
        return 'TerminateInstance: terminating instance {}'.format(self.state_context.ec_node.get_ip())


class WaitInstanceTerminated(State):
    def run(self):
        def func():
            return self.state_context.ec_node.is_terminated()

        return self.run_with_timeout(func)

    def next(self):
        return WaitVolumeAvailable(self.state_context)

    def __str__(self):
        return 'WaitInstanceTerminated: waiting for instance {} to be terminated'.format(
            self.state_context.ec_node.get_ip())


class WaitVolumeAvailable(State):
    def run(self):
        def func():
            return self.state_context.ec_node.is_volume_available()

        return self.run_with_timeout(func)

    def next(self):
        return LaunchInstance(self.state_context)

    def __str__(self):
        return 'WaitVolumeAvailable: waiting for volume {} to be available'.format(
            self.state_context.ec_node.get_volume_id())


class LaunchInstance(State):
    def run(self):
        self.state_context.new_instance_id = self.state_context.ec2_node_launcher.launch()
        return True

    def next(self):
        return WaitVolumeAttached(self.state_context)


class WaitVolumeAttached(State):
    def run(self):
        def func():
            if self.state_context.ec_node.is_volume_in_use():
                self.state_context.ec2_node_launcher.create_auto_recovery_alarm(self.state_context.new_instance_id)
                return True
            return False

        return self.run_with_timeout(func)

    def next(self):
        return WaitKafkaRunning(self.state_context)

    def __str__(self):
        return 'WaitVolumeAttached: waiting for volume {} to be attached'.format(
            self.state_context.ec_node.get_volume_id())


class WaitKafkaRunning(State):
    def run(self):
        def func():
            return self.state_context.zk.is_broker_registered(self.state_context.broker_id_to_restart)

        return self.run_with_timeout(func)

    def next(self):
        return RegisterRollingRestart(self.state_context)

    def __str__(self):
        return 'WaitKafkaRunning: waiting broker {} is running'.format(self.state_context.broker_id_to_restart)


class RegisterRollingRestart(State):
    def __init__(self, state_context):
        super(RegisterRollingRestart, self).__init__(state_context)
        self.cluster_is_healthy_from = 0

    def run(self):
        if len(self.state_context.restart_assignment) == 0:
            _LOG.info('Rolling restart is successfully finished')
            return True
        else:
            if utils.is_cluster_healthy():
                if self.cluster_is_healthy_from == 0:
                    self.cluster_is_healthy_from = time()
            else:
                _LOG.warning('Cluster is not healthy, waiting for it to recover')
                self.cluster_is_healthy_from = 0
                return False

            if time() - self.cluster_is_healthy_from >= self.state_context.cool_down:
                action = {'name': 'rolling_restart',
                          'restart_assignment': self.state_context.restart_assignment,
                          'image': self.state_context.cluster_config.get_application_version(),
                          'instance_type': self.state_context.cluster_config.get_instance_type(),
                          'scalyr_key': self.state_context.cluster_config.get_scalyr_account_key(),
                          'scalyr_region': self.state_context.cluster_config.get_scalyr_region(),
                          'kms_key_id': self.state_context.cluster_config.get_kms_key_id(),
                          'cool_down': self.state_context.cool_down}
                next_broker_id = self.state_context.broker_id_to_restart
                self.state_context.zk.register_action(action, broker_id=next_broker_id)
                return True
            return False

    def next(self):
        return None
