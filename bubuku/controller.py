import logging
from time import sleep

from kazoo.exceptions import NodeExistsError

from bubuku.amazon import Amazon
from bubuku.broker import BrokerManager
from bubuku.zookeeper import Exhibitor

_LOG = logging.getLogger('bubuku.controller')


class Change(object):
    def get_name(self) -> str:
        raise NotImplementedError('Not implemented yet')

    def can_run(self, current_actions) -> bool:
        raise NotImplementedError('Not implemented yet')

    def run(self, current_actions) -> bool:
        raise NotImplementedError('Not implemented')

    def can_run_at_exit(self) -> bool:
        return False


class Check(object):
    def check(self) -> Change:
        raise NotImplementedError('Not implemented')


def _exclude_self(ip, name, running_actions):
    return [k for k, v in running_actions.items() if k != name and v != ip]


class Controller(object):
    def __init__(self, broker_manager: BrokerManager, zk: Exhibitor, amazon: Amazon):
        self.broker_manager = broker_manager
        self.zk = zk
        self.amazon = amazon
        self.checks = []
        self.changes = []
        self.running = True

    def add_check(self, check):
        _LOG.info('Adding check {}'.format(str(check)))
        self.checks.append(check)

    def loop(self):
        ip = self.amazon.get_own_ip()

        try:
            self.zk.create('/bubuku/changes', makepath=True)
        except NodeExistsError:
            pass
        while self.running or self.changes:
            _LOG.debug('Taking lock for processing')
            with self.zk.take_lock('/bubuku/global_lock', ip):
                _LOG.debug('Lock is taken')
                # Get list of current running changes
                running_changes = {
                    change: self.zk.get('/bubuku/changes/{}'.format(change))[0].decode('utf-8')
                    for change in self.zk.get_children('/bubuku/changes')
                    }
                if running_changes:
                    _LOG.info("Running changes: {}".format(running_changes))
                # Register changes to run
                for change in self.changes:
                    name = change.get_name()
                    if change.can_run(_exclude_self(ip, name, running_changes)):
                        if name not in running_changes:
                            _LOG.info('Registering change in zk: {}'.format(name))
                            self.zk.create('/bubuku/changes/{}'.format(name), ip.encode('utf-8'), ephemeral=True)
                            running_changes[name] = ip
                    else:
                        _LOG.info('Change {} is waiting for others: {}'.format(change.get_name(), running_changes))
            # apply changes without holding lock
            actions_to_remove = []
            for change in self.changes:
                if change.get_name() in running_changes and running_changes[change.get_name()] == ip:
                    _LOG.info('Executing action {} step'.format(change.get_name()))
                    if self.running or change.can_run_at_exit():
                        if not change.run(_exclude_self(ip, change.get_name(), running_changes)):
                            _LOG.info('Action {} completed'.format(change.get_name()))
                            actions_to_remove.append(change.get_name())
                        else:
                            _LOG.info('Action {} will be executed on next loop step'.format(change.get_name()))
                    else:
                        _LOG.info(
                            'Action {} can not be run while stopping, forcing to stop it'.format(change.get_name()))
                        actions_to_remove.append(change.get_name())

            # remove processed actions
            if actions_to_remove:
                self.changes = [ch for ch in self.changes if ch.get_name() not in actions_to_remove]
                with self.zk.take_lock('/bubuku/global_lock'):
                    for name in actions_to_remove:
                        _LOG.info('Removing action {} from locks'.format(name))
                        self.zk.delete('/bubuku/changes/{}'.format(name), recursive=True)

            if self.running:
                for check in self.checks:
                    _LOG.info('Executing check {}'.format(check))
                    change = check.check()
                    if change:
                        _LOG.info('Adding change {} to pending changes'.format(change.get_name()))
                        self.changes.append(change)

            if self.changes:
                sleep(0.5)
            else:
                sleep(5)

    def stop(self, change: Change):
        _LOG.info('Stopping controller with additional change: {}'.format(change.get_name() if change else None))
        # clear all pending changes
        if change:
            self.changes.append(change)
        self.running = False
