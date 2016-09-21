import logging
from time import time

from bubuku.broker import BrokerManager
from bubuku.communicate import sleep_and_operate
from bubuku.env_provider import EnvProvider
from bubuku.zookeeper import BukuExhibitor

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

    def on_remove(self):
        pass


class Check(object):
    def __init__(self, check_interval_s=5):
        self.check_interval_s = check_interval_s
        self.__last_check_timestamp_s = 0

    def check_if_time(self) -> Change:
        if self.time_till_check() <= 0:
            self.__last_check_timestamp_s = time()
            _LOG.info('Executing check {}'.format(self))
            return self.check()
        return None

    def time_till_check(self):
        return self.__last_check_timestamp_s + self.check_interval_s - time()

    def check(self) -> Change:
        raise NotImplementedError('Not implemented')


def _exclude_self(provider_id, name, running_actions):
    return [k for k, v in running_actions.items() if k != name or v != provider_id]


class Controller(object):
    def __init__(self, broker_manager: BrokerManager, zk: BukuExhibitor, env_provider: EnvProvider):
        self.broker_manager = broker_manager
        self.zk = zk
        self.env_provider = env_provider
        self.checks = []
        self.changes = {}  # Holds mapping from change name to array of pending changes
        self.running = True
        self.provider_id = None  # provider id must not be requested on initialization

    def enumerate_changes(self):
        with self.zk.lock(self.provider_id):
            running_changes = self.zk.get_running_changes()

        result = []
        for name, change_list in self.changes.items():
            running = running_changes.get(name) == self.provider_id
            first = True
            for change in change_list:
                result.append({
                    'type': name,
                    'description': str(change),
                    'running': bool(first and running)
                })
                first = False
        return result

    def cancel_changes(self, name):
        result = len(self.changes.get(name, {}))
        if result:
            with self.zk.lock(self.provider_id):
                self.zk.unregister_change(name)

            del self.changes[name]
        return result

    def add_check(self, check):
        _LOG.info('Adding check {}'.format(str(check)))
        self.checks.append(check)

    def _register_running_changes(self) -> dict:
        if not self.changes:
            return {}  # Do not take lock if there are no changes to register
        _LOG.debug('Taking lock for processing')
        with self.zk.lock(self.provider_id):
            _LOG.debug('Lock is taken')
            # Get list of current running changes
            running_changes = self.zk.get_running_changes()
            if running_changes:
                _LOG.info("Running changes: {}".format(running_changes))
            # Register changes to run
            for name, change_list in self.changes.items():
                # Only first change is able to run
                first_change = change_list[0]
                if first_change.can_run(_exclude_self(self.provider_id, name, running_changes)):
                    if name not in running_changes:
                        self.zk.register_change(name, self.provider_id)
                        running_changes[name] = self.provider_id
                else:
                    _LOG.info('Change {} is waiting for others: {}'.format(name, running_changes))
            return running_changes

    def _run_changes(self, running_changes: dict) -> list:
        changes_to_remove = []
        for name, change_list in self.changes.items():
            if name in running_changes and running_changes[name] == self.provider_id:
                change = change_list[0]
                _LOG.info('Executing action {} step'.format(change))
                if self.running or change.can_run_at_exit():
                    try:
                        if not change.run(_exclude_self(self.provider_id, change.get_name(), running_changes)):
                            _LOG.info('Action {} completed'.format(change))
                            changes_to_remove.append(change.get_name())
                        else:
                            _LOG.info('Action {} will be executed on next loop step'.format(change))
                    except Exception as e:
                        _LOG.error('Failed to execute change {} because of exception, removing'.format(change),
                                   exc_info=e)
                        changes_to_remove.append(change.get_name())
                else:
                    _LOG.info(
                        'Action {} can not be run while stopping, forcing to stop it'.format(change))
                    changes_to_remove.append(change.get_name())
        return changes_to_remove

    def _release_changes_lock(self, changes_to_remove):
        if changes_to_remove:
            for change_name in changes_to_remove:
                removed_change = self.changes[change_name][0]
                del self.changes[change_name][0]
                if not self.changes[change_name]:
                    del self.changes[change_name]
                removed_change.on_remove()
            with self.zk.lock():
                for name in changes_to_remove:
                    self.zk.unregister_change(name)

    def loop(self, change_on_init=None):
        self.provider_id = self.env_provider.get_id()
        if change_on_init:
            self._add_change_to_queue(change_on_init)
        while self.running or self.changes:
            self.make_step()

            if self.changes:
                timeout = 0.5
            else:
                timeout = min([check.time_till_check() for check in self.checks])
            sleep_and_operate(self, timeout)

    def make_step(self):
        # register running changes
        running_changes = self._register_running_changes()
        # apply changes without holding lock
        changes_to_remove = self._run_changes(running_changes)
        # remove processed actions
        self._release_changes_lock(changes_to_remove)
        if self.running:
            for check in self.checks:
                self._add_change_to_queue(check.check_if_time())

    def _add_change_to_queue(self, change):
        if not change:
            return
        _LOG.info('Adding change {} to pending changes'.format(change.get_name()))
        if change.get_name() not in self.changes:
            self.changes[change.get_name()] = []
        self.changes[change.get_name()].append(change)

    def stop(self, change: Change):
        _LOG.info('Stopping controller with additional change: {}'.format(change.get_name() if change else None))
        # clear all pending changes
        self._add_change_to_queue(change)
        self.running = False
