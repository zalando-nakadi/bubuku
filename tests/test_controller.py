from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError, NodeExistsError

from bubuku.controller import Controller, Check, Change


def test_multiple_changes_are_executed_one_by_one():
    running_count = [3, 3, 3]

    class FakeChange(Change):
        def __init__(self, index):
            self.index = index

        def get_name(self):
            return 'fake'

        def can_run(self, current_actions):
            return True

        def run(self, current_actions):
            running_count[self.index] -= 1
            return running_count[self.index] > 0

    class FakeCheck(Check):
        def __init__(self):
            super().__init__(0)
            self.changes_limit = 3
            self.changes_issued = 0

        def check(self):
            if self.changes_issued < self.changes_limit:
                self.changes_issued += 1
                return FakeChange(self.changes_issued - 1)

    current_changes = {}
    ip = 'fake'
    zk = MagicMock()

    def _get_children(path):
        if path == '/bubuku/changes':
            return current_changes.keys()
        else:
            raise NotImplementedError()

    def _get(path: str):
        if path.startswith('/bubuku/changes/'):
            return current_changes.get(path[len('/bubuku/changes/'):]), 'xxx'
        raise NoNodeError()

    def _create(path: str, data: bytes, ephemeral=False):
        if path.startswith('/bubuku/changes/'):
            name = path[len('/bubuku/changes/'):]
            if name in current_changes:
                raise NodeExistsError()
            assert ephemeral
            current_changes[name] = data
        else:
            raise NotImplementedError()

    def _delete(path, **kwargs):
        if path.startswith('/bubuku/changes/'):
            name = path[len('/bubuku/changes/'):]
            if name not in current_changes:
                raise NoNodeError()
            del current_changes[name]
        else:
            raise NotImplementedError()

    zk.get_children = _get_children
    zk.get = _get
    zk.create = _create
    zk.delete = _delete
    controller = Controller(MagicMock(), zk, MagicMock())
    controller.add_check(FakeCheck())

    assert [3, 3, 3] == running_count
    controller.make_step(ip)
    assert not current_changes
    assert [3, 3, 3] == running_count
    controller.make_step(ip)
    assert current_changes
    assert [2, 3, 3] == running_count
    controller.make_step(ip)
    assert [1, 3, 3] == running_count
    controller.make_step(ip)
    assert [0, 3, 3] == running_count
    controller.make_step(ip)
    assert [0, 2, 3] == running_count
    controller.make_step(ip)
    assert [0, 1, 3] == running_count
    controller.make_step(ip)
    assert [0, 0, 3] == running_count
    controller.make_step(ip)
    assert [0, 0, 2] == running_count
    controller.make_step(ip)
    assert [0, 0, 1] == running_count
    assert current_changes
    controller.make_step(ip)
    assert [0, 0, 0] == running_count
    assert not current_changes
    controller.make_step(ip)
    assert [0, 0, 0] == running_count
    assert not current_changes
