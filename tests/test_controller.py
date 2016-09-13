from unittest.mock import MagicMock

from bubuku.controller import Controller, Check, Change, _exclude_self


def test_exculde_self():

    assert sorted(['test1', 'test2']) == sorted(_exclude_self('127.0.0.1', 'xxx', {
        'test1': '127.0.0.1',
        'test2': '127.0.0.2',
        'xxx': '127.0.0.1',
    }))


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
    zk = MagicMock()
    zk.get_running_changes.return_value = current_changes
    zk.register_change = lambda x, y: current_changes.update({x: y})
    zk.unregister_change = lambda x: current_changes.pop(x)

    controller = Controller(MagicMock(), zk, MagicMock())
    controller.provider_id = 'fake'
    controller.add_check(FakeCheck())

    assert [3, 3, 3] == running_count
    controller.make_step()
    assert not current_changes
    assert [3, 3, 3] == running_count
    controller.make_step()
    assert current_changes
    assert [2, 3, 3] == running_count
    controller.make_step()
    assert [1, 3, 3] == running_count
    controller.make_step()
    assert [0, 3, 3] == running_count
    controller.make_step()
    assert [0, 2, 3] == running_count
    controller.make_step()
    assert [0, 1, 3] == running_count
    controller.make_step()
    assert [0, 0, 3] == running_count
    controller.make_step()
    assert [0, 0, 2] == running_count
    controller.make_step()
    assert [0, 0, 1] == running_count
    assert current_changes
    controller.make_step()
    assert [0, 0, 0] == running_count
    assert not current_changes
    controller.make_step()
    assert [0, 0, 0] == running_count
    assert not current_changes
