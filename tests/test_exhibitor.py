import json
import re
from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError, NodeExistsError

from bubuku.zookeeper import BukuExhibitor


def test_get_broker_ids():
    real_ex = MagicMock()

    def _get_children(path):
        if path == '/brokers/ids':
            return ['3', '1', '2']
        else:
            raise NotImplementedError()

    real_ex.get_children = _get_children

    buku = BukuExhibitor(real_ex)

    assert ['1', '2', '3'] == buku.get_broker_ids()  # ensure that return list is sorted


def test_is_broker_registered():
    def _get(path):
        if path == '/brokers/ids/123':
            return '123', object()
        elif path == '/brokers/ids/321':
            return None, None
        else:
            raise NoNodeError()

    real_ex = MagicMock()
    real_ex.get = _get
    buku = BukuExhibitor(real_ex)

    assert buku.is_broker_registered('123')
    assert buku.is_broker_registered(123)
    assert not buku.is_broker_registered('321')
    assert not buku.is_broker_registered(321)
    assert not buku.is_broker_registered(333)
    assert not buku.is_broker_registered('333')


def _test_load_partition_assignment(async: bool):
    real_ex = MagicMock()

    def _get_children(path):
        if path == '/brokers/topics':
            return ['t01', 't02']
        else:
            raise NotImplementedError()

    def _get(path):
        if path == '/brokers/topics/t01':
            return json.dumps({'partitions': {0: [1, 2, 3], 1: [3, 2, 1]}}).encode('utf-8'), object()
        elif path == '/brokers/topics/t02':
            return json.dumps({'partitions': {0: [4, 5, 6], 1: [5, 1, 2]}}).encode('utf-8'), object()
        else:
            raise NotImplementedError()

    def _get_async(path):
        def _get_iresult(block):
            assert block
            return _get(path)

        mock = MagicMock()
        mock.get = _get_iresult
        return mock

    real_ex.get = _get
    real_ex.get_async = _get_async
    real_ex.get_children = _get_children

    buku_ex = BukuExhibitor(real_ex, async)

    expected_result = [
        ('t01', 0, [1, 2, 3]),
        ('t01', 1, [3, 2, 1]),
        ('t02', 0, [4, 5, 6]),
        ('t02', 1, [5, 1, 2]),
    ]
    result = [r for r in buku_ex.load_partition_assignment()]
    assert len(expected_result) == len(result)
    for e in expected_result:
        assert e in result


def test_load_partition_assignment_sync():
    _test_load_partition_assignment(False)


def test_load_partition_assignment_async():
    _test_load_partition_assignment(True)


def _test_load_partition_states(async: bool):
    real_ex = MagicMock()

    def _get_children(path):
        if path == '/brokers/topics':
            return ['t01', 't02']
        elif path == '/brokers/topics/t01/partitions':
            return ['0', '1']
        elif path == '/brokers/topics/t02/partitions':
            return ['0', '1', '2']
        else:
            raise NotImplementedError()

    def _get(path: str):
        matched = re.match('/brokers/topics/(.*)/partitions/(.*)/state', path)
        if not matched:
            topic = path[len('/brokers/topics/'):]
            if topic not in ['t01', 't02']:
                raise NotImplementedError('Not implemented for path {}'.format(path))
            cnt = 2 if topic == 't01' else 3
            return json.dumps({'partitions': {x: None for x in range(0, cnt)}}).encode('utf-8'), object()
        topic = matched.group(1)
        partition = matched.group(2)
        if topic == 't01' and partition not in ('0', '1'):
            raise NotImplementedError()
        elif topic == 't02' and partition not in ('0', '1', '2'):
            raise NotImplementedError()
        elif topic not in ('t01', 't02'):
            raise NotImplementedError()
        idx = (100 if topic == 't01' else 200) + int(partition)
        return json.dumps({'fake_data': idx}).encode('utf-8'), object()

    def _get_async(path):
        def _get_iasync(block):
            assert block
            return _get(path)

        mock = MagicMock()
        mock.get = _get_iasync
        return mock

    real_ex.get = _get
    real_ex.get_async = _get_async
    real_ex.get_children = _get_children

    buku_ex = BukuExhibitor(real_ex, async=async)

    expected_result = [
        ('t01', 0, {'fake_data': 100}),
        ('t01', 1, {'fake_data': 101}),
        ('t02', 0, {'fake_data': 200}),
        ('t02', 1, {'fake_data': 201}),
        ('t02', 2, {'fake_data': 202}),
    ]

    result = [r for r in buku_ex.load_partition_states()]
    assert len(expected_result) == len(result)
    for e in expected_result:
        assert e in result


def test_load_partition_states_sync():
    _test_load_partition_states(False)


def test_load_partition_states_async():
    _test_load_partition_states(True)


def test_reallocate_partition():
    call_idx = [0]

    def _create(path, value=None, **kwargs):
        if path == '/bubuku/changes':
            pass
        elif path == '/admin/reassign_partitions':
            if call_idx[0] >= 5:
                raise NodeExistsError()
            call_idx[0] += 1
            j = json.loads(value.decode('utf-8'))
            assert j['version'] == '1'
            assert len(j['partitions']) == 1
            p = j['partitions'][0]
            assert p['topic'] == 't01'
            assert p['partition'] == 0
            assert p['replicas'] == [1, 2, 3]
        else:
            raise NotImplementedError('Not implemented for path {}'.format(path))

    zk = MagicMock()
    zk.create = _create

    buku = BukuExhibitor(zk)

    assert buku.reallocate_partition('t01', 0, ['1', '2', '3'])
    assert buku.reallocate_partition('t01', 0, ['1', '2', 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    # Node exists
    assert not buku.reallocate_partition('t01', 0, [1, 2, 3])
