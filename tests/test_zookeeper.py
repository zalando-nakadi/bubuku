import json
import math
import re
import time
import unittest
from unittest.mock import MagicMock

from kazoo.exceptions import NoNodeError, NodeExistsError

from bubuku.zookeeper import BukuExhibitor, SlowlyUpdatedCache


def test_get_broker_ids():
    exhibitor_mock = MagicMock()

    def _get_children(path):
        if path == '/brokers/ids':
            return ['3', '1', '2']
        else:
            raise NotImplementedError()

    exhibitor_mock.get_children = _get_children

    buku = BukuExhibitor(exhibitor_mock)

    assert ['1', '2', '3'] == buku.get_broker_ids()  # ensure that return list is sorted


def test_is_broker_registered():
    def _get(path):
        if path == '/brokers/ids/123':
            return '123', object()
        elif path == '/brokers/ids/321':
            return None, None
        else:
            raise NoNodeError()

    exhibitor_mock = MagicMock()
    exhibitor_mock.get = _get
    buku = BukuExhibitor(exhibitor_mock)

    assert buku.is_broker_registered('123')
    assert buku.is_broker_registered(123)
    assert not buku.is_broker_registered('321')
    assert not buku.is_broker_registered(321)
    assert not buku.is_broker_registered(333)
    assert not buku.is_broker_registered('333')


def _test_load_partition_assignment(async: bool):
    exhibitor_mock = MagicMock()

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

    exhibitor_mock.get = _get
    exhibitor_mock.get_async = _get_async
    exhibitor_mock.get_children = _get_children

    buku_ex = BukuExhibitor(exhibitor_mock, async)

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
    exhibitor_mock = MagicMock()

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

    exhibitor_mock.get = _get
    exhibitor_mock.get_async = _get_async
    exhibitor_mock.get_children = _get_children

    buku_ex = BukuExhibitor(exhibitor_mock, async=async)

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
        if path in ('/bubuku/changes', '/bubuku/actions/global'):
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

    exhibitor_mock = MagicMock()
    exhibitor_mock.create = _create

    buku = BukuExhibitor(exhibitor_mock)

    assert buku.reallocate_partition('t01', 0, ['1', '2', '3'])
    assert buku.reallocate_partition('t01', 0, ['1', '2', 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    assert buku.reallocate_partition('t01', 0, [1, 2, 3])
    # Node exists
    assert not buku.reallocate_partition('t01', 0, [1, 2, 3])


class SlowlyUpdatedCacheTest(unittest.TestCase):
    def test_initial_update_fast(self):
        result = [None]

        def _update(value_):
            result[0] = value_

        cache = SlowlyUpdatedCache(lambda: (['test'], 1), _update, 0, 0)

        cache.touch()
        assert result[0] == (['test'], 1)

    def test_exception_eating(self):
        result = [10, None]

        def _update(value_):
            result[1] = value_

        def _load():
            if result[0] > 0:
                result[0] -= 1
                raise Exception()
            return ['test'], 1

        cache = SlowlyUpdatedCache(_load, _update, 0, 0)
        cache.force = False  # Small hack to avoid initial refresh cycle
        for i in range(0, 10):
            cache.touch()
            assert result[1] is None
            assert result[0] == 9 - i
        cache.touch()
        assert result[1] == (['test'], 1)

    def test_initial_update_slow(self):
        result = [None]
        call_count = [0]

        def _load():
            call_count[0] += 1
            if call_count[0] == 100:
                return ['test'], 1
            return None

        def _update(value_):
            result[0] = value_

        cache = SlowlyUpdatedCache(_load, _update, 0, 0)

        cache.touch()
        assert call_count[0] == 100
        assert result[0] == (['test'], 1)

    def test_delays_illegal(self):
        result = [None]
        load_calls = []
        update_calls = []

        def _load():
            load_calls.append(time.time())
            return ['test'], 0 if len(load_calls) > 1 else 1

        def _update(value_):
            update_calls.append(time.time())
            result[0] = value_

        # refresh every 1 second, delay 0.5 second
        cache = SlowlyUpdatedCache(_load, _update, 0.5, 0.25)

        while len(update_calls) != 2:
            time.sleep(0.1)
            cache.touch()
            print(cache)

        assert math.fabs(update_calls[0] - load_calls[0]) <= 0.15  # 0.1 + 0.1/2
        # Verify that load calls were made one by another
        assert math.fabs(load_calls[1] - load_calls[0] - .5) <= 0.15
        # Verity that update call was made in correct interval

        assert load_calls[1] + 0.25 <= update_calls[1] <= load_calls[1] + 0.25 + 0.15

    def test_delays_legal(self):
        result = [None]
        main_call = []
        load_calls = []
        update_calls = []

        def _load():
            load_calls.append(time.time())
            if len(load_calls) == 5:
                main_call.append(time.time())
            return ['test'], 0 if len(load_calls) >= 5 else len(load_calls)

        def _update(value_):
            update_calls.append(time.time())
            result[0] = value_

        # refresh every 1 second, delay 5 second - in case where situation is constantly changing - wait for
        # last stable update
        cache = SlowlyUpdatedCache(_load, _update, 0.5, 3)

        while len(update_calls) != 2:
            time.sleep(0.1)
            cache.touch()
            print(cache)

        assert len(main_call) == 1
        assert main_call[0] + 3 - .15 < update_calls[1] < main_call[0] + 3 + .15
