from bubuku.cli import _print_table, _dump_replica_assignment_as_json_bytes


def test_print_table():
    lines = []
    _print_table([{'Test': 1, 'Test2': '123456789'}, {'Test2': 'Test1', 'Test3': None}], lambda x: lines.append(x))
    assert len(lines) == 3
    assert lines[0] == 'Test  Test2      Test3'
    assert lines[1] == '1     123456789       '
    assert lines[2] == '      Test1      None '


def test_dump_replica_assignment():
    assert _dump_replica_assignment_as_json_bytes([('topic-a', "1")]) \
        == b'''{"version":1,"partitions":[{"topic":"topic-a","partition":1}]}'''
