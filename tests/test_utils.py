from bubuku.utils import split_json

json_to_split = {
    "my_key1": "value1",
    "my_key2": "value2",
    "my_key3": "value3",
    "my_key4": "value4",
    "my_key5": "value5",
    "my_key6": "value6",
}


def test_split_json():
    json_parts = split_json(json_to_split, 50)
    print(json_parts)
    assert len(json_parts) == 3
    assert json_parts.pop() == {"my_key1": "value1", "my_key2": "value2"}
    assert json_parts.pop() == {"my_key3": "value3", "my_key4": "value4"}
    assert json_parts.pop() == {"my_key5": "value5", "my_key6": "value6"}


def test_split_json_not_needed():
    json_parts = split_json(json_to_split, 1000)
    assert len(json_parts) == 1
    assert json_parts[0] == json_to_split
