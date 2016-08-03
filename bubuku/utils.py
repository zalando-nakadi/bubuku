import json
import subprocess


class CmdHelper:
    def cmd_run(self, cmd):
        output = subprocess.check_output(cmd, shell=True)
        return output.decode("utf-8")


def split_json(json_data, size_limit_b):
    def json_size_b(json_data):
        json_byte_str = json.dumps(json_data, separators=(',', ':')).encode("utf-8")
        return len(json_byte_str)

    if json_size_b(json_data) < size_limit_b:
        return [json_data]

    split_list = {0: {}}
    cur_index = 0
    keys = list(json_data.keys())
    keys.sort()
    while len(keys) > 0:
        key = keys.pop()
        split_list[cur_index][key] = json_data[key]

        if json_size_b(split_list[cur_index]) > size_limit_b:
            del split_list[cur_index][key]
            keys.append(key)
            cur_index += 1
            split_list[cur_index] = {}
        else:
            del json_data[key]
            if json_size_b(json_data) < size_limit_b:
                split_list[cur_index + 1] = json_data
                break

    return list(split_list.values())
