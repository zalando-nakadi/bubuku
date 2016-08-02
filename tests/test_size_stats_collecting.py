import json
from unittest.mock import MagicMock

from bubuku.features.rebalance_by_size import GenerateDataSizeStatistics
from bubuku.utils import CmdHelper


def test_size_stats_collecting():
    zk = MagicMock()

    stat_check = GenerateDataSizeStatistics(zk, mock_broker(), mock_cmd_helper(), "/kafka-logs")
    stat_check.check()

    expected_json = {
        "disk": {"free": 606, "used": 404},
        "topics": {
            "another_topic": {"0": 3},
            "my-topic": {"0": 10, "2": 200}
        }
    }
    expected_data = json.dumps(expected_json, sort_keys=True, separators=(',', ':')).encode("utf-8")

    zk.create.assert_called_with("/bubuku/size_stats/dummy_id", expected_data, makepath=True)


def mock_cmd_helper() -> CmdHelper:
    class CmdHelperMock(CmdHelper):
        def cmd_run(self, cmd: str):
            if cmd.startswith("du"):
                return "10\t/kafka-logs/my-topic-0\n" \
                       "200\t/kafka-logs/my-topic-2\n" \
                       "3\t/kafka-logs/another_topic-0\n" \
                       "55\t/kafka-logs\n" \
                       "77\t/kafka-logs/wrong_topic\n" \
                       "blah"
            elif cmd.startswith("df"):
                return "101 202\n" \
                       "303 404\n" \
                       "500"
            else:
                raise ValueError("Call not expected")

    return CmdHelperMock()


def mock_broker() -> MagicMock:
    broker = MagicMock()
    broker.is_running_and_registered.return_value = True
    broker.id_manager.get_broker_id.return_value = "dummy_id"
    return broker
