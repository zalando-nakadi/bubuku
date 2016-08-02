import json
from unittest.mock import MagicMock

from bubuku.features.rebalance_by_size import GenerateDataSizeStatistics
from bubuku.utils import CmdHelper


def test_size_stats_collecting():
    zk = MagicMock()

    stat_check = GenerateDataSizeStatistics(zk, mock_broker(), mock_cmd_helper(), "/kafka-logs")
    stat_check.check()

    expected_json = {"disk": {"free": 500, "used": 150},
                     "topics": {"another_topic": {"0": 12}, "my-topic": {"0": 1120, "2": 551}}}
    expected_data = json.dumps(expected_json, sort_keys=True, separators=(',', ':')).encode("utf-8")

    zk.create.assert_called_with("/bubuku/size_stats/dummy_id", expected_data, makepath=True)


def mock_cmd_helper() -> CmdHelper:

    class CmdHelperMock(CmdHelper):
        def cmd_run(self, cmd: str):
            if cmd.startswith("du"):
                return "1120\t/kafka-logs/my-topic-0\n" \
                       "551\t/kafka-logs/my-topic-2\n" \
                       "12\t/kafka-logs/another_topic-0"
            elif cmd.startswith("df"):
                return "100 200\n" \
                       "50 300"
            else:
                raise ValueError("Call not expected")

    return CmdHelperMock()


def mock_broker() -> MagicMock:
    broker = MagicMock()
    broker.is_running_and_registered.return_value = True
    broker.id_manager.get_broker_id.return_value = "dummy_id"
    return broker
