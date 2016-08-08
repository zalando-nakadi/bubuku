from unittest.mock import MagicMock

from bubuku.features.data_size_stats import GenerateDataSizeStatistics
from bubuku.utils import CmdHelper


def test_size_stats_collecting():
    zk = MagicMock()

    stat_check = GenerateDataSizeStatistics(zk, mock_broker(), mock_cmd_helper(), ["/kafka-logs"])
    stat_check.check()

    expected_json = {
        "disk": {"free_kb": 606, "used_kb": 404},
        "topics": {
            "another_topic": {"0": 3},
            "my-topic": {"0": 10, "2": 200}
        }
    }
    zk.update_disk_stats.assert_called_with('dummy_id', expected_json)


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
