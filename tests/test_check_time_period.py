from bubuku.controller import Check, Change
from time import sleep


def test_check_time_period():

    test_check = TestCheck()

    assert test_check.check_if_time() is not None # first time it should always run
    assert test_check.check_if_time() is None # time has not come yet

    sleep(1)
    assert test_check.time_till_check() < 0 # time to run the check
    assert test_check.check_if_time() is not None # should run the check
    assert 0.0 < test_check.time_till_check() < 1 # there's still some time before the check can be run again


class TestCheck(Check):

    check_interval_s = 0.5

    def check(self) -> Change:
        return Change()