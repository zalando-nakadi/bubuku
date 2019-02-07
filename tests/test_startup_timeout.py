import unittest

from bubuku.broker import StartupTimeout


class TestStartupTimeout(unittest.TestCase):
    @staticmethod
    def _verify(o: StartupTimeout, border_value: float, border_value_after_fail: float):
        print(o)
        assert not o.is_timed_out(border_value)
        assert o.is_timed_out(border_value + 1)
        o.on_timeout_fail()
        assert not o.is_timed_out(border_value_after_fail)
        assert o.is_timed_out(border_value_after_fail + 1)

    def test_linear_defaults(self):
        o = StartupTimeout.build({'type': 'linear'})
        TestStartupTimeout._verify(o, 300., 360.)

    def test_linear(self):
        o = StartupTimeout.build({'type': 'linear', 'initial': '10', 'step': 2})
        TestStartupTimeout._verify(o, 10., 12.)

    def test_progressive_defaults(self):
        o = StartupTimeout.build({'type': 'progressive'})
        TestStartupTimeout._verify(o, 300., 450.)

    def test_progressive(self):
        o = StartupTimeout.build({'type': 'progressive', 'initial': '16', 'step': '0.25'})

        TestStartupTimeout._verify(o, 16., 20.)
