import unittest

from bubuku.broker import StartupTimeout


class TestDataSizeStats(unittest.TestCase):
    def test_linear_defaults(self):
        o = StartupTimeout.build({'type': 'linear'})
        TestDataSizeStats._verify(o, 300., 60.)

    @staticmethod
    def _verify(o: StartupTimeout, value: float, step: float):
        print(o)
        assert o.get_timeout() == value
        assert o.get_step() == step

    def test_linear(self):
        o = StartupTimeout.build({'type': 'linear', 'initial': '10', 'step': 2})
        TestDataSizeStats._verify(o, 10., 2.)

        o.on_timeout_fail()
        TestDataSizeStats._verify(o, 12., 2.)

        o.on_timeout_fail()
        TestDataSizeStats._verify(o, 14., 2.)

    def test_progressive_defaults(self):
        o = StartupTimeout.build({'type': 'progressive'})
        TestDataSizeStats._verify(o, 300., 150.)

    def test_progressive(self):
        o = StartupTimeout.build({'type': 'progressive', 'initial': '16', 'scale': '0.25'})

        TestDataSizeStats._verify(o, 16., 4.)

        o.on_timeout_fail()
        TestDataSizeStats._verify(o, 20., 5.)

        o.on_timeout_fail()
        TestDataSizeStats._verify(o, 25., 6.25)
