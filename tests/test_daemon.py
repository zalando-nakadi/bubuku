from unittest.mock import MagicMock

from bubuku.daemon import apply_features
from bubuku.features.rebalance import RebalanceOnStartCheck, RebalanceOnBrokerListChange
from bubuku.features.restart_on_zk_change import CheckExhibitorAddressChanged
from bubuku.features.terminate import get_registration
from test_config import build_test_properties


class TestController(object):
    def __init__(self):
        self.checks = []

    def add_check(self, check):
        self.checks.append(check)


def test_load_restart_on_exhibitor():
    exhibitor = object()
    broker = object()

    controller = TestController()

    apply_features(-1, {'restart_on_exhibitor': {}}, controller, exhibitor, broker, None, None)

    assert len(controller.checks) == 1
    check = controller.checks[0]
    assert type(check) == CheckExhibitorAddressChanged
    assert check.zk == exhibitor
    assert check.broker == broker


def test_rebalance_on_start():
    exhibitor = object()
    broker = object()

    controller = TestController()

    apply_features(-1, {'rebalance_on_start': {}}, controller, exhibitor, broker, None, None)

    assert len(controller.checks) == 1
    check = controller.checks[0]
    assert type(check) == RebalanceOnStartCheck
    assert check.zk == exhibitor
    assert check.broker == broker
    assert not check.executed


def test_rebalance_on_broker_list_change():
    exhibitor = object()
    broker = object()

    controller = TestController()

    apply_features(-1, {'rebalance_on_brokers_change': {}}, controller, exhibitor, broker, None, None)

    assert len(controller.checks) == 1
    check = controller.checks[0]
    assert type(check) == RebalanceOnBrokerListChange
    assert check.zk == exhibitor
    assert check.broker == broker


def test_graceful_terminate():
    c, b = get_registration()
    assert c is None
    assert b is None

    broker = object()

    controller = TestController()

    apply_features(-1, {'graceful_terminate': {}}, controller, None, broker, None, None)

    assert len(controller.checks) == 0

    c, b = get_registration()
    assert c == controller
    assert b == broker


def test_use_ip_address():
    props = build_test_properties()
    assert props.get_property('advertised.host.name') is None

    amazon = MagicMock()
    amazon.get_id = MagicMock(return_value='172.31.146.57')

    apply_features(-1, {'use_ip_address': {}}, None, None, None, props, amazon)

    assert props.get_property('advertised.host.name') == '172.31.146.57'
