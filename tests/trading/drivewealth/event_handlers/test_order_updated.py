from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from gainy.trading.drivewealth.event_handlers import OrderUpdatedEventHandler
from gainy.trading.drivewealth.models import DriveWealthOrder
from gainy.trading.drivewealth import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository

message = {
    "id": "JK.7f6bbe0d-9341-437a-abf2-f028c2e7eb02",
    "status": "NEW",
    "accountID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494",
    "symbol": "GOOG",
    "lastExecuted": "2022-11-07T14:31:20.187Z",
    "side": "BUY",
    "totalOrderAmount": 1,
}


def test_exists(monkeypatch):
    order = DriveWealthOrder()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthOrder, {
            "ref_id": message["id"]
        }, order)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None, None)
    handle_order_calls = []
    monkeypatch.setattr(provider, 'handle_order',
                        mock_record_calls(handle_order_calls))

    event_handler = OrderUpdatedEventHandler(repository, provider, None, None)

    event_handler.handle(message)

    assert DriveWealthOrder in persisted_objects
    assert order in persisted_objects[DriveWealthOrder]
    assert order in [args[0] for args, kwargs in handle_order_calls]


def test_not_exists(monkeypatch):
    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthOrder, {
            "ref_id": message["id"]
        }, None)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None, None)
    handle_order_calls = []
    monkeypatch.setattr(provider, 'handle_order',
                        mock_record_calls(handle_order_calls))

    event_handler = OrderUpdatedEventHandler(repository, provider, None, None)

    event_handler.handle(message)

    assert DriveWealthOrder in persisted_objects
    assert len(persisted_objects[DriveWealthOrder]) > 0
    for order in persisted_objects[DriveWealthOrder]:
        assert order in [args[0] for args, kwargs in handle_order_calls]
