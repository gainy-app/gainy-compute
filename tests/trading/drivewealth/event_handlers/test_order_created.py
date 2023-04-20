from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from gainy.trading.drivewealth.event_handlers import OrderCreatedEventHandler
from gainy.trading.drivewealth.models import DriveWealthOrder
from gainy.trading.drivewealth import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None, None)
    handle_order_calls = []
    monkeypatch.setattr(provider, 'handle_order',
                        mock_record_calls(handle_order_calls))

    event_handler = OrderCreatedEventHandler(repository, provider, None, None)

    message = {
        "id": "JK.7f6bbe0d-9341-437a-abf2-f028c2e7eb02",
        "status": "NEW",
        "accountID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494",
        "symbol": "GOOG",
        "side": "BUY",
        "totalOrderAmount": 1,
    }
    event_handler.handle(message)

    assert DriveWealthOrder in persisted_objects
    assert len(persisted_objects[DriveWealthOrder]) > 0
    for order in persisted_objects[DriveWealthOrder]:
        assert order in [args[0] for args, kwargs in handle_order_calls]
