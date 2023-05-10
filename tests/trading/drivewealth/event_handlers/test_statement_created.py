import pytest

from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from gainy.trading.drivewealth.event_handlers import StatementCreatedEventHandler
from gainy.trading.drivewealth.models import DriveWealthStatement
from gainy.trading.models import TradingStatementType
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def get_test_types():
    return ["taxForm", "tradeConfirm", "statement"]


@pytest.mark.parametrize("type", get_test_types())
def test(monkeypatch, type):
    profile_id = 1
    message = {
        type: {
            "displayName": "Apr 28, 2017 Statement",
            "fileKey": "2017042802"
        },
        "accountID": "cc07f91b-7ee1-4868-b8fc-823c70a1b932.1407775317759",
        "userID": "cc07f91b-7ee1-4868-b8fc-823c70a1b932"
    }
    if type == "taxForm":
        type_enum = TradingStatementType.TAX
    elif type == "tradeConfirm":
        type_enum = TradingStatementType.TRADE_CONFIRMATION
    elif type == "statement":
        type_enum = TradingStatementType.MONTHLY_STATEMENT
    else:
        raise Exception('unknown type')

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))
    refresh_calls = []
    monkeypatch.setattr(repository, 'refresh',
                        mock_record_calls(refresh_calls))

    provider = DriveWealthProvider(None, None, None, None, None)
    create_trading_statements_calls = []
    monkeypatch.setattr(provider, 'create_trading_statements',
                        mock_record_calls(create_trading_statements_calls))

    def mock_get_profile_id_by_user_id(user_id):
        assert user_id == message["userID"]
        return profile_id

    monkeypatch.setattr(provider, 'get_profile_id_by_user_id',
                        mock_get_profile_id_by_user_id)

    event_handler = StatementCreatedEventHandler(repository, provider, None,
                                                 None)

    event_handler.handle(message)

    assert DriveWealthStatement in persisted_objects
    statement: DriveWealthStatement = persisted_objects[DriveWealthStatement][
        0]
    assert ((statement, ), {}) in refresh_calls
    assert statement.file_key == message[type]["fileKey"]
    assert statement.type == type_enum
    assert statement.display_name == message[type]["displayName"]
    assert statement.account_id == message["accountID"]
    assert statement.user_id == message["userID"]

    assert ([statement], profile_id) in [
        args for args, kwargs in create_trading_statements_calls
    ]
