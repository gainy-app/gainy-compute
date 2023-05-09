from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from gainy.trading.drivewealth.models import DriveWealthTransaction
from gainy.trading.drivewealth.event_handlers.transactions_created import TransactionsCreatedEventHandler
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None, None)
    on_new_transaction_calls = []
    monkeypatch.setattr(provider, "on_new_transaction",
                        mock_record_calls(on_new_transaction_calls))
    event_handler = TransactionsCreatedEventHandler(repository, provider, None,
                                                    None)
    sync_trading_account_balances_calls = []
    monkeypatch.setattr(event_handler, "sync_trading_account_balances",
                        mock_record_calls(sync_trading_account_balances_calls))

    message = {
        "accountID": "b25f0d36-b4e4-41f8-b3d9-9249e46402cd.1491330741850",
        "transaction": {
            "accountAmount": 2.29,
            "finTranID": "GF.861e931d-e7aa-47c8-b87a-b1e55acf3862",
            "finTranTypeID": "DIV",
            "instrument": {
                "symbol": "BPY",
            }
        }
    }

    event_handler.handle(message)

    assert DriveWealthTransaction in persisted_objects
    transaction: DriveWealthTransaction = persisted_objects[
        DriveWealthTransaction][0]
    assert transaction.account_id == message["accountID"]
    assert transaction.ref_id == message["transaction"]["finTranID"]
    assert transaction.type == message["transaction"]["finTranTypeID"]
    assert transaction.symbol == message["transaction"]["instrument"]["symbol"]
    assert transaction.account_amount_delta == message["transaction"][
        "accountAmount"]

    # assert ((transaction.account_id, ), {}) in on_new_transaction_calls
    assert ((transaction.account_id, ), {
        "force": True
    }) in sync_trading_account_balances_calls
