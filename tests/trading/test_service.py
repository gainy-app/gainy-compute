from gainy.plaid.models import PlaidAccessToken, PlaidAccount
from gainy.plaid.service import PlaidService
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist
from gainy.trading.service import TradingService
from gainy.trading.repository import TradingRepository
from gainy.trading.models import FundingAccount


def test_update_funding_accounts_balance(monkeypatch):
    plaid_access_token_id = 1
    plaid_account_id = 2
    balance_available = 3
    access_token = "access_token"

    funding_account = FundingAccount()
    monkeypatch.setattr(funding_account, "plaid_access_token_id",
                        plaid_access_token_id)
    monkeypatch.setattr(funding_account, "plaid_account_id", plaid_account_id)

    plaid_access_token = PlaidAccessToken()
    monkeypatch.setattr(plaid_access_token, "access_token", access_token)

    plaid_account = PlaidAccount()
    monkeypatch.setattr(plaid_account, "account_id", plaid_account_id)
    monkeypatch.setattr(plaid_account, "balance_available", balance_available)

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "find_one",
        mock_find([
            (PlaidAccessToken, {
                "id": plaid_access_token_id
            }, plaid_access_token),
        ]))
    persisted_objects = {}
    monkeypatch.setattr(trading_repository, "persist",
                        mock_persist(persisted_objects))

    plaid_service = PlaidService(None)

    def mock_get_item_accounts(_access_token, plaid_account_ids):
        assert _access_token == plaid_access_token
        assert plaid_account_ids == [plaid_account_id]
        return [plaid_account]

    monkeypatch.setattr(plaid_service, "get_item_accounts",
                        mock_get_item_accounts)

    trading_service = TradingService(trading_repository, None, plaid_service)
    trading_service.update_funding_accounts_balance([funding_account])

    assert funding_account in persisted_objects[FundingAccount]
    assert funding_account.balance == balance_available
