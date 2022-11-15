from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls
from gainy.trading.service import TradingService
from gainy.trading.repository import TradingRepository
from gainy.trading.jobs.update_account_balances import UpdateAccountBalancesJob
from gainy.trading.models import TradingAccount, FundingAccount


def test_update_account_balances_job(monkeypatch):
    account_id = 10

    trading_account = TradingAccount()
    monkeypatch.setattr(trading_account, "id", account_id)

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "iterate_all",
        mock_find([
            (TradingAccount, None, [trading_account]),
        ]))

    sync_balances_calls = []
    trading_service = TradingService(None, None, None)
    monkeypatch.setattr(trading_service, "sync_balances",
                        mock_record_calls(sync_balances_calls))

    UpdateAccountBalancesJob(trading_repository,
                             trading_service)._update_trading_accounts()

    assert trading_account in [args[0] for args, kwargs in sync_balances_calls]


def test_update_funding_accounts(monkeypatch):
    account_id = 10

    funding_account = FundingAccount()
    monkeypatch.setattr(funding_account, "id", account_id)

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "iterate_all",
        mock_find([
            (FundingAccount, None, [funding_account]),
        ]))

    trading_service = TradingService(None, None, None)
    update_funding_accounts_balance_calls = []
    monkeypatch.setattr(
        trading_service, "update_funding_accounts_balance",
        mock_record_calls(update_funding_accounts_balance_calls))

    UpdateAccountBalancesJob(trading_repository,
                             trading_service)._update_funding_accounts()

    assert [funding_account] in [
        args[0] for args, kwargs in update_funding_accounts_balance_calls
    ]
