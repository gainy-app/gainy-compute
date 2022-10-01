from gainy.tests.mocks.repository_mocks import mock_find
from gainy.trading import TradingRepository, TradingService
from gainy.trading.jobs.update_account_balances import UpdateAccountBalancesJob
from gainy.trading.models import TradingAccount


def mock_sync_trading_account(visited_trading_accounts=None):

    def mock(account):
        if visited_trading_accounts is not None:
            visited_trading_accounts[account] = True

    return mock


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

    visited_trading_accounts = {}
    trading_service = TradingService(None)
    monkeypatch.setattr(trading_service, "sync_trading_account",
                        mock_sync_trading_account(visited_trading_accounts))

    UpdateAccountBalancesJob(trading_repository, trading_service).run()

    assert trading_account in visited_trading_accounts
