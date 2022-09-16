from gainy.tests.mocks.repository_mocks import mock_find
from gainy.trading import TradingRepository, TradingService
from gainy.trading.jobs.update_account_balances import UpdateAccountBalancesJob
from gainy.trading.models import TradingAccount


def mock_sync_trading_accounts(visited_users=None):

    def mock(profile_id):
        print(profile_id)
        if visited_users is not None:
            visited_users[profile_id] = True

    return mock


def test_update_account_balances_job(monkeypatch):
    profile_id = 10

    trading_account = TradingAccount()
    monkeypatch.setattr(trading_account, "profile_id", profile_id)

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "iterate_all",
        mock_find([
            (TradingAccount, None, [trading_account]),
        ]))

    visited_users = {}
    trading_service = TradingService(None)
    monkeypatch.setattr(trading_service, "sync_trading_accounts",
                        mock_sync_trading_accounts(visited_users))

    UpdateAccountBalancesJob(trading_repository, trading_service).run()

    assert profile_id in visited_users
