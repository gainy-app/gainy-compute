import pytest

from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.models import DriveWealthPortfolio
from gainy.trading.service import TradingService
from gainy.trading.jobs.update_account_balances import UpdateAccountBalancesJob
from gainy.trading.models import TradingAccount, FundingAccount


def test_update_account_balances_job(monkeypatch):
    account_id = 10

    trading_account = TradingAccount()
    monkeypatch.setattr(trading_account, "id", account_id)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "iterate_all",
        mock_find([
            (TradingAccount, None, [trading_account]),
        ]))

    sync_balances_calls = []
    trading_service = TradingService(None, None, None)
    monkeypatch.setattr(trading_service, "sync_balances",
                        mock_record_calls(sync_balances_calls))

    UpdateAccountBalancesJob(repository,
                             trading_service)._update_trading_accounts()

    assert trading_account in [args[0] for args, kwargs in sync_balances_calls]


def test_update_funding_accounts(monkeypatch):
    account_id = 10

    funding_account = FundingAccount()
    monkeypatch.setattr(funding_account, "id", account_id)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "iterate_all",
        mock_find([
            (FundingAccount, None, [funding_account]),
        ]))

    trading_service = TradingService(None, None, None)
    update_funding_accounts_balance_calls = []
    monkeypatch.setattr(
        trading_service, "update_funding_accounts_balance",
        mock_record_calls(update_funding_accounts_balance_calls))

    UpdateAccountBalancesJob(repository,
                             trading_service)._update_funding_accounts()

    assert [funding_account] in [
        args[0] for args, kwargs in update_funding_accounts_balance_calls
    ]


def get_realtime():
    return [True, False]


@pytest.mark.parametrize("realtime", get_realtime())
def test_update_portfolios(monkeypatch, realtime):
    portfolio = DriveWealthPortfolio()

    repository = DriveWealthRepository(None)
    if realtime:
        monkeypatch.setattr(repository, "iterate_portfolios_to_sync",
                            lambda: [portfolio])
    else:
        monkeypatch.setattr(
            repository, "iterate_all",
            mock_find([
                (DriveWealthPortfolio, None, [portfolio]),
            ]))

    provider = DriveWealthProvider(None, None, None)
    sync_portfolio_calls = []
    monkeypatch.setattr(provider, "sync_portfolio",
                        mock_record_calls(sync_portfolio_calls))
    sync_portfolio_status_calls = []
    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_record_calls(sync_portfolio_status_calls))

    trading_service = TradingService(None, provider, None)

    UpdateAccountBalancesJob(repository,
                             trading_service)._update_portfolios(realtime)

    assert portfolio in [args[0] for args, kwargs in sync_portfolio_calls]
    assert portfolio in [
        args[0] for args, kwargs in sync_portfolio_status_calls
    ]
