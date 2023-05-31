import pytest

from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_find
from gainy.trading.drivewealth.locking_functions.handle_new_transaction import HandleNewTransaction
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthPortfolioStatus, DriveWealthAccount
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def get_test_on_new_transaction_portfolio_changed():
    return [True, False]


@pytest.mark.parametrize("portfolio_changed",
                         get_test_on_new_transaction_portfolio_changed())
@pytest.mark.skip()
def test_on_new_transaction(monkeypatch, portfolio_changed):
    account_ref_id = "account_ref_id"

    portfolio_status = DriveWealthPortfolioStatus()

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "is_open", lambda: True)

    portfolio = DriveWealthPortfolio()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthPortfolio, {
                "drivewealth_account_id": account_ref_id
            }, portfolio),
            (DriveWealthAccount, {
                "ref_id": account_ref_id
            }, account),
        ]))

    provider = DriveWealthProvider(repository, None, None, None, None)

    def mock_sync_portfolio_status(_portfolio, force=None, allow_invalid=None):
        assert _portfolio == portfolio
        assert force
        assert allow_invalid
        return portfolio_status

    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_sync_portfolio_status)

    def mock_actualize_portfolio(_portfolio, _portfolio_status):
        assert _portfolio == portfolio
        assert _portfolio_status == portfolio_status
        return portfolio_changed

    monkeypatch.setattr(provider, "actualize_portfolio",
                        mock_actualize_portfolio)

    send_portfolio_to_api_calls = []
    monkeypatch.setattr(provider, "send_portfolio_to_api",
                        mock_record_calls(send_portfolio_to_api_calls))
    sync_portfolio_calls = []
    monkeypatch.setattr(provider, "sync_portfolio",
                        mock_record_calls(sync_portfolio_calls))

    func = HandleNewTransaction(repository, provider, None, account_ref_id)
    func._do(None)

    assert ((portfolio, ), {}) in sync_portfolio_calls
    if portfolio_changed:
        assert ((portfolio, ), {}) in send_portfolio_to_api_calls
    else:
        assert not send_portfolio_to_api_calls
