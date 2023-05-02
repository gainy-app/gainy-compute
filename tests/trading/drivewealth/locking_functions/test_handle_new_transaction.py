import pytest

from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_find
from gainy.trading.drivewealth.locking_functions.handle_new_transaction import HandleNewTransaction
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthPortfolioStatus
from gainy.trading.drivewealth import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def get_test_on_new_transaction_portfolio_changed():
    return [True, False]


@pytest.mark.parametrize("portfolio_changed",
                         get_test_on_new_transaction_portfolio_changed())
def test_on_new_transaction(monkeypatch, portfolio_changed):
    account_ref_id = "account_ref_id"

    portfolio_status = DriveWealthPortfolioStatus()

    portfolio = DriveWealthPortfolio()
    normalize_weights_calls = []
    monkeypatch.setattr(portfolio, "normalize_weights",
                        mock_record_calls(normalize_weights_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthPortfolio, {
                "drivewealth_account_id": account_ref_id
            }, portfolio),
        ]))

    provider = DriveWealthProvider(repository, None, None, None, None)

    def mock_sync_portfolio_status(_portfolio, force=None):
        assert _portfolio == portfolio
        assert force
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
        assert normalize_weights_calls
        assert ((portfolio, ), {}) in send_portfolio_to_api_calls
    else:
        assert not normalize_weights_calls
        assert not send_portfolio_to_api_calls
