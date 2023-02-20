from gainy.tests.mocks.trading.drivewealth.api_mocks import PORTFOLIO_STATUS, CASH_ACTUAL_VALUE, \
    FUND1_ID, FUND2_ID, FUND2_VALUE, FUND1_ACTUAL_VALUE

from gainy.trading.drivewealth.models import DriveWealthPortfolioStatus


def test_set_from_response(monkeypatch):
    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)
    assert portfolio_status.cash_value == CASH_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND1_ID) == FUND1_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND2_ID) == FUND2_VALUE
