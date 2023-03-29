import copy

import pytest
from _decimal import Decimal

from gainy.tests.mocks.trading.drivewealth.api_mocks import PORTFOLIO_STATUS, CASH_ACTUAL_VALUE, \
    FUND1_ID, FUND2_ID, FUND2_VALUE, FUND1_ACTUAL_VALUE

from gainy.trading.drivewealth.models import DriveWealthPortfolioStatus


def test_set_from_response(monkeypatch):
    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)
    assert portfolio_status.cash_value == CASH_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND1_ID) == FUND1_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND2_ID) == FUND2_VALUE
    assert portfolio_status.is_valid()


def get_test_invalid_t():
    return range(9)


def get_test_invalid_sign():
    return [-1, 1]


@pytest.mark.parametrize("t", get_test_invalid_t())
@pytest.mark.parametrize("sign", get_test_invalid_sign())
def test_invalid(monkeypatch, t, sign):
    portfolio_status = DriveWealthPortfolioStatus()
    data = copy.deepcopy(PORTFOLIO_STATUS)

    if t == 0:
        # data["equity"] += Decimal(1.01) * sign
        return
    elif t == 1:
        data["holdings"][0]["actual"] += Decimal(0.01) * sign
    elif t == 2:
        # data["holdings"][0]["value"] += Decimal(1.01) * sign
        return
    elif t == 3:
        data["holdings"][1]["actual"] += Decimal(0.01) * sign
    elif t == 4:
        data["holdings"][1]["target"] += Decimal(0.01) * sign
    elif t == 5:
        data["holdings"][1]["value"] += Decimal(1.01) * sign
    elif t == 6:
        data["holdings"][1]["holdings"][0]["actual"] += 0.01 * sign
    elif t == 7:
        data["holdings"][1]["holdings"][0]["target"] += 0.01 * sign
    elif t == 8:
        data["holdings"][1]["holdings"][0]["value"] += 1.01 * sign

    portfolio_status.set_from_response(data)
    assert not portfolio_status.is_valid()
