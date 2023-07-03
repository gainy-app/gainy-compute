import logging

import copy

import pytest
from _decimal import Decimal

from gainy.tests.mocks.trading.drivewealth.api_mocks import PORTFOLIO_STATUS, CASH_ACTUAL_VALUE, \
    FUND1_ID, FUND2_ID, FUND2_ACTUAL_VALUE, FUND1_ACTUAL_VALUE

from gainy.trading.drivewealth.models import DriveWealthPortfolioStatus, WEIGHT_ERROR_THRESHOLD


def test_set_from_response(monkeypatch):
    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)
    assert portfolio_status.cash_value == CASH_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND1_ID) == FUND1_ACTUAL_VALUE
    assert portfolio_status.get_fund_value(FUND2_ID) == FUND2_ACTUAL_VALUE
    assert portfolio_status.is_valid()


def get_test_invalid_data():
    for sign in [-1, 1]:
        data = copy.deepcopy(PORTFOLIO_STATUS)
        data["holdings"][0]["actual"] += (WEIGHT_ERROR_THRESHOLD +
                                          Decimal(1e-3)) * sign
        yield data

        data = copy.deepcopy(PORTFOLIO_STATUS)
        data["holdings"][1]["actual"] += (WEIGHT_ERROR_THRESHOLD +
                                          Decimal(1e-3)) * sign
        yield data

        data = copy.deepcopy(PORTFOLIO_STATUS)
        data["holdings"][1]["value"] += Decimal(1.01) * sign
        yield data

        data = copy.deepcopy(PORTFOLIO_STATUS)
        data["holdings"][1]["holdings"][0]["actual"] += float(
            WEIGHT_ERROR_THRESHOLD + Decimal(1e-3)) * sign
        yield data

        data = copy.deepcopy(PORTFOLIO_STATUS)
        data["holdings"][1]["holdings"][0]["value"] += Decimal(1.01 * sign)
        yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][0]["value"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][0]["target"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][0]["target"] = Decimal(1 + 1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][0]["actual"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][0]["actual"] = Decimal(1 + 1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][1]["holdings"][0]["value"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][1]["holdings"][0]["target"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][1]["holdings"][0]["target"] = Decimal(1 + 1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][1]["holdings"][0]["actual"] = Decimal(-1e-2)
    yield data

    data = copy.deepcopy(PORTFOLIO_STATUS)
    data["holdings"][1]["holdings"][0]["actual"] = Decimal(1 + 1e-2)
    yield data


@pytest.mark.parametrize("data", get_test_invalid_data())
def test_invalid(monkeypatch, data):
    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(data)
    logging.info(data)
    assert not portfolio_status.is_valid()
