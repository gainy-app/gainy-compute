import datetime
from decimal import Decimal

import pytest

from gainy.tests.mocks.trading.drivewealth.api_mocks import PORTFOLIO, CASH_TARGET_WEIGHT, PORTFOLIO_STATUS, \
    CASH_ACTUAL_WEIGHT, FUND2_ACTUAL_WEIGHT, FUND1_ACTUAL_WEIGHT, FUND1_ID, FUND2_ID

from gainy.trading.drivewealth.models import DriveWealthPortfolio, PRECISION, DriveWealthPortfolioStatus


def get_test_rebalance_cash_weight_deltas_ok():
    yield from [-CASH_TARGET_WEIGHT / 2, -CASH_TARGET_WEIGHT]
    yield 0
    yield from [(1 - CASH_TARGET_WEIGHT) / 2, 1 - CASH_TARGET_WEIGHT]


@pytest.mark.parametrize("weight_delta",
                         get_test_rebalance_cash_weight_deltas_ok())
def test_rebalance_cash(monkeypatch, weight_delta):
    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)

    weight_sum = Decimal(0)
    for fund_ref_id in portfolio.holdings.keys():
        fund_weight = portfolio.get_fund_weight(fund_ref_id)
        weight_sum += fund_weight
    fund_relative_weight = {}
    for fund_ref_id in portfolio.holdings.keys():
        fund_weight = portfolio.get_fund_weight(fund_ref_id)
        fund_relative_weight[fund_ref_id] = fund_weight / weight_sum

    assert abs(weight_sum + portfolio.cash_target_weight - 1) < PRECISION

    portfolio.rebalance_cash(Decimal(weight_delta))

    weight_sum = Decimal(0)
    for fund_ref_id in portfolio.holdings.keys():
        fund_weight = portfolio.get_fund_weight(fund_ref_id)
        weight_sum += fund_weight

    assert abs(weight_sum + portfolio.cash_target_weight - 1) < PRECISION

    assert abs(portfolio.cash_target_weight -
               (CASH_TARGET_WEIGHT + weight_delta)) < PRECISION
    for fund_ref_id in portfolio.holdings.keys():
        fund_weight = portfolio.get_fund_weight(fund_ref_id)
        if weight_sum < PRECISION:
            assert abs(fund_weight) < PRECISION
        else:
            assert abs(fund_relative_weight[fund_ref_id] -
                       fund_weight / weight_sum) < PRECISION


def test_update_from_status(monkeypatch):
    dt = datetime.datetime.now()

    portfolio_status = DriveWealthPortfolioStatus()
    monkeypatch.setattr(portfolio_status, 'last_portfolio_rebalance_at', dt)

    portfolio = DriveWealthPortfolio()
    portfolio.update_from_status(portfolio_status)
    assert portfolio.last_rebalance_at == dt


def test_set_target_weights_from_status_actual_weights(monkeypatch):
    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)

    portfolio = DriveWealthPortfolio()
    portfolio.set_target_weights_from_status_actual_weights(portfolio_status)
    assert portfolio.cash_target_weight == CASH_ACTUAL_WEIGHT
    assert portfolio.get_fund_weight(FUND1_ID) == FUND1_ACTUAL_WEIGHT
    assert portfolio.get_fund_weight(FUND2_ID) == FUND2_ACTUAL_WEIGHT


def test_set_pending_rebalance(monkeypatch):
    portfolio = DriveWealthPortfolio()
    assert not portfolio.is_pending_rebalance()
    portfolio.set_pending_rebalance()
    assert portfolio.is_pending_rebalance()
