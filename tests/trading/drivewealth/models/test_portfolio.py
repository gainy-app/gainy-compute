from decimal import Decimal

import pytest

from gainy.tests.mocks.trading.drivewealth.api_mocks import PORTFOLIO, CASH_TARGET_WEIGHT

from gainy.trading.drivewealth.models import DriveWealthPortfolio, PRECISION


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

    assert abs(portfolio.cash_target_weight - (CASH_TARGET_WEIGHT + weight_delta)) < PRECISION
    for fund_ref_id in portfolio.holdings.keys():
        fund_weight = portfolio.get_fund_weight(fund_ref_id)
        if weight_sum < PRECISION:
            assert abs(fund_weight) < PRECISION
        else:
            assert abs(fund_relative_weight[fund_ref_id] - fund_weight / weight_sum) < PRECISION
