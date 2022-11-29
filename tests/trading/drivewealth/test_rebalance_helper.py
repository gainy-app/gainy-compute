from decimal import Decimal

import pytest

from gainy.exceptions import EntityNotFoundException
from gainy.tests.mocks.repository_mocks import mock_noop, mock_find
from gainy.tests.mocks.trading.drivewealth.api_mocks import CASH_TARGET_WEIGHT, FUND1_ID, FUND1_TARGET_WEIGHT, \
    PORTFOLIO_STATUS, CASH_VALUE, FUND1_VALUE, USER_ID, PORTFOLIO
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.drivewealth.api import DriveWealthApi
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthInstrumentStatus, DriveWealthInstrument, \
    DriveWealthPortfolioStatus, PRECISION
from gainy.trading.drivewealth.provider import DriveWealthProvider
from gainy.trading.models import TradingCollectionVersion

from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthPortfolio, DriveWealthFund

_FUND_WEIGHTS = {
    "symbol_B": Decimal(0.3),
    "symbol_C": Decimal(0.7),
}
_FUND_HOLDINGS = [{
    "instrumentID": "A",
    "target": 0.6
}, {
    "instrumentID": "B",
    "target": 0.4
}]
_NEW_FUND_HOLDINGS = [
    {
        "instrumentID": "A",
        "target": 0,
    },
    {
        "instrumentID": "B",
        "target": 0.3,
    },
    {
        "instrumentID": "C",
        "target": 0.7,
    },
]


def _mock_get_instrument(monkeypatch, service):
    instrument_B = DriveWealthInstrument()
    monkeypatch.setattr(instrument_B, 'ref_id', 'B')
    instrument_C = DriveWealthInstrument()
    monkeypatch.setattr(instrument_C, 'ref_id', 'C')

    def mock_get_instrument(symbol):
        instruments = {
            "symbol_B": instrument_B,
            "symbol_C": instrument_C,
        }
        return instruments[symbol]

    monkeypatch.setattr(service, "_get_instrument", mock_get_instrument)


def get_test_upsert_fund_fund_exists():
    return [False, True]


@pytest.mark.parametrize("fund_exists", get_test_upsert_fund_fund_exists())
def test_upsert_fund(fund_exists, monkeypatch):
    profile_id = 1
    collection_id = 2
    trading_collection_version_id = 3
    weights = _FUND_WEIGHTS
    fund_ref_id = "fund_dff726ff-f213-42b1-a759-b20efa3f56d7"

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", USER_ID)

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", fund_ref_id)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    def mock_get_profile_fund(_profile_id, _collection_id):
        assert _profile_id == profile_id
        assert _collection_id == collection_id
        return fund if fund_exists else None

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(drivewealth_repository, "get_profile_fund",
                        mock_get_profile_fund)

    data = {"id": fund_ref_id, "userID": USER_ID, "holdings": _FUND_HOLDINGS}

    new_fund_holdings = [
        {
            "instrumentID": "B",
            "target": round(Decimal(0.3), 4),
        },
        {
            "instrumentID": "C",
            "target": round(Decimal(0.7), 4),
        },
    ]

    api = DriveWealthApi(None)
    if fund_exists:

        def mock_update_fund(_fund):
            assert _fund == fund
            assert _fund.holdings == new_fund_holdings
            _fund.set_from_response(data)

        monkeypatch.setattr(api, "update_fund", mock_update_fund)
    else:

        def mock_create_fund(_fund, _name, _client_fund_id, _description):
            assert _client_fund_id == f"{profile_id}_{collection_id}"
            assert _fund.holdings == new_fund_holdings
            _fund.set_from_response(data)

        monkeypatch.setattr(api, "create_fund", mock_create_fund)

    collection_version = TradingCollectionVersion()
    collection_version.profile_id = profile_id
    collection_version.collection_id = collection_id
    collection_version.weights = weights
    collection_version.target_amount_delta = Decimal(0)

    monkeypatch.setattr(collection_version, "id",
                        trading_collection_version_id)
    monkeypatch.setattr(collection_version, "collection_id", collection_id)
    monkeypatch.setattr(collection_version, "weights", weights)

    provider = DriveWealthProvider(drivewealth_repository, api)
    helper = DriveWealthProviderRebalanceHelper(provider)
    _mock_get_instrument(monkeypatch, helper)
    fund = helper.upsert_fund(profile_id, collection_version)

    assert fund.ref_id == fund_ref_id
    assert fund.collection_id == collection_id
    assert fund.trading_collection_version_id == trading_collection_version_id
    assert fund.weights == weights
    assert fund.data == data


def test_generate_new_fund_holdings(monkeypatch):
    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    provider = DriveWealthProvider(drivewealth_repository, api)
    helper = DriveWealthProviderRebalanceHelper(provider)
    fund = DriveWealthFund()
    monkeypatch.setattr(DriveWealthFund, "holdings", _FUND_HOLDINGS)

    _mock_get_instrument(monkeypatch, helper)

    new_holdings = helper._generate_new_fund_holdings(_FUND_WEIGHTS, fund)
    new_holdings = {i["instrumentID"]: i["target"] for i in new_holdings}

    assert "A" in new_holdings
    assert abs(new_holdings["A"]) < PRECISION

    assert "B" in new_holdings
    assert abs(new_holdings["B"] - Decimal(0.3)) < PRECISION

    assert "C" in new_holdings
    assert abs(new_holdings["C"] - Decimal(0.7)) < PRECISION


def get_test_handle_cash_amount_change_amounts_ok():
    yield from [100, CASH_VALUE - 100, CASH_VALUE]
    yield 0
    yield from [-100, -CASH_VALUE, -FUND1_VALUE + 100, -FUND1_VALUE]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ok())
def test_handle_cash_amount_change_ok(amount, monkeypatch):
    amount = Decimal(amount)
    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)
    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    provider = DriveWealthProvider(drivewealth_repository, None)

    def mock_sync_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        status = DriveWealthPortfolioStatus()
        status.set_from_response(PORTFOLIO_STATUS)
        return status

    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_sync_portfolio_status)

    helper = DriveWealthProviderRebalanceHelper(provider)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)

    assert abs(portfolio.cash_target_weight - CASH_TARGET_WEIGHT) < 1e-3
    assert abs(portfolio.get_fund_weight(FUND1_ID) -
               FUND1_TARGET_WEIGHT) < 1e-3

    helper.handle_cash_amount_change(amount, portfolio, fund)

    if amount:
        assert abs(portfolio.cash_target_weight -
                   (CASH_VALUE - amount) / PORTFOLIO_STATUS['equity']) < 1e-3
        assert abs(
            portfolio.get_fund_weight(FUND1_ID) -
            (FUND1_VALUE + amount) / PORTFOLIO_STATUS['equity']) < 1e-3
    else:
        assert abs(portfolio.cash_target_weight - CASH_TARGET_WEIGHT) < 1e-3
        assert abs(portfolio.get_fund_weight(FUND1_ID) -
                   FUND1_TARGET_WEIGHT) < 1e-3


def get_test_handle_cash_amount_change_amounts_ko():
    return [CASH_VALUE + 1, -FUND1_VALUE - 1]


@pytest.mark.parametrize("amount",
                         get_test_handle_cash_amount_change_amounts_ko())
def test_handle_cash_amount_change_ko(amount, monkeypatch):
    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    provider = DriveWealthProvider(drivewealth_repository, None)

    def mock_sync_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        status = DriveWealthPortfolioStatus()
        status.set_from_response(PORTFOLIO_STATUS)
        return status

    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_sync_portfolio_status)

    helper = DriveWealthProviderRebalanceHelper(provider)
    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", FUND1_ID)

    with pytest.raises(InsufficientFundsException) as error_info:
        helper.handle_cash_amount_change(amount, portfolio, fund)
        assert error_info.__class__ == InsufficientFundsException


def get_test_upsert_fund_instrument_exists():
    return [False, True]


@pytest.mark.parametrize("instrument_exists",
                         get_test_upsert_fund_instrument_exists())
def test_get_instrument(instrument_exists, monkeypatch):
    _symbol = "symbol"
    drivewealth_repository = DriveWealthRepository(None)

    provider = DriveWealthProvider(drivewealth_repository, None)
    helper = DriveWealthProviderRebalanceHelper(provider)
    instrument = DriveWealthInstrument()

    if instrument_exists:
        monkeypatch.setattr(drivewealth_repository, 'get_instrument_by_symbol',
                            lambda x: instrument)
        assert helper._get_instrument(_symbol) == instrument

    else:
        monkeypatch.setattr(drivewealth_repository, 'get_instrument_by_symbol',
                            lambda x: None)

        with pytest.raises(EntityNotFoundException):
            helper._get_instrument(_symbol)
