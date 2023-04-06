import datetime

from decimal import Decimal

import pytest

from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls, mock_persist, mock_noop
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.jobs.rebalance_portfolios import RebalancePortfoliosJob
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolioStatus, DriveWealthPortfolioStatusHolding
from gainy.trading.models import TradingCollectionVersion, TradingOrderStatus, TradingOrderSource, TradingOrder
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService


def mock_ensure_portfolio(portfolio, profile_ids: list = None):

    def mock(profile_id, trading_account_id):
        if profile_ids is not None:
            profile_ids.append((profile_id, trading_account_id))

        return portfolio

    return mock


def mock_send_portfolio_to_api(portfolios: list = None):

    def mock(portfolio):
        if portfolios is None:
            return

        portfolios.append(portfolio)

    return mock


def test_rebalance_portfolios(monkeypatch):
    profile_id1 = 1
    trading_account_id_1 = 3
    drivewealth_account_id1 = "drivewealth_account_id1"
    drivewealth_account_id2 = "drivewealth_account_id2"
    is_pending_rebalance = True
    portfolio_changed = False

    account1 = DriveWealthAccount()
    monkeypatch.setattr(account1, "is_open", lambda: True)
    account2 = DriveWealthAccount()
    monkeypatch.setattr(account2, "is_open", lambda: True)

    portfolio1 = DriveWealthPortfolio()
    portfolio1.drivewealth_account_id = drivewealth_account_id1
    portfolio1.cash_target_weight = Decimal(1)
    portfolio1.holdings = {}
    portfolio2 = DriveWealthPortfolio()
    portfolio2.drivewealth_account_id = drivewealth_account_id2
    portfolio2.cash_target_weight = Decimal(1)
    portfolio2.holdings = {}

    portfolio_status = DriveWealthPortfolioStatus()
    monkeypatch.setattr(portfolio_status, "is_pending_rebalance",
                        lambda: is_pending_rebalance)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, "portfolio_has_pending_orders",
                        lambda p: True)

    provider = DriveWealthProvider(repository, None, None, None)
    send_portfolio_to_api_calls = []
    monkeypatch.setattr(provider, "send_portfolio_to_api",
                        mock_record_calls(send_portfolio_to_api_calls))
    ensure_portfolio_profile_ids = []
    monkeypatch.setattr(
        provider, "ensure_portfolio",
        mock_ensure_portfolio(portfolio1, ensure_portfolio_profile_ids))

    def mock_sync_portfolio_status(_portfolio, force=None):
        return portfolio_status

    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_sync_portfolio_status)

    def mock_actualize_portfolio(_portfolio, _portfolio_status):
        return portfolio_changed

    monkeypatch.setattr(provider, "actualize_portfolio",
                        mock_actualize_portfolio)

    sync_portfolio_calls = []
    monkeypatch.setattr(provider, "sync_portfolio",
                        mock_record_calls(sync_portfolio_calls))

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "iterate_all",
        mock_find([(DriveWealthPortfolio, None, [portfolio1, portfolio2])]))
    monkeypatch.setattr(trading_repository, "commit", mock_noop)
    monkeypatch.setattr(
        trading_repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "ref_id": drivewealth_account_id1
            }, account1),
            (DriveWealthAccount, {
                "ref_id": drivewealth_account_id2
            }, account2),
        ]))

    job = RebalancePortfoliosJob(trading_repository, repository, provider,
                                 None)
    monkeypatch.setattr(
        job, "_iterate_accounts_with_pending_trading_collection_versions",
        lambda: [(profile_id1, trading_account_id_1)])

    apply_trading_collection_versions_calls = []
    apply_trading_orders_calls = []
    rebalance_existing_funds_calls = []
    force_rebalance_calls = []
    automatic_sell_calls = []
    monkeypatch.setattr(
        job, "apply_trading_collection_versions",
        mock_record_calls(apply_trading_collection_versions_calls))
    monkeypatch.setattr(job, "apply_trading_orders",
                        mock_record_calls(apply_trading_orders_calls))
    monkeypatch.setattr(job, "rebalance_existing_funds",
                        mock_record_calls(rebalance_existing_funds_calls))
    monkeypatch.setattr(job, "_force_rebalance",
                        mock_record_calls(force_rebalance_calls))
    monkeypatch.setattr(job, "automatic_sell",
                        mock_record_calls(automatic_sell_calls))

    job.run()

    assert ((portfolio1, ), {}) in sync_portfolio_calls
    assert ((portfolio2, ), {}) in sync_portfolio_calls

    assert (profile_id1, trading_account_id_1) in ensure_portfolio_profile_ids

    calls_args = [
        args for args, kwargs in apply_trading_collection_versions_calls
    ]
    assert (portfolio1, is_pending_rebalance) in calls_args
    assert (portfolio2, is_pending_rebalance) in calls_args

    calls_args = [args for args, kwargs in apply_trading_orders_calls]
    assert (portfolio1, is_pending_rebalance) in calls_args
    assert (portfolio2, is_pending_rebalance) in calls_args

    calls_args = [args for args, kwargs in rebalance_existing_funds_calls]
    assert (portfolio1, is_pending_rebalance) in calls_args
    assert (portfolio2, is_pending_rebalance) in calls_args

    # no orders - no api calls
    calls_args = [args for args, kwargs in send_portfolio_to_api_calls]
    assert (portfolio1, ) not in calls_args
    assert (portfolio2, ) not in calls_args

    calls_args = [args for args, kwargs in force_rebalance_calls]
    assert (portfolio1, ) in calls_args
    assert (portfolio2, ) in calls_args

    calls_args = [args for args, kwargs in automatic_sell_calls]
    assert (
        portfolio1,
        portfolio_status,
        is_pending_rebalance,
    ) in calls_args
    assert (
        portfolio2,
        portfolio_status,
        is_pending_rebalance,
    ) in calls_args


def test_apply_trading_collection_versions(monkeypatch):
    profile_id = 1
    trading_account_id = 1
    is_pending_rebalance = True

    portfolio = DriveWealthPortfolio()
    monkeypatch.setattr(portfolio, "profile_id", profile_id)

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "trading_account_id", trading_account_id)

    trading_collection_version = TradingCollectionVersion()

    repository = TradingRepository(None)
    monkeypatch.setattr(
        repository, "iterate_all",
        mock_find([(TradingCollectionVersion, {
            "profile_id": profile_id,
            "trading_account_id": trading_account_id,
            "status": TradingOrderStatus.PENDING.name
        }, [trading_collection_version])]))
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "ref_id": portfolio.drivewealth_account_id
            }, account),
        ]))
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None)
    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    RebalancePortfoliosJob(repository, None, provider,
                           None).apply_trading_collection_versions(
                               portfolio, is_pending_rebalance)

    assert (portfolio, trading_collection_version, is_pending_rebalance) in [
        args for args, kwargs in reconfigure_collection_holdings_calls
    ]
    assert trading_collection_version in persisted_objects[
        TradingCollectionVersion]


def test_apply_trading_orders(monkeypatch):
    profile_id = 1
    trading_account_id = 1
    is_pending_rebalance = True

    portfolio = DriveWealthPortfolio()
    monkeypatch.setattr(portfolio, "profile_id", profile_id)

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "trading_account_id", trading_account_id)

    trading_order = TradingOrder()

    repository = TradingRepository(None)
    monkeypatch.setattr(
        repository, "iterate_all",
        mock_find([(TradingOrder, {
            "profile_id": profile_id,
            "trading_account_id": trading_account_id,
            "status": TradingOrderStatus.PENDING.name
        }, [trading_order])]))
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "ref_id": portfolio.drivewealth_account_id
            }, account),
        ]))
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(None, None, None, None)
    execute_order_in_portfolio_calls = []
    monkeypatch.setattr(provider, "execute_order_in_portfolio",
                        mock_record_calls(execute_order_in_portfolio_calls))

    RebalancePortfoliosJob(repository, None, provider,
                           None).apply_trading_orders(portfolio,
                                                      is_pending_rebalance)

    assert (portfolio, trading_order, is_pending_rebalance) in [
        args for args, kwargs in execute_order_in_portfolio_calls
    ]
    assert trading_order in persisted_objects[TradingOrder]


def test_rebalance_existing_funds(monkeypatch):
    profile_id = 1
    fund_ref_id = 'fund_ref_id'
    fund_weight = Decimal(0.1)
    collection_id = 2
    trading_collection_version_id = 3
    symbol = "AAPL"
    weights = [{"symbol": symbol, "weight": Decimal(1)}]
    collection_last_optimization_at = datetime.date.today()
    last_optimization_at = collection_last_optimization_at - datetime.timedelta(
        days=1)
    trading_account_id = 4
    is_pending_rebalance = True

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", fund_ref_id)
    monkeypatch.setattr(fund, "collection_id", collection_id)
    monkeypatch.setattr(fund, "trading_collection_version_id",
                        trading_collection_version_id)
    monkeypatch.setattr(fund, "weights",
                        {i["symbol"]: i["weight"]
                         for i in weights})

    trading_collection_version = TradingCollectionVersion()
    monkeypatch.setattr(trading_collection_version, "last_optimization_at",
                        last_optimization_at)
    new_trading_collection_version = TradingCollectionVersion()

    portfolio = DriveWealthPortfolio()
    monkeypatch.setattr(portfolio, "profile_id", profile_id)

    def mock_get_fund_weight(_fund_ref_id):
        assert _fund_ref_id == fund_ref_id
        return fund_weight

    monkeypatch.setattr(portfolio, "get_fund_weight", mock_get_fund_weight)

    repository = TradingRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (TradingCollectionVersion, {
                "id": fund.trading_collection_version_id
            }, trading_collection_version),
        ]))

    def mock_get_collection_actual_weights(_collection_id):
        assert _collection_id == collection_id
        return weights, collection_last_optimization_at

    monkeypatch.setattr(repository, "get_collection_actual_weights",
                        mock_get_collection_actual_weights)

    trading_service = TradingService(None, None, None)

    def mock_create_collection_version(*args, **kwargs):
        assert (profile_id, TradingOrderSource.AUTOMATIC, fund.collection_id,
                trading_account_id) == args
        assert {
            "weights": weights,
            "target_amount_delta_relative": None,
            "last_optimization_at": collection_last_optimization_at
        } == kwargs
        return new_trading_collection_version

    monkeypatch.setattr(trading_service, "create_collection_version",
                        mock_create_collection_version)

    provider = DriveWealthProvider(None, None, None, None)

    def mock_iterate_profile_funds(_profile_id):
        assert _profile_id == profile_id
        return [fund]

    monkeypatch.setattr(provider, "iterate_profile_funds",
                        mock_iterate_profile_funds)

    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    job = RebalancePortfoliosJob(repository, None, provider, trading_service)

    def mock_get_trading_account_id(_portfolio):
        assert _portfolio == portfolio
        return trading_account_id

    monkeypatch.setattr(job, "_get_trading_account_id",
                        mock_get_trading_account_id)

    job.rebalance_existing_funds(portfolio, is_pending_rebalance)

    assert (portfolio, new_trading_collection_version,
            is_pending_rebalance) in [
                args for args, kwargs in reconfigure_collection_holdings_calls
            ]


def get_test_automatic_sell_is_pending_rebalance():
    return [True, False]


@pytest.mark.parametrize("is_pending_rebalance",
                         get_test_automatic_sell_is_pending_rebalance())
def test_automatic_sell(monkeypatch, is_pending_rebalance):
    profile_id = 1
    fund1_ref_id = 'fund1_ref_id'
    fund1_actual_weight = Decimal(0.1)
    fund1_target_weight = Decimal(0.3)
    fund2_ref_id = 'fund2_ref_id'
    fund2_actual_weight = Decimal(0.2)
    fund2_target_weight = Decimal(0.4)

    collection_id = 2
    symbol = "AAPL"
    amount_to_sell = Decimal(3)
    trading_account_id = 4

    if is_pending_rebalance:
        weight_sum = fund1_target_weight + fund2_target_weight
        order1_amount = -amount_to_sell * fund1_target_weight / weight_sum
        order2_amount = -amount_to_sell * fund2_target_weight / weight_sum
    else:
        weight_sum = fund1_actual_weight + fund2_actual_weight
        order1_amount = -amount_to_sell * fund1_actual_weight / weight_sum
        order2_amount = -amount_to_sell * fund2_actual_weight / weight_sum

    trading_collection_version = TradingCollectionVersion()
    trading_order = TradingOrder()

    portfolio_status = DriveWealthPortfolioStatus()
    monkeypatch.setattr(
        portfolio_status, "holdings", {
            fund1_ref_id:
            DriveWealthPortfolioStatusHolding({"actual": fund1_actual_weight}),
            fund2_ref_id:
            DriveWealthPortfolioStatusHolding({"actual": fund2_actual_weight}),
        })
    portfolio = DriveWealthPortfolio()
    monkeypatch.setattr(portfolio, "profile_id", profile_id)

    fund1 = DriveWealthFund()
    fund1.collection_id = collection_id
    fund2 = DriveWealthFund()
    fund2.symbol = symbol

    def mock_get_fund_weight(_fund_ref_id):
        if _fund_ref_id == fund1_ref_id:
            return fund1_target_weight
        if _fund_ref_id == fund2_ref_id:
            return fund2_target_weight
        assert False, 'fund not found'

    monkeypatch.setattr(portfolio, "get_fund_weight", mock_get_fund_weight)

    repository = TradingRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthFund, {
                "ref_id": fund1_ref_id
            }, fund1),
            (DriveWealthFund, {
                "ref_id": fund2_ref_id
            }, fund2),
        ]))

    def mock_get_buying_power_minus_pending_fees(_profile_id):
        assert _profile_id == profile_id
        return -amount_to_sell

    monkeypatch.setattr(repository, "get_buying_power_minus_pending_fees",
                        mock_get_buying_power_minus_pending_fees)

    trading_service = TradingService(None, None, None)

    def mock_create_collection_version(*args, **kwargs):
        assert (profile_id, TradingOrderSource.AUTOMATIC, collection_id,
                trading_account_id) == args
        assert {
            "target_amount_delta": order1_amount,
        } == kwargs
        return trading_collection_version

    monkeypatch.setattr(trading_service, "create_collection_version",
                        mock_create_collection_version)

    def mock_create_stock_order(*args, **kwargs):
        assert (profile_id, TradingOrderSource.AUTOMATIC, symbol,
                trading_account_id) == args
        assert {
            "target_amount_delta": order2_amount,
        } == kwargs
        return trading_order

    monkeypatch.setattr(trading_service, "create_stock_order",
                        mock_create_stock_order)
    #
    provider = DriveWealthProvider(None, None, None, None)

    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    execute_order_in_portfolio_calls = []
    monkeypatch.setattr(provider, "execute_order_in_portfolio",
                        mock_record_calls(execute_order_in_portfolio_calls))

    job = RebalancePortfoliosJob(repository, None, provider, trading_service)

    pending_sell_orders_exist_calls = []

    def mock_pending_sell_orders_exist(*args, **kwargs):
        mock_record_calls(pending_sell_orders_exist_calls)(*args, **kwargs)
        return False

    monkeypatch.setattr(job, "_pending_sell_orders_exist",
                        mock_pending_sell_orders_exist)

    def mock_get_trading_account_id(_portfolio):
        assert _portfolio == portfolio
        return trading_account_id

    monkeypatch.setattr(job, "_get_trading_account_id",
                        mock_get_trading_account_id)

    job.automatic_sell(portfolio, portfolio_status, is_pending_rebalance)

    assert ((profile_id, ), {}) in pending_sell_orders_exist_calls

    assert ((portfolio, trading_collection_version, is_pending_rebalance),
            {}) in reconfigure_collection_holdings_calls
    assert ((portfolio, trading_order, True),
            {}) in execute_order_in_portfolio_calls
