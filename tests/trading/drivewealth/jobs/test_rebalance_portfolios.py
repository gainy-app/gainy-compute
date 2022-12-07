import datetime

from decimal import Decimal

from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls, mock_persist
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.jobs.rebalance_portfolios import RebalancePortfoliosJob
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount, DriveWealthFund
from gainy.trading.models import TradingCollectionVersion, TradingCollectionVersionStatus, TradingOrderSource
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

    portfolio1 = DriveWealthPortfolio()
    portfolio2 = DriveWealthPortfolio()

    repository = DriveWealthRepository(None)

    provider = DriveWealthProvider(repository, None, None)
    send_portfolio_to_api_calls = []
    monkeypatch.setattr(provider, "send_portfolio_to_api",
                        mock_record_calls(send_portfolio_to_api_calls))
    ensure_portfolio_profile_ids = []
    monkeypatch.setattr(
        provider, "ensure_portfolio",
        mock_ensure_portfolio(portfolio1, ensure_portfolio_profile_ids))

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "iterate_all",
        mock_find([(DriveWealthPortfolio, None, [portfolio1, portfolio2])]))

    job = RebalancePortfoliosJob(trading_repository, provider, None)
    monkeypatch.setattr(
        job, "_iterate_accounts_with_pending_trading_collection_versions",
        lambda: [(profile_id1, trading_account_id_1)])

    rebalance_portfolio_cash_calls = []
    apply_trading_collection_versions_calls = []
    rebalance_existing_collection_funds_calls = []
    monkeypatch.setattr(job, "rebalance_portfolio_cash",
                        mock_record_calls(rebalance_portfolio_cash_calls))
    monkeypatch.setattr(
        job, "apply_trading_collection_versions",
        mock_record_calls(apply_trading_collection_versions_calls))
    monkeypatch.setattr(
        job, "rebalance_existing_collection_funds",
        mock_record_calls(rebalance_existing_collection_funds_calls))

    job.run()

    assert (profile_id1, trading_account_id_1) in ensure_portfolio_profile_ids
    assert portfolio1 in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert portfolio2 in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert portfolio1 in [
        args[0] for args, kwargs in apply_trading_collection_versions_calls
    ]
    assert portfolio2 in [
        args[0] for args, kwargs in apply_trading_collection_versions_calls
    ]
    assert portfolio1 in [
        args[0] for args, kwargs in rebalance_existing_collection_funds_calls
    ]
    assert portfolio2 in [
        args[0] for args, kwargs in rebalance_existing_collection_funds_calls
    ]
    assert portfolio1 in [
        args[0] for args, kwargs in send_portfolio_to_api_calls
    ]
    assert portfolio2 in [
        args[0] for args, kwargs in send_portfolio_to_api_calls
    ]


def test_apply_trading_collection_versions(monkeypatch):
    profile_id = 1
    trading_account_id = 1

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
            "status": TradingCollectionVersionStatus.PENDING.name
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

    provider = DriveWealthProvider(None, None, None)
    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    RebalancePortfoliosJob(repository, provider,
                           None).apply_trading_collection_versions(portfolio)

    assert (portfolio, trading_collection_version) in [
        args for args, kwargs in reconfigure_collection_holdings_calls
    ]
    assert trading_collection_version.status == TradingCollectionVersionStatus.PENDING_EXECUTION
    assert trading_collection_version in persisted_objects[
        TradingCollectionVersion]


def test_rebalance_existing_collection_funds(monkeypatch):
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

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "ref_id", fund_ref_id)
    monkeypatch.setattr(fund, "collection_id", collection_id)
    monkeypatch.setattr(fund, "trading_collection_version_id",
                        trading_collection_version_id)

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
            "last_optimization_at": collection_last_optimization_at
        } == kwargs
        return new_trading_collection_version

    monkeypatch.setattr(trading_service, "create_collection_version",
                        mock_create_collection_version)

    provider = DriveWealthProvider(None, None, None)

    def mock_iterate_profile_funds(_profile_id):
        assert _profile_id == profile_id
        return [fund]

    monkeypatch.setattr(provider, "iterate_profile_funds",
                        mock_iterate_profile_funds)

    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    job = RebalancePortfoliosJob(repository, provider, trading_service)

    def mock_get_trading_account_id(_portfolio):
        assert _portfolio == portfolio
        return trading_account_id

    monkeypatch.setattr(job, "_get_trading_account_id",
                        mock_get_trading_account_id)

    job.rebalance_existing_collection_funds(portfolio)

    assert (portfolio, new_trading_collection_version) in [
        args for args, kwargs in reconfigure_collection_holdings_calls
    ]


def test_rebalance_portfolio_cash(monkeypatch):
    portfolio = DriveWealthPortfolio()

    repository = DriveWealthRepository(None)

    def mock_get_profile_portfolio(profile_id):
        if profile_id == profile_id:
            return portfolio
        raise Exception(f"unknown profile_id {profile_id}")

    monkeypatch.setattr(repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)

    provider = DriveWealthProvider(repository, None, None)
    rebalance_portfolio_cash_calls = []
    monkeypatch.setattr(provider, "rebalance_portfolio_cash",
                        mock_record_calls(rebalance_portfolio_cash_calls))

    trading_repository = TradingRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(trading_repository, "persist",
                        mock_persist(persisted_objects))
    RebalancePortfoliosJob(trading_repository, provider,
                           None).rebalance_portfolio_cash(portfolio)

    assert portfolio in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert portfolio in persisted_objects[DriveWealthPortfolio]
