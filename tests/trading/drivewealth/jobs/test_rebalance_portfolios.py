from gainy.tests.mocks.repository_mocks import mock_find, mock_record_calls, mock_persist
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.jobs.rebalance_portfolios import RebalancePortfoliosJob
from gainy.trading.drivewealth.models import DriveWealthPortfolio
from gainy.trading.models import TradingCollectionVersion, TradingCollectionVersionStatus


def mock_ensure_portfolio(portfolio, profile_ids: list = None):

    def mock(profile_id):
        if profile_ids is not None:
            profile_ids.append(profile_id)

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
    profile_id2 = 2

    portfolio1 = DriveWealthPortfolio()
    portfolio2 = DriveWealthPortfolio()

    def mock_get_profile_portfolio(profile_id):
        if profile_id == profile_id1:
            return portfolio1
        if profile_id == profile_id2:
            return portfolio2
        raise Exception(f"unknown profile_id {profile_id}")

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository,
        "iterate_profiles_with_pending_trading_collection_versions",
        lambda: [profile_id1])
    monkeypatch.setattr(repository, "iterate_profiles_with_portfolio",
                        lambda: [profile_id1, profile_id2])
    monkeypatch.setattr(repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)

    provider = DriveWealthProvider(repository, None)
    send_portfolio_to_api_calls = []
    monkeypatch.setattr(provider, "send_portfolio_to_api",
                        mock_record_calls(send_portfolio_to_api_calls))
    ensure_portfolio_profile_ids = []
    monkeypatch.setattr(
        provider, "ensure_portfolio",
        mock_ensure_portfolio(portfolio1, ensure_portfolio_profile_ids))

    job = RebalancePortfoliosJob(repository, provider)

    rebalance_portfolio_cash_calls = []
    apply_trading_collection_versions_calls = []
    monkeypatch.setattr(job, "rebalance_portfolio_cash",
                        mock_record_calls(rebalance_portfolio_cash_calls))
    monkeypatch.setattr(
        job, "apply_trading_collection_versions",
        mock_record_calls(apply_trading_collection_versions_calls))

    job.run()

    assert profile_id1 in ensure_portfolio_profile_ids
    assert profile_id1 in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert profile_id2 in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert profile_id1 in [
        args[0] for args, kwargs in apply_trading_collection_versions_calls
    ]
    assert profile_id2 in [
        args[0] for args, kwargs in apply_trading_collection_versions_calls
    ]
    assert portfolio1 in [
        args[0] for args, kwargs in send_portfolio_to_api_calls
    ]
    assert portfolio2 in [
        args[0] for args, kwargs in send_portfolio_to_api_calls
    ]


def test_apply_trading_collection_versions(monkeypatch):
    profile_id = 1

    trading_collection_version = TradingCollectionVersion()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "iterate_all",
        mock_find([(TradingCollectionVersion, {
            "profile_id": profile_id,
            "status": TradingCollectionVersionStatus.PENDING.name
        }, [trading_collection_version])]))
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None)
    reconfigure_collection_holdings_calls = []
    monkeypatch.setattr(
        provider, "reconfigure_collection_holdings",
        mock_record_calls(reconfigure_collection_holdings_calls))

    RebalancePortfoliosJob(
        repository, provider).apply_trading_collection_versions(profile_id)

    assert trading_collection_version in [
        args[0] for args, kwargs in reconfigure_collection_holdings_calls
    ]
    assert trading_collection_version.status == TradingCollectionVersionStatus.PENDING_EXECUTION
    assert trading_collection_version in persisted_objects[
        TradingCollectionVersion]


def test_rebalance_portfolio_cash(monkeypatch):
    profile_id = 1
    portfolio = DriveWealthPortfolio()

    repository = DriveWealthRepository(None)

    def mock_get_profile_portfolio(profile_id):
        if profile_id == profile_id:
            return portfolio
        raise Exception(f"unknown profile_id {profile_id}")

    monkeypatch.setattr(repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None)
    rebalance_portfolio_cash_calls = []
    monkeypatch.setattr(provider, "rebalance_portfolio_cash",
                        mock_record_calls(rebalance_portfolio_cash_calls))

    RebalancePortfoliosJob(repository,
                           provider).rebalance_portfolio_cash(profile_id)

    assert portfolio in [
        args[0] for args, kwargs in rebalance_portfolio_cash_calls
    ]
    assert portfolio in persisted_objects[DriveWealthPortfolio]
