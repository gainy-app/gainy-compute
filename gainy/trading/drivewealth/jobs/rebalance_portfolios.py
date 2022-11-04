from typing import Iterable, List, Tuple

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthApi, DriveWealthRepository
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from gainy.trading.drivewealth.models import DriveWealthPortfolio
from gainy.trading.models import TradingCollectionVersion, TradingCollectionVersionStatus
from gainy.utils import get_logger

logger = get_logger(__name__)


class RebalancePortfoliosJob:

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider, api: DriveWealthApi):
        self.repo = repo
        self.provider = provider
        self.api = api

    def run(self):
        # todo thread safety

        for profile_id in self.repo.iterate_profiles_with_pending_trading_collection_versions(
        ):
            start_time = time.time()
            try:
                portfolio = self.provider.ensure_portfolio(profile_id)

                logger.info("Upsert portfolio %s for profile %d in %fs",
                            portfolio.ref_id, profile_id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)

        for profile_id in self.repo.iterate_profiles_with_portfolio():
            self.rebalance_portfolio_cash(profile_id)
            self.apply_trading_collection_versions(profile_id)

            for portfolio in self.repo.iterate_profile_portfolios(profile_id):
                self.provider.send_portfolio_to_api(portfolio)

    def apply_trading_collection_versions(self, profile_id: int):
        for trading_collection_version in self._iterate_profile_pending_trading_collection_versions(
                profile_id):
            start_time = time.time()
            try:
                self.provider.reconfigure_collection_holdings(
                    trading_collection_version)

                logger.info(
                    "Reconfigured collection holdings %s for profile %d, collections %s in %fs",
                    trading_collection_version.id, profile_id,
                    trading_collection_version.collection_id,
                    time.time() - start_time)

                trading_collection_version.status = TradingCollectionVersionStatus.PENDING_EXECUTION
                self.repo.persist(trading_collection_version)
            except DriveWealthApiException as e:
                logger.exception(e)

    def rebalance_portfolio_cash(self, profile_id):
        for portfolio in self.repo.iterate_profile_portfolios(profile_id):
            start_time = time.time()

            try:
                self.provider.rebalance_portfolio_cash(portfolio)
                logger.info("Rebalanced portfolio %s in %fs", portfolio.ref_id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)

    def _iterate_profile_pending_trading_collection_versions(
            self, profile_id: int) -> Iterable[TradingCollectionVersion]:
        yield from self.repo.iterate_all(
            TradingCollectionVersion, {
                "profile_id": profile_id,
                "status": TradingCollectionVersionStatus.PENDING.name
            })


def cli():
    try:
        with ContextContainer() as context_container:
            job = RebalancePortfoliosJob(
                context_container.drivewealth_repository,
                context_container.drivewealth_provider,
                context_container.drivewealth_api)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
