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
        self.apply_trading_collection_versions()
        self.rebalance_portfolio_cash()

    def apply_trading_collection_versions(self):
        for profile_id, trading_collection_versions in self._iterate_trading_collection_versions_by_profile_id(
        ):
            start_time = time.time()
            try:
                self.provider.reconfigure_collection_holdings(
                    trading_collection_versions)

                logger.info(
                    "Reconfigured collection holdings %s for profiles %s, collections %s in %fs",
                    list(map(lambda x: x.id, trading_collection_versions)),
                    list(
                        map(lambda x: x.profile_id,
                            trading_collection_versions)),
                    list(
                        map(lambda x: x.collection_id,
                            trading_collection_versions)),
                    time.time() - start_time)

                for trading_collection_version in trading_collection_versions:
                    trading_collection_version.status = TradingCollectionVersionStatus.PENDING_EXECUTION
                self.repo.persist(trading_collection_versions)
            except DriveWealthApiException as e:
                logger.exception(e)

    def rebalance_portfolio_cash(self):
        portfolios: Iterable[DriveWealthPortfolio] = self.repo.iterate_all(
            DriveWealthPortfolio)
        for portfolio in portfolios:
            start_time = time.time()

            try:
                self.provider.rebalance_portfolio_cash(portfolio)
                logger.info("Rebalanced portfolio %s in %fs", portfolio.ref_id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)

    def _iterate_trading_collection_versions_by_profile_id(
            self) -> Iterable[Tuple[int, List[TradingCollectionVersion]]]:
        trading_collection_versions: Iterable[
            TradingCollectionVersion] = self.repo.iterate_all(
                TradingCollectionVersion,
                {"status": TradingCollectionVersionStatus.PENDING.name},
                [("profile_id", "ASC")])

        current_profile_id = None
        profile_trading_collection_versions: List[
            TradingCollectionVersion] = []
        for trading_collection_version in trading_collection_versions:
            if current_profile_id and trading_collection_version.profile_id != current_profile_id:
                yield current_profile_id, profile_trading_collection_versions
                profile_trading_collection_versions = []

            current_profile_id = trading_collection_version.profile_id
            profile_trading_collection_versions.append(
                trading_collection_version)

        if current_profile_id and profile_trading_collection_versions:
            yield current_profile_id, profile_trading_collection_versions


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
