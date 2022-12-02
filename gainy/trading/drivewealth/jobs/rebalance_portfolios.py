from typing import Iterable, Tuple

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthRepository
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.models import TradingCollectionVersion, TradingCollectionVersionStatus
from gainy.utils import get_logger

logger = get_logger(__name__)


class RebalancePortfoliosJob:

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider):
        self.repo = repo
        self.provider = provider

    def run(self):
        # todo thread safety

        for profile_id, trading_account_id in self._iterate_accounts_with_pending_trading_collection_versions(
        ):
            start_time = time.time()
            try:
                portfolio = self.provider.ensure_portfolio(
                    profile_id, trading_account_id)

                logger.info(
                    "Upsert portfolio %s for profile %d account %d in %fs",
                    portfolio.ref_id, profile_id, trading_account_id,
                    time.time() - start_time)
            except Exception as e:
                logger.exception(e)

        for portfolio in self.repo.iterate_all(DriveWealthPortfolio):
            try:
                self.rebalance_portfolio_cash(portfolio)
                self.apply_trading_collection_versions(portfolio)
                self.provider.send_portfolio_to_api(portfolio)
            except Exception as e:
                logger.exception(e)

    def apply_trading_collection_versions(self,
                                          portfolio: DriveWealthPortfolio):
        profile_id = portfolio.profile_id
        drivewealth_account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": portfolio.drivewealth_account_id})
        if not drivewealth_account:
            raise Exception('drivewealth_account not found')
        trading_account_id = drivewealth_account.trading_account_id

        for trading_collection_version in self.repo.iterate_trading_collection_versions(
                profile_id=profile_id,
                trading_account_id=trading_account_id,
                status=TradingCollectionVersionStatus.PENDING):
            start_time = time.time()
            try:
                self.provider.reconfigure_collection_holdings(
                    trading_collection_version)

                logger.info(
                    "Reconfigured collection holdings %s for profile %d account %d, collections %s in %fs",
                    trading_collection_version.id, profile_id,
                    trading_account_id,
                    trading_collection_version.collection_id,
                    time.time() - start_time)

                trading_collection_version.status = TradingCollectionVersionStatus.PENDING_EXECUTION
                self.repo.persist(trading_collection_version)
            except InsufficientFundsException as e:
                trading_collection_version.status = TradingCollectionVersionStatus.FAILED
                trading_collection_version.fail_reason = e.__class__.__name__
                self.repo.persist(trading_collection_version)
            except DriveWealthApiException as e:
                logger.exception(e)

    def rebalance_portfolio_cash(self, portfolio: DriveWealthPortfolio):
        start_time = time.time()

        try:
            self.provider.rebalance_portfolio_cash(portfolio)
            self.repo.persist(portfolio)
            logger.info("Rebalanced portfolio %s in %fs", portfolio.ref_id,
                        time.time() - start_time)
        except DriveWealthApiException as e:
            logger.exception(e)

    def _iterate_accounts_with_pending_trading_collection_versions(
            self) -> Iterable[Tuple[int, int]]:
        query = "select distinct profile_id, trading_account_id from app.trading_collection_versions where status = %(status)s"
        with self.repo.db_conn.cursor() as cursor:
            cursor.execute(
                query, {"status": TradingCollectionVersionStatus.PENDING.name})
            for row in cursor:
                yield row[0], row[1]


def cli():
    try:
        with ContextContainer() as context_container:
            job = RebalancePortfoliosJob(
                context_container.drivewealth_repository,
                context_container.drivewealth_provider)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
