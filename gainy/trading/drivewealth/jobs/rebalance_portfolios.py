import psycopg2.errors
from decimal import Decimal

import datetime
import dateutil.parser

from typing import Iterable, Tuple

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthRepository
from gainy.trading.drivewealth.exceptions import DriveWealthApiException, TradingAccountNotOpenException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount, DW_WEIGHT_THRESHOLD, \
    DriveWealthFund
from gainy.trading.exceptions import InsufficientFundsException, SymbolIsNotTradeableException
from gainy.trading.models import TradingCollectionVersion, TradingOrderStatus, TradingOrderSource, TradingOrder
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService
from gainy.utils import get_logger

logger = get_logger(__name__)


class RebalancePortfoliosJob:

    def __init__(self, trading_repository: TradingRepository,
                 drivewealth_repository: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 trading_service: TradingService):
        self.repo = trading_repository
        self.drivewealth_repository = drivewealth_repository
        self.provider = provider
        self.trading_service = trading_service

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
                    portfolio.ref_id,
                    profile_id,
                    trading_account_id,
                    time.time() - start_time,
                    extra={"profile_id": profile_id})
            except TradingAccountNotOpenException:
                pass
            except Exception as e:
                logger.exception(e)

        for portfolio in self.repo.iterate_all(DriveWealthPortfolio):
            portfolio: DriveWealthPortfolio
            if portfolio.is_artificial:
                continue

            try:
                account: DriveWealthAccount = self.repo.find_one(
                    DriveWealthAccount,
                    {"ref_id": portfolio.drivewealth_account_id})
                if not account or not account.is_open():
                    continue

                portfolio_status = self.provider.actualize_portfolio(portfolio)
                is_pending_rebalance = portfolio_status.is_pending_rebalance()

                trading_collection_versions = self.apply_trading_collection_versions(
                    portfolio, is_pending_rebalance)
                is_pending_rebalance = is_pending_rebalance or len(
                    trading_collection_versions) > 0

                trading_orders = self.apply_trading_orders(
                    portfolio, is_pending_rebalance)
                is_pending_rebalance = is_pending_rebalance or len(
                    trading_orders) > 0

                is_pending_rebalance = self.rebalance_existing_funds(
                    portfolio, is_pending_rebalance)

                portfolio.normalize_weights()
                self.provider.send_portfolio_to_api(portfolio)

                is_portfolio_pending_rebalance = self.drivewealth_repository.is_portfolio_pending_rebalance(
                    portfolio)
                logger.info("is_portfolio_pending_rebalance",
                            extra={
                                "profile_id":
                                portfolio.profile_id,
                                "portfolio_ref_id":
                                portfolio.ref_id,
                                "is_portfolio_pending_rebalance":
                                is_portfolio_pending_rebalance,
                            })
                if is_portfolio_pending_rebalance:
                    self.force_rebalance(
                        portfolio,
                        trading_collection_versions=trading_collection_versions,
                        trading_orders=trading_orders)

                self.repo.commit()
            except (psycopg2.errors.OperationalError,
                    DriveWealthApiException) as e:
                logger.exception(e)
                self.repo.rollback()
            except Exception as e:
                logger.exception(e)

    def apply_trading_orders(self, portfolio: DriveWealthPortfolio,
                             is_pending_rebalance: bool) -> list[TradingOrder]:
        profile_id = portfolio.profile_id
        trading_account_id = self._get_trading_account_id(portfolio)

        trading_orders = []
        for trading_order in self.repo.iterate_trading_orders(
                profile_id=profile_id,
                trading_account_id=trading_account_id,
                status=TradingOrderStatus.PENDING):
            start_time = time.time()

            logger.info(
                "Executing order %s for profile %d account %d, symbol %s in %fs",
                trading_order.id,
                profile_id,
                trading_account_id,
                trading_order.symbol,
                time.time() - start_time,
                extra={"profile_id": profile_id})

            try:
                self.provider.execute_order_in_portfolio(
                    portfolio, trading_order, is_pending_rebalance)
                is_pending_rebalance = True

                trading_orders.append(trading_order)
                trading_order.status = TradingOrderStatus.PENDING_EXECUTION
                trading_order.pending_execution_since = datetime.datetime.now()
                self.repo.persist(trading_order)
            except InsufficientFundsException as e:
                logger.info(
                    "Skipping order %s for profile %d account %d, symbol %s: %s",
                    trading_order.id,
                    profile_id,
                    trading_account_id,
                    trading_order.symbol,
                    e.__class__.__name__,
                    extra={"profile_id": profile_id})
                # let it stay pending until there are money on the account
                continue
            except DriveWealthApiException as e:
                logger.exception(e)

        return trading_orders

    def apply_trading_collection_versions(
            self, portfolio: DriveWealthPortfolio,
            is_pending_rebalance: bool) -> list[TradingCollectionVersion]:
        profile_id = portfolio.profile_id
        trading_account_id = self._get_trading_account_id(portfolio)

        trading_collection_versions = []

        for trading_collection_version in self.repo.iterate_trading_collection_versions(
                profile_id=profile_id,
                trading_account_id=trading_account_id,
                status=TradingOrderStatus.PENDING):
            start_time = time.time()

            logger.info(
                "Reconfiguring collection holdings %s for profile %d account %d, collections %s in %fs",
                trading_collection_version.id,
                profile_id,
                trading_account_id,
                trading_collection_version.collection_id,
                time.time() - start_time,
                extra={"profile_id": profile_id})

            try:
                self.provider.reconfigure_collection_holdings(
                    portfolio, trading_collection_version,
                    is_pending_rebalance)
                is_pending_rebalance = True

                trading_collection_versions.append(trading_collection_version)
                trading_collection_version.status = TradingOrderStatus.PENDING_EXECUTION
                trading_collection_version.pending_execution_since = datetime.datetime.now(
                )
                self.repo.persist(trading_collection_version)
            except InsufficientFundsException as e:
                logger.info(
                    "Skipping trading_collection_version %s for profile %d account %d, collections %s: %s",
                    trading_collection_version.id,
                    profile_id,
                    trading_account_id,
                    trading_collection_version.collection_id,
                    e.__class__.__name__,
                    extra={"profile_id": profile_id})
                # let it stay pending until there are money on the account
                continue
            except DriveWealthApiException as e:
                logger.exception(e)

        return trading_collection_versions

    def rebalance_existing_funds(self, portfolio: DriveWealthPortfolio,
                                 is_pending_rebalance: True) -> bool:
        """
        Automatically change portfolio weights according to the new collection weights
        :param portfolio: DriveWealthPortfolio
        :return:
        """
        profile_id = portfolio.profile_id
        try:
            for fund in self.provider.iterate_profile_funds(profile_id):
                fund_weight = portfolio.get_fund_weight(fund.ref_id)
                if fund_weight < DW_WEIGHT_THRESHOLD:
                    continue

                if fund.trading_collection_version_id:
                    result = self.rebalance_existing_collection_fund(
                        portfolio, fund, is_pending_rebalance)
                    is_pending_rebalance = is_pending_rebalance or result
                if fund.trading_order_id:
                    result = self.rebalance_existing_ticker_fund(
                        portfolio, fund, is_pending_rebalance)
                    is_pending_rebalance = is_pending_rebalance or result
        except DriveWealthApiException as e:
            logger.exception(e)

        return is_pending_rebalance

    def _iterate_accounts_with_pending_trading_collection_versions(
            self) -> Iterable[Tuple[int, int]]:
        query = "select distinct profile_id, trading_account_id from app.trading_collection_versions where status = %(status)s"
        with self.repo.db_conn.cursor() as cursor:
            cursor.execute(query, {"status": TradingOrderStatus.PENDING.name})
            for row in cursor:
                yield row[0], row[1]

    def _get_trading_account_id(self, portfolio: DriveWealthPortfolio) -> int:
        drivewealth_account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": portfolio.drivewealth_account_id})
        if not drivewealth_account:
            raise Exception('drivewealth_account not found')
        return drivewealth_account.trading_account_id

    def force_rebalance(
            self, portfolio: DriveWealthPortfolio,
            trading_collection_versions: list[TradingCollectionVersion],
            trading_orders: list[TradingOrder]):
        try:
            data = self.provider.api.create_autopilot_run(
                [portfolio.drivewealth_account_id])

            d = dateutil.parser.parse(data["created"])
            d -= datetime.timedelta(microseconds=d.microsecond)
            portfolio.waiting_rebalance_since = d
            self.repo.persist(portfolio)

            for trading_collection_version in trading_collection_versions:
                trading_collection_version.pending_execution_since = d
            self.repo.persist(trading_collection_versions)
            for trading_order in trading_orders:
                trading_order.pending_execution_since = d
            self.repo.persist(trading_orders)

            logger.info("Forced portfolio rebalance",
                        extra={
                            "portfolio_red_id": portfolio.ref_id,
                            "profile_id": portfolio.profile_id
                        })
        except DriveWealthApiException as e:
            logger.info("Failed to force portfolio rebalance",
                        extra={
                            "profile_id": portfolio.profile_id,
                            "e": e,
                        })
            pass

    def rebalance_existing_collection_fund(self,
                                           portfolio: DriveWealthPortfolio,
                                           fund: DriveWealthFund,
                                           is_pending_rebalance: bool) -> bool:
        logging_extra = {
            "profile_id": portfolio.profile_id,
            "fund_ref_id": fund.ref_id,
            "trading_collection_version_id":
            fund.trading_collection_version_id,
            "collection_id": fund.collection_id,
        }

        weights, collection_last_optimization_at = self.repo.get_collection_actual_weights(
            fund.collection_id)

        tcv: TradingCollectionVersion = self.repo.find_one(
            TradingCollectionVersion,
            {"id": fund.trading_collection_version_id})
        logging_extra["last_optimization_at"] = tcv.last_optimization_at
        logging_extra[
            "collection_last_optimization_at"] = collection_last_optimization_at
        logging_extra["weights"] = weights

        symbols_differ = set(i["symbol"]
                             for i in weights) != set(fund.weights.keys())
        if symbols_differ:
            logger.info(
                'rebalance_existing_collection_fund forcing rebalance fund: symbols differ',
                extra=logging_extra)

        if not symbols_differ and not tcv.last_optimization_at:
            logger.debug(
                'rebalance_existing_collection_fund skipping fund: not eligible for automatic rebalancing',
                extra=logging_extra)
            return False
        if not symbols_differ and tcv.last_optimization_at >= collection_last_optimization_at:
            logger.debug(
                'rebalance_existing_collection_fund skipping fund: already automatically rebalanced',
                extra=logging_extra)
            # Already automatically rebalanced
            return False

        trading_account_id = self._get_trading_account_id(portfolio)

        if weights:
            target_amount_delta_relative = None
        else:
            target_amount_delta_relative = Decimal(-1)

        trading_collection_version = self.trading_service.create_collection_version(
            portfolio.profile_id,
            TradingOrderSource.AUTOMATIC,
            fund.collection_id,
            trading_account_id,
            target_amount_delta_relative=target_amount_delta_relative,
            weights=weights,
            last_optimization_at=collection_last_optimization_at)

        self.provider.reconfigure_collection_holdings(
            portfolio, trading_collection_version, is_pending_rebalance)

        return True

    def rebalance_existing_ticker_fund(self, portfolio: DriveWealthPortfolio,
                                       fund: DriveWealthFund,
                                       is_pending_rebalance: bool) -> bool:
        logging_extra = {
            "profile_id": portfolio.profile_id,
            "fund_ref_id": fund.ref_id,
            "symbol": fund.symbol,
        }

        try:
            self.trading_service.check_tradeable_symbol(fund.symbol)
            return False
        except SymbolIsNotTradeableException:
            logger.info(
                'rebalance_existing_ticker_fund: symbol not tradeable, force selling',
                extra=logging_extra)

        trading_account_id = self._get_trading_account_id(portfolio)

        trading_order = self.trading_service.create_stock_order(
            portfolio.profile_id,
            TradingOrderSource.AUTOMATIC,
            fund.symbol,
            trading_account_id,
            target_amount_delta_relative=Decimal(-1))

        self.provider.execute_order_in_portfolio(portfolio, trading_order,
                                                 is_pending_rebalance)
        return True


def cli():
    try:
        with ContextContainer() as context_container:
            job = RebalancePortfoliosJob(
                context_container.trading_repository,
                context_container.drivewealth_repository,
                context_container.drivewealth_provider,
                context_container.trading_service)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
