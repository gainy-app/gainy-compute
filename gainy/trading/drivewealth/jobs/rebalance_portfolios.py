import psycopg2.errors
from decimal import Decimal

import datetime
import dateutil.parser

from typing import Iterable, Tuple

import time

from gainy.context_container import ContextContainer
from gainy.data_access.operators import OperatorIn
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthRepository
from gainy.trading.drivewealth.exceptions import DriveWealthApiException, TradingAccountNotOpenException, \
    InvalidDriveWealthPortfolioStatusException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount, DW_WEIGHT_THRESHOLD, \
    DriveWealthFund
from gainy.trading.exceptions import InsufficientFundsException, SymbolIsNotTradeableException
from gainy.trading.models import TradingCollectionVersion, TradingOrderStatus, TradingOrderSource, TradingOrder, \
    AbstractTradingOrder
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

        force_rebalance_portfolios = []
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

                portfolio_status = self.provider.sync_portfolio_status(
                    portfolio, force=True)

                # 0. update pending execution orders
                self.provider.update_trading_orders_pending_execution_from_portfolio_status(
                    portfolio_status)

                # 1. set target weights from actual weights and change cash weight in case of new transactions
                portfolio_changed = self.provider.actualize_portfolio(
                    portfolio, portfolio_status)

                # 3. apply all pending orders
                trading_orders = self.apply_trading_orders(portfolio)
                portfolio_changed = trading_orders or portfolio_changed

                # 4. rebalance collections automatically
                portfolio_changed = self.rebalance_existing_funds(
                    portfolio) or portfolio_changed

                if not portfolio_changed:
                    continue

                self.provider.send_portfolio_to_api(portfolio)
                force_rebalance_portfolios.append(portfolio)

                self.repo.commit()
            except (psycopg2.errors.OperationalError,
                    DriveWealthApiException) as e:
                logger.exception(e)
                self.repo.rollback()
            except InvalidDriveWealthPortfolioStatusException:
                logger.info(
                    f"Skipping portfolio {portfolio.ref_id} due to invalid status",
                    extra={
                        "profile_id": portfolio.profile_id,
                        "account_id": portfolio.drivewealth_account_id
                    })
            except Exception as e:
                logger.exception(e)

        self.force_rebalance(force_rebalance_portfolios)

    def apply_trading_orders(
            self,
            portfolio: DriveWealthPortfolio) -> list[AbstractTradingOrder]:
        profile_id = portfolio.profile_id
        trading_account_id = self._get_trading_account_id(portfolio)

        trading_orders = []
        for trading_order in self.repo.iterate_trading_orders(
                profile_id=profile_id,
                trading_account_id=trading_account_id,
                status=[
                    TradingOrderStatus.PENDING,
                    TradingOrderStatus.PENDING_EXECUTION
                ]):
            start_time = time.time()

            logger.info("Executing %s %d for profile %d account %d in %fs",
                        trading_order.__class__.__name__,
                        trading_order.id,
                        profile_id,
                        trading_account_id,
                        time.time() - start_time,
                        extra={"profile_id": profile_id})

            try:
                self.provider.execute_order_in_portfolio(
                    portfolio, trading_order)

                trading_orders.append(trading_order)
                trading_order.status = TradingOrderStatus.PENDING_EXECUTION
                trading_order.pending_execution_since = datetime.datetime.now()
                self.repo.persist(trading_order)
            except InsufficientFundsException as e:
                logger.info("Skipping order %s for profile %d account %d: %s",
                            trading_order.id,
                            profile_id,
                            trading_account_id,
                            e.__class__.__name__,
                            extra={"profile_id": profile_id})
                # let it stay pending until there are money on the account
                continue
            except DriveWealthApiException as e:
                logger.exception(e)

        return trading_orders

    def rebalance_existing_funds(self,
                                 portfolio: DriveWealthPortfolio) -> bool:
        """
        Automatically change portfolio weights according to the new collection weights
        :param portfolio: DriveWealthPortfolio
        :return:
        """
        profile_id = portfolio.profile_id
        portfolio_changed = False
        try:
            for fund in self.provider.iterate_profile_funds(profile_id):
                fund_weight = portfolio.get_fund_weight(fund.ref_id)
                if fund_weight < DW_WEIGHT_THRESHOLD:
                    continue

                if fund.trading_collection_version_id:
                    result = self.rebalance_existing_collection_fund(
                        portfolio, fund)
                    portfolio_changed = portfolio_changed or result
                if fund.trading_order_id:
                    result = self.rebalance_existing_ticker_fund(
                        portfolio, fund)
                    portfolio_changed = portfolio_changed or result
        except DriveWealthApiException as e:
            logger.exception(e)

        return portfolio_changed

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

    def force_rebalance(self, portfolios: list[DriveWealthPortfolio]):
        try:
            data = self.provider.api.create_autopilot_run(
                [portfolio.drivewealth_account_id for portfolio in portfolios])

            d = dateutil.parser.parse(data["created"])
            d -= datetime.timedelta(microseconds=d.microsecond)
            for portfolio in portfolios:
                portfolio.waiting_rebalance_since = d
            self.repo.persist(portfolios)

            logger.info("Forced portfolio rebalance",
                        extra={
                            "portfolios":
                            [portfolio.ref_id for portfolio in portfolios],
                            "profile_ids":
                            [portfolio.profile_id for portfolio in portfolios],
                            "data":
                            data,
                        })
        except DriveWealthApiException as e:
            logger.info("Failed to force portfolio rebalance",
                        extra={
                            "portfolios":
                            [portfolio.ref_id for portfolio in portfolios],
                            "profile_ids":
                            [portfolio.profile_id for portfolio in portfolios],
                            "e":
                            e,
                        })
            pass

    def rebalance_existing_collection_fund(self,
                                           portfolio: DriveWealthPortfolio,
                                           fund: DriveWealthFund) -> bool:
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

        self.provider.execute_order_in_portfolio(portfolio,
                                                 trading_collection_version)

        return True

    def rebalance_existing_ticker_fund(self, portfolio: DriveWealthPortfolio,
                                       fund: DriveWealthFund) -> bool:
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

        self.provider.execute_order_in_portfolio(portfolio, trading_order)
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
