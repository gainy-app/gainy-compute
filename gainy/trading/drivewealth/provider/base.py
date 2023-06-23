from decimal import Decimal

import datetime
from itertools import groupby
from typing import List, Iterable, Dict, Optional

from gainy.analytics.service import AnalyticsService
from gainy.data_access.operators import OperatorNot, OperatorIn, OperatorGt
from gainy.exceptions import KYCFormHasNotBeenSentException, EntityNotFoundException
from gainy.services.notification import NotificationService
from gainy.trading.drivewealth import DriveWealthApi
from gainy.trading.drivewealth.exceptions import InvalidDriveWealthPortfolioStatusException
from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthPortfolio, DriveWealthPortfolioStatus, \
    DriveWealthFund, DriveWealthInstrument, DriveWealthAccount, EXECUTED_AMOUNT_PRECISION, DriveWealthPortfolioHolding, \
    PRECISION, DriveWealthInstrumentStatus, DriveWealthTransaction, DriveWealthSpinOffTransaction, \
    DriveWealthAccountPositions
from gainy.trading.drivewealth.provider.interface import DriveWealthProviderInterface
from gainy.trading.drivewealth.provider.misc import normalize_symbol
from gainy.trading.drivewealth.provider.transaction_handler import DriveWealthTransactionHandler
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.models import TradingOrderStatus, TradingAccount, TradingCollectionVersion, TradingOrder, \
    AbstractTradingOrder
from gainy.trading.repository import TradingRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

DRIVE_WEALTH_PORTFOLIO_STATUS_TTL = 300  # in seconds
DRIVE_WEALTH_ACCOUNT_MONEY_STATUS_TTL = 300  # in seconds
DRIVE_WEALTH_ACCOUNT_POSITIONS_STATUS_TTL = 300  # in seconds


class DriveWealthProviderBase(DriveWealthProviderInterface):
    repository: DriveWealthRepository = None
    trading_repository: TradingRepository = None

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi,
                 trading_repository: TradingRepository,
                 notification_service: NotificationService,
                 analytics_service: AnalyticsService):
        self.repository = repository
        self.trading_repository = trading_repository
        self.api = api
        self.notification_service = notification_service
        self.analytics_service = analytics_service

    def sync_portfolios(self, profile_id: int, force: bool = False):
        repository = self.repository

        portfolios: List[DriveWealthPortfolio] = repository.find_all(
            DriveWealthPortfolio, {"profile_id": profile_id})
        for portfolio in portfolios:
            if portfolio.is_artificial:
                return
            self.sync_portfolio(portfolio, force=force)
            try:
                self.sync_portfolio_status(portfolio, force=force)
            except InvalidDriveWealthPortfolioStatusException as e:
                logger.warning(e,
                               extra={
                                   "profile_id": portfolio.profile_id,
                                   "account_id":
                                   portfolio.drivewealth_account_id
                               })

    def sync_portfolio(self,
                       portfolio: DriveWealthPortfolio,
                       force: bool = False):
        if not force and portfolio.last_sync_at is not None and portfolio.last_sync_at > datetime.datetime.now(
                datetime.timezone.utc) - datetime.timedelta(
                    seconds=DRIVE_WEALTH_PORTFOLIO_STATUS_TTL):
            return portfolio

        data = self.api.get_portfolio(portfolio)
        portfolio.set_from_response(data)
        portfolio.last_sync_at = datetime.datetime.now()
        self.repository.persist(portfolio)

    def sync_portfolio_status(
            self,
            portfolio: DriveWealthPortfolio,
            force: bool = False,
            allow_invalid: bool = False) -> DriveWealthPortfolioStatus:

        try:
            portfolio_status = self._get_portfolio_status(portfolio,
                                                          force=force)
            portfolio.update_from_status(portfolio_status)
            self.repository.persist(portfolio)

            return portfolio_status
        except InvalidDriveWealthPortfolioStatusException as e:
            logger.warning(e)

            if allow_invalid:
                return e.portfolio_status

            # in case we received an invalid portfolio status - look for a valid one, which is not more than an hour old
            portfolio_status = self.get_latest_portfolio_status(
                portfolio.ref_id)
            min_created_at = datetime.datetime.now(
                datetime.timezone.utc) - datetime.timedelta(hours=1)
            if portfolio_status and portfolio_status.created_at > min_created_at:
                return portfolio_status

            raise e

    def update_trading_orders_pending_execution_from_portfolio_status(
            self, portfolio_status: DriveWealthPortfolioStatus):
        if portfolio_status.last_portfolio_rebalance_at is None:
            return

        try:
            trading_account = self.get_trading_account_by_portfolio_status(
                portfolio_status)
        except EntityNotFoundException:
            return

        profile_id = trading_account.profile_id

        by_collection: Dict[int, list[TradingCollectionVersion]] = {}
        by_symbol: Dict[str, list[TradingOrder]] = {}
        for trading_order in self.trading_repository.iterate_trading_orders(
                profile_id=trading_account.profile_id,
                trading_account_id=trading_account.id,
                status=TradingOrderStatus.PENDING_EXECUTION,
                pending_execution_to=portfolio_status.
                last_portfolio_rebalance_at):

            if isinstance(trading_order, TradingCollectionVersion):
                collection_id = trading_order.collection_id
                if collection_id not in by_collection:
                    by_collection[collection_id] = []
                by_collection[collection_id].append(trading_order)
            elif isinstance(trading_order, TradingOrder):
                symbol = trading_order.symbol
                if symbol not in by_symbol:
                    by_symbol[symbol] = []
                by_symbol[symbol].append(trading_order)
            else:
                raise Exception("Unsupported order class.")

        for collection_id, trading_collection_versions in by_collection.items(
        ):
            self._fill_executed_amount(profile_id,
                                       trading_collection_versions,
                                       portfolio_status,
                                       collection_id=collection_id)
            for tcv in trading_collection_versions:
                if not tcv.is_executed():
                    continue
                self.analytics_service.on_order_executed(tcv)

        for symbol, trading_orders in by_symbol.items():
            self._fill_executed_amount(profile_id,
                                       trading_orders,
                                       portfolio_status,
                                       symbol=symbol)
            for order in trading_orders:
                if not order.is_executed():
                    continue
                self.analytics_service.on_order_executed(order)

    def iterate_profile_funds(self,
                              profile_id: int) -> Iterable[DriveWealthFund]:
        yield from self.repository.iterate_all(DriveWealthFund, {
            "profile_id": profile_id,
        })

    def sync_instrument(self, ref_id: str = None, symbol: str = None):
        data = self.api.get_instrument_details(ref_id=ref_id, symbol=symbol)
        instrument = DriveWealthInstrument()
        instrument.set_from_response(data)
        self.repository.persist(instrument)
        return instrument

    def _get_portfolio_status(
            self,
            portfolio: DriveWealthPortfolio,
            force: bool = False) -> DriveWealthPortfolioStatus:

        if not force:
            portfolio_status = self.get_latest_portfolio_status(
                portfolio.ref_id)

            min_created_at = datetime.datetime.now(
                datetime.timezone.utc) - datetime.timedelta(
                    seconds=DRIVE_WEALTH_PORTFOLIO_STATUS_TTL)
            if portfolio_status and portfolio_status.created_at > min_created_at:
                return portfolio_status

        data = self.api.get_portfolio_status(portfolio)
        portfolio_status = DriveWealthPortfolioStatus()
        portfolio_status.set_from_response(data)
        if not portfolio_status.is_valid():
            raise InvalidDriveWealthPortfolioStatusException(portfolio_status)
        self.repository.persist(portfolio_status)

        self._create_portfolio_holdings_from_status(portfolio_status)

        return portfolio_status

    def _get_user(self, profile_id) -> DriveWealthUser:
        repository = self.repository
        user = repository.get_user(profile_id)
        if user is None:
            raise KYCFormHasNotBeenSentException("KYC form has not been sent")
        return user

    def get_trading_account_by_portfolio_status(
            self, portfolio_status) -> TradingAccount:
        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio,
            {"ref_id": portfolio_status.drivewealth_portfolio_id})
        return self.get_trading_account_by_portfolio(portfolio)

    def get_trading_account_by_portfolio(
            self, portfolio: DriveWealthPortfolio) -> TradingAccount:
        if not portfolio or not portfolio.drivewealth_account_id:
            raise EntityNotFoundException(DriveWealthPortfolio)

        dw_account: DriveWealthAccount = self.repository.find_one(
            DriveWealthAccount, {"ref_id": portfolio.drivewealth_account_id})
        if not dw_account or not dw_account.trading_account_id:
            raise EntityNotFoundException(DriveWealthAccount)

        trading_account: TradingAccount = self.repository.find_one(
            TradingAccount, {"id": dw_account.trading_account_id})
        if not trading_account:
            raise EntityNotFoundException(TradingAccount)

        return trading_account

    def _fill_executed_amount(self,
                              profile_id,
                              orders: List[AbstractTradingOrder],
                              portfolio_status: DriveWealthPortfolioStatus,
                              collection_id: int = None,
                              symbol: str = None):
        pending_amount_sum = sum(order.target_amount_delta for order in orders
                                 if order.target_amount_delta is not None)

        if collection_id:
            min_date = self.trading_repository.get_last_selloff_date(
                profile_id, collection_id=collection_id)
            executed_amount_sum = self.trading_repository.calculate_executed_amount_sum(
                profile_id, collection_id=collection_id, min_date=min_date)
            cash_flow_sum = self.trading_repository.calculate_cash_flow_sum(
                profile_id, collection_id=collection_id, min_date=min_date)
            fund = self.repository.get_profile_fund(
                profile_id, collection_id=collection_id)
        elif symbol:
            min_date = self.trading_repository.get_last_selloff_date(
                profile_id, symbol=symbol)
            executed_amount_sum = self.trading_repository.calculate_executed_amount_sum(
                profile_id, symbol=symbol, min_date=min_date)
            cash_flow_sum = self.trading_repository.calculate_cash_flow_sum(
                profile_id, symbol=symbol, min_date=min_date)
            fund = self.repository.get_profile_fund(profile_id, symbol=symbol)
        else:
            raise Exception("You must specify either collection_id or symbol")

        last_portfolio_rebalance_at = portfolio_status.last_portfolio_rebalance_at

        # executed_amount_sum + pending_amount_sum = cash_flow_sum
        diff = executed_amount_sum + pending_amount_sum - cash_flow_sum
        logger_extra = {
            "profile_id": profile_id,
            "executed_amount_sum": executed_amount_sum,
            "pending_amount_sum": pending_amount_sum,
            "cash_flow_sum": cash_flow_sum,
            "last_portfolio_rebalance_at": last_portfolio_rebalance_at,
            "is_pending_rebalance": portfolio_status.is_pending_rebalance(),
        }
        last_portfolio_rebalance_at_threshold = datetime.datetime.now(
            tz=datetime.timezone.utc) - datetime.timedelta(minutes=30)
        for order in reversed(orders):
            logger_extra["order_class"] = order.__class__.__name__
            logger_extra["order_id"] = order.id
            logger_extra["diff"] = diff

            if order.target_amount_delta is None or abs(
                    order.target_amount_delta) < PRECISION:
                if last_portfolio_rebalance_at and last_portfolio_rebalance_at > max(
                        order.pending_execution_since,
                        last_portfolio_rebalance_at_threshold):
                    order.status = TradingOrderStatus.EXECUTED_FULLY
                    order.executed_at = last_portfolio_rebalance_at

                    logger_extra[
                        "target_amount_delta"] = order.target_amount_delta
                    logger_extra["is_executed"] = order.is_executed()
                    logger.info('_fill_executed_amount', extra=logger_extra)

                continue

            if order.target_amount_delta > 0:
                error = min(order.target_amount_delta, diff)
            else:
                error = max(order.target_amount_delta, diff)

            logger_extra["error"] = error

            diff = diff - error
            order.executed_amount = order.target_amount_delta - error
            logger_extra["executed_amount"] = order.executed_amount

            if order.target_amount_delta_relative is None:
                is_executed = abs(error) < max(
                    EXECUTED_AMOUNT_PRECISION,
                    Decimal(0.0005) * portfolio_status.equity_value)
            else:
                if not fund:
                    raise Exception('No fund for %s %d' %
                                    (order.__class__.__name__, order.id))
                is_executed = portfolio_status.get_fund_value(
                    fund.ref_id) < PRECISION

            if is_executed:
                order.status = TradingOrderStatus.EXECUTED_FULLY
                order.executed_at = last_portfolio_rebalance_at

            logger_extra["is_executed"] = order.is_executed()
            logger.info('_fill_executed_amount', extra=logger_extra)
        self.trading_repository.persist(orders)

    def _create_portfolio_holdings_from_status(
            self, portfolio_status: DriveWealthPortfolioStatus):
        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio,
            {"ref_id": portfolio_status.drivewealth_portfolio_id})
        if not portfolio:
            return
        profile_id = portfolio.profile_id

        holdings = []
        for holding_data in portfolio_status.data["holdings"]:
            if holding_data["type"] == "CASH_RESERVE":
                holding = DriveWealthPortfolioHolding()
                holding.portfolio_status_id = portfolio_status.id
                holding.profile_id = profile_id
                holding.holding_id_v2 = f"{profile_id}_cash_CUR:USD"
                holding.actual_value = holding_data["value"]
                holding.quantity = holding_data["value"]
                holding.symbol = "CUR:USD"
                holding.collection_uniq_id = None
                holding.collection_id = None
                holdings.append(holding)
                continue

            fund_id = holding_data["id"]
            fund: DriveWealthFund = self.repository.find_one(
                DriveWealthFund, {"ref_id": fund_id})
            if not fund:
                continue

            collection_id = fund.collection_id
            for fund_folding_data in holding_data['holdings']:
                symbol = normalize_symbol(fund_folding_data['symbol'])
                if collection_id:
                    holding_id_v2 = f"dw_ttf_{profile_id}_{collection_id}_{symbol}"
                else:
                    holding_id_v2 = f"dw_ticker_{profile_id}_{symbol}"

                quantity = Decimal(fund_folding_data["openQty"])

                holding = DriveWealthPortfolioHolding()
                holding.portfolio_status_id = portfolio_status.id
                holding.profile_id = profile_id
                holding.holding_id_v2 = holding_id_v2
                holding.actual_value = fund_folding_data["value"]
                holding.quantity = quantity
                holding.symbol = symbol
                if collection_id:
                    holding.collection_uniq_id = f"0_{collection_id}"
                    holding.collection_id = collection_id
                else:
                    holding.collection_uniq_id = None
                    holding.collection_id = None
                holdings.append(holding)

        self.repository.persist(holdings)
        holding_ids = [i.holding_id_v2 for i in holdings]
        self.repository.delete_by(
            DriveWealthPortfolioHolding, {
                "profile_id": profile_id,
                "holding_id_v2": OperatorNot(OperatorIn(holding_ids))
            })

    def get_latest_portfolio_status(
            self,
            portfolio_ref_id: str) -> Optional[DriveWealthPortfolioStatus]:
        return self.repository.find_one(
            DriveWealthPortfolioStatus, {
                "drivewealth_portfolio_id": portfolio_ref_id,
            }, [("created_at", "DESC")])

    def actualize_portfolio(self, portfolio: DriveWealthPortfolio,
                            portfolio_status: DriveWealthPortfolioStatus):

        logging_extra = {
            "profile_id": portfolio.profile_id,
            "portfolio_status": portfolio_status.to_dict(),
            "portfolio_pre": portfolio.to_dict(),
        }
        portfolio.set_target_weights_from_status_actual_weights(
            portfolio_status)
        self.repository.persist(portfolio)

        funds: list[DriveWealthFund] = self.repository.find_all(
            DriveWealthFund,
            {"ref_id": OperatorIn(portfolio_status.get_fund_ref_ids())})
        logging_extra["funds_pre"] = [fund.to_dict() for fund in funds]
        for fund in funds:
            fund.set_target_weights_from_status_actual_weights(
                portfolio_status)
        self.repository.persist(funds)

        logging_extra["portfolio_post"] = portfolio.to_dict()
        logging_extra["funds_post"] = [fund.to_dict() for fund in funds]
        logger.info('set_target_weights_from_status_actual_weights',
                    extra=logging_extra)

    #todo deprecated
    def rebalance_portfolio_cash(
            self, portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus) -> bool:
        new_equity_value = portfolio_status.equity_value

        new_transactions_amount_sum = Decimal(0)
        new_transactions = self.repository.get_new_transactions(
            portfolio.drivewealth_account_id, portfolio.last_transaction_id)
        for transaction in new_transactions:
            if portfolio.last_transaction_id:
                portfolio.last_transaction_id = max(
                    portfolio.last_transaction_id, transaction.id)
            else:
                portfolio.last_transaction_id = transaction.id

            new_transactions_amount_sum += transaction.account_amount_delta

        # pending redemptions do not have transactions, but are already accounted in portfolio balance.
        pending_redemptions_amount_sum = Decimal(0)
        pending_redemptions = self.repository.get_pending_redemptions(
            portfolio.drivewealth_account_id)
        for redemption in pending_redemptions:
            pending_redemptions_amount_sum += redemption.amount

        new_transactions_amount_sum += pending_redemptions_amount_sum - portfolio.pending_redemptions_amount_sum

        logging_extra = {
            "profile_id": portfolio.profile_id,
            "prev_pending_redemptions_amount_sum":
            portfolio.pending_redemptions_amount_sum,
            "new_pending_redemptions_amount_sum":
            pending_redemptions_amount_sum,
            "new_transactions_amount_sum": new_transactions_amount_sum,
            "new_transactions": [i.to_dict() for i in new_transactions],
            "portfolio_pre": portfolio.to_dict(),
            "portfolio_status": portfolio_status.to_dict(),
        }

        if abs(new_transactions_amount_sum) < PRECISION:
            portfolio.last_equity_value = new_equity_value
            portfolio.pending_redemptions_amount_sum = pending_redemptions_amount_sum
            self.repository.persist(portfolio)
            return False

        if not new_equity_value:
            # todo handle?
            return False
        '''
        new_transactions_amount_sum=200
        cash_weight 0.5     0.8333
        fund_weight 0.5     0.1667
        equity_value 100    300
        
        cash_weight_delta = (0.5 * 100 + 200) / 300 - 0.5 = 0.3333
        '''

        if portfolio.last_equity_value:
            last_equity_value = portfolio.last_equity_value
        else:
            last_equity_value = Decimal(0)

        try:
            cash_weight_delta = (
                portfolio.cash_target_weight * last_equity_value +
                new_transactions_amount_sum
            ) / new_equity_value - portfolio.cash_target_weight
            logging_extra["cash_weight_delta"] = cash_weight_delta
            portfolio.rebalance_cash(cash_weight_delta)
            portfolio.last_equity_value = new_equity_value
            portfolio.pending_redemptions_amount_sum = pending_redemptions_amount_sum
            logging_extra["portfolio_post"] = portfolio.to_dict()

            logger.info('rebalance_portfolio_cash', extra=logging_extra)
            self.repository.persist(portfolio)
            return True
        except Exception as e:
            logging_extra["exc"] = e
            logger.exception('rebalance_portfolio_cash', extra=logging_extra)
            raise e

    def send_portfolio_to_api(self, portfolio: DriveWealthPortfolio):
        if portfolio.is_artificial:
            return

        funds: list[DriveWealthFund] = self.repository.find_all(
            DriveWealthFund,
            {"ref_id": OperatorIn(portfolio.get_fund_ref_ids())})
        for fund in funds:
            self.remove_inactive_instruments(fund)
            fund.normalize_weights()
            self.api.update_fund(fund)
        self.repository.persist(funds)

        portfolio.normalize_weights()
        self.api.update_portfolio(portfolio)
        self.repository.persist(portfolio)

    def remove_inactive_instruments(self, fund: DriveWealthFund):
        instrument_ids = fund.get_instrument_ids()
        active_instruments: list[
            DriveWealthInstrument] = self.repository.find_all(
                DriveWealthInstrument, {
                    "ref_id": OperatorIn(instrument_ids),
                    "status": DriveWealthInstrumentStatus.ACTIVE.name,
                })
        active_instrument_ids = set(i.ref_id for i in active_instruments)
        fund.remove_instrument_ids(set(instrument_ids) - active_instrument_ids)

    def sync_account_positions(
            self,
            account_ref_id: str,
            force: bool = False) -> DriveWealthAccountPositions:

        if not force:
            account_positions: DriveWealthAccountPositions = self.repository.find_one(
                DriveWealthAccountPositions, {
                    "drivewealth_account_id":
                    account_ref_id,
                    "created_at":
                    OperatorGt(
                        datetime.datetime.now(datetime.timezone.utc) -
                        datetime.timedelta(
                            seconds=DRIVE_WEALTH_ACCOUNT_POSITIONS_STATUS_TTL)
                    ),
                }, [("created_at", "DESC")])

            if account_positions:
                return account_positions

        account_positions_data = self.api.get_account_positions(account_ref_id)
        account_positions = DriveWealthAccountPositions()
        account_positions.set_from_response(account_positions_data)
        self.repository.persist(account_positions)
        return account_positions
