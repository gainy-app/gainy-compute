import regex
from decimal import Decimal

import datetime
from typing import List, Iterable, Dict

from gainy.data_access.operators import OperatorGt
from gainy.exceptions import KYCFormHasNotBeenSentException, EntityNotFoundException
from gainy.trading.drivewealth import DriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthPortfolio, DriveWealthPortfolioStatus, \
    DriveWealthFund, DriveWealthInstrument, DriveWealthAccount, EXECUTED_AMOUNT_PRECISION, DriveWealthPortfolioHolding
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.models import TradingOrderStatus, TradingAccount, TradingCollectionVersion, TradingOrder, \
    AmountAwareTradingOrder
from gainy.trading.repository import TradingRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

DRIVE_WEALTH_PORTFOLIO_STATUS_TTL = 300  # in seconds


# also in https://github.com/gainy-app/gainy-app/blob/main/src/meltano/meltano/seed/00_functions.sql
# also in https://github.com/gainy-app/gainy-compute/blob/main/fixtures/functions.sql
def normalize_symbol(s: str):
    s = regex.sub(r'\.([AB])$', '-\\1', s)
    return regex.sub(r'\.(.*)$', '', s)


class DriveWealthProviderBase:
    repository: DriveWealthRepository = None
    trading_repository: TradingRepository = None

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi,
                 trading_repository: TradingRepository):
        self.repository = repository
        self.trading_repository = trading_repository
        self.api = api

    def sync_portfolios(self, profile_id: int, force: bool = False):
        repository = self.repository

        portfolios: List[DriveWealthPortfolio] = repository.find_all(
            DriveWealthPortfolio, {"profile_id": profile_id})
        for portfolio in portfolios:
            if portfolio.is_artificial:
                return
            self.sync_portfolio(portfolio, force=force)
            self.sync_portfolio_status(portfolio, force=force)

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

    def sync_portfolio_status(self,
                              portfolio: DriveWealthPortfolio,
                              force: bool = False):
        portfolio_status = self._get_portfolio_status(portfolio, force=force)
        portfolio.update_from_status(portfolio_status)
        self.repository.persist(portfolio)
        self.update_trading_collection_versions_pending_execution_from_portfolio_status(
            portfolio_status)
        self.update_trading_orders_pending_execution_from_portfolio_status(
            portfolio_status)
        return portfolio_status

    def update_trading_collection_versions_pending_execution_from_portfolio_status(
            self, portfolio_status: DriveWealthPortfolioStatus):
        if portfolio_status.last_portfolio_rebalance_at is None:
            return

        try:
            trading_account = self._get_trading_account_by_portfolio_status(
                portfolio_status)
        except EntityNotFoundException:
            return

        profile_id = trading_account.profile_id

        by_collection: Dict[int, list[TradingCollectionVersion]] = {}
        for trading_collection_version in self.trading_repository.iterate_trading_collection_versions(
                profile_id=trading_account.profile_id,
                trading_account_id=trading_account.id,
                status=TradingOrderStatus.PENDING_EXECUTION,
                pending_execution_to=portfolio_status.
                last_portfolio_rebalance_at):

            collection_id = trading_collection_version.collection_id
            if collection_id not in by_collection:
                by_collection[collection_id] = []
            by_collection[collection_id].append(trading_collection_version)

        for collection_id, trading_collection_versions in by_collection.items(
        ):
            self._fill_executed_amount(
                profile_id,
                trading_collection_versions,
                portfolio_status.last_portfolio_rebalance_at,
                collection_id=collection_id)

    def update_trading_orders_pending_execution_from_portfolio_status(
            self, portfolio_status: DriveWealthPortfolioStatus):
        if portfolio_status.last_portfolio_rebalance_at is None:
            return

        try:
            trading_account = self._get_trading_account_by_portfolio_status(
                portfolio_status)
        except EntityNotFoundException:
            return

        profile_id = trading_account.profile_id

        by_symbol: Dict[str, list[TradingOrder]] = {}
        for trading_order in self.trading_repository.iterate_trading_orders(
                profile_id=trading_account.profile_id,
                trading_account_id=trading_account.id,
                status=TradingOrderStatus.PENDING_EXECUTION,
                pending_execution_to=portfolio_status.
                last_portfolio_rebalance_at):

            symbol = trading_order.symbol
            if symbol not in by_symbol:
                by_symbol[symbol] = []
            by_symbol[symbol].append(trading_order)

        for symbol, trading_orders in by_symbol.items():
            self._fill_executed_amount(
                profile_id,
                trading_orders,
                portfolio_status.last_portfolio_rebalance_at,
                symbol=symbol)

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

        portfolio_status: DriveWealthPortfolioStatus = self.repository.find_one(
            DriveWealthPortfolioStatus, {
                "drivewealth_portfolio_id":
                portfolio.ref_id,
                "created_at":
                OperatorGt(
                    datetime.datetime.now(datetime.timezone.utc) -
                    datetime.timedelta(
                        seconds=DRIVE_WEALTH_PORTFOLIO_STATUS_TTL)),
            }, [("created_at", "DESC")])

        if not force and portfolio_status:
            return portfolio_status

        data = self.api.get_portfolio_status(portfolio)
        portfolio_status = DriveWealthPortfolioStatus()
        portfolio_status.set_from_response(data)
        self.repository.persist(portfolio_status)

        self._create_portfolio_holdings_from_status(portfolio_status)

        return portfolio_status

    def _get_user(self, profile_id) -> DriveWealthUser:
        repository = self.repository
        user = repository.get_user(profile_id)
        if user is None:
            raise KYCFormHasNotBeenSentException("KYC form has not been sent")
        return user

    def _get_trading_account_by_portfolio_status(
            self, portfolio_status) -> TradingAccount:
        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio,
            {"ref_id": portfolio_status.drivewealth_portfolio_id})
        if not portfolio or not portfolio.drivewealth_account_id:
            raise EntityNotFoundException

        dw_account: DriveWealthAccount = self.repository.find_one(
            DriveWealthAccount, {"ref_id": portfolio.drivewealth_account_id})
        if not dw_account or not dw_account.trading_account_id:
            raise EntityNotFoundException

        trading_account: TradingAccount = self.repository.find_one(
            TradingAccount, {"id": dw_account.trading_account_id})
        if not trading_account:
            raise EntityNotFoundException

        return trading_account

    def _fill_executed_amount(self,
                              profile_id,
                              orders: List[AmountAwareTradingOrder],
                              last_portfolio_rebalance_at: datetime.datetime,
                              collection_id: int = None,
                              symbol: str = None):
        orders = [
            order for order in orders if order.target_amount_delta is not None
        ]
        pending_amount_sum = sum(order.target_amount_delta for order in orders)

        if collection_id:
            executed_amount_sum = self.trading_repository.calculate_executed_amount_sum(
                profile_id, collection_id=collection_id)
            cash_flow_sum = self.trading_repository.calculate_cash_flow_sum(
                profile_id, collection_id=collection_id)
        elif symbol:
            executed_amount_sum = self.trading_repository.calculate_executed_amount_sum(
                profile_id, symbol=symbol)
            cash_flow_sum = self.trading_repository.calculate_cash_flow_sum(
                profile_id, symbol=symbol)
        else:
            raise Exception("You must specify either collection_id or symbol")

        # executed_amount_sum + pending_amount_sum = cash_flow_sum
        diff = executed_amount_sum + pending_amount_sum - cash_flow_sum
        for order in reversed(orders):
            if order.target_amount_delta > 0:
                error = max(Decimal(0), min(order.target_amount_delta, diff))
            elif order.target_amount_delta < 0:
                error = min(Decimal(0), max(order.target_amount_delta, diff))
            else:
                continue

            diff = diff - error
            order.executed_amount = order.target_amount_delta - error

            if abs(error) < EXECUTED_AMOUNT_PRECISION:
                order.status = TradingOrderStatus.EXECUTED_FULLY
                order.executed_at = last_portfolio_rebalance_at
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
            if holding_data["type"] != "FUND":
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
