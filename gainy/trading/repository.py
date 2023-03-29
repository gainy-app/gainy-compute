from decimal import Decimal

import datetime

from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple, Iterable, Optional

from gainy.billing.models import Invoice, PaymentTransaction, InvoiceStatus, PaymentTransactionStatus
from gainy.data_access.operators import OperatorLte
from gainy.data_access.repository import Repository
from gainy.trading.models import TradingOrderStatus, TradingCollectionVersion, TradingOrder
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingRepository(Repository):

    def get_collection_actual_weights(
            self,
            collection_id: int) -> Tuple[List[Dict[str, Any]], datetime.date]:
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """select symbol, weight, optimized_at 
                from collection_ticker_actual_weights
                         join tickers using (symbol) 
                where collection_id = %(collection_id)s
                  and is_trading_enabled""", {
                    "collection_id": collection_id,
                })
            weights = cursor.fetchall()

        last_optimization_at = None
        if weights:
            last_optimization_at = max(i["optimized_at"] for i in weights)

        weight_sum = sum(i["weight"] for i in weights)
        for i in weights:
            i["weight"] /= weight_sum

        return weights, last_optimization_at

    def iterate_trading_collection_versions(
        self,
        profile_id: int = None,
        trading_account_id: int = None,
        status: TradingOrderStatus = None,
        pending_execution_to: datetime.datetime = None
    ) -> Iterable[TradingCollectionVersion]:

        params = {}
        if profile_id:
            params["profile_id"] = profile_id
        if trading_account_id:
            params["trading_account_id"] = trading_account_id
        if status:
            params["status"] = status.name
        if pending_execution_to:
            params["pending_execution_since"] = OperatorLte(
                pending_execution_to)

        yield from self.iterate_all(TradingCollectionVersion, params,
                                    [("target_amount_delta", "asc")])

    def iterate_trading_orders(
        self,
        profile_id: int = None,
        trading_account_id: int = None,
        status: TradingOrderStatus = None,
        pending_execution_to: datetime.datetime = None
    ) -> Iterable[TradingOrder]:

        params = {}
        if profile_id:
            params["profile_id"] = profile_id
        if trading_account_id:
            params["trading_account_id"] = trading_account_id
        if status:
            params["status"] = status.name
        if pending_execution_to:
            params["pending_execution_since"] = OperatorLte(
                pending_execution_to)

        yield from self.iterate_all(TradingOrder, params,
                                    [("target_amount_delta", "asc")])

    def get_buying_power(self, trading_account_id: int) -> Decimal:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "select buying_power from trading_account_status where trading_account_id = %(trading_account_id)s",
                {
                    "trading_account_id": trading_account_id,
                })
            row = cursor.fetchone()

        if row:
            return Decimal(row[0])

        return Decimal(0)

    def get_fees_to_charge_sum(self, profile_id: int) -> Decimal:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "select pending_fees from trading_profile_status where profile_id = %(profile_id)s",
                {
                    "profile_id": profile_id,
                })
            row = cursor.fetchone()

        if row and row[0]:
            result = Decimal(row[0])
        else:
            result = Decimal(0)

        invoices = self.find_all(Invoice, {
            "profile_id": profile_id,
            "status": InvoiceStatus.PENDING
        })
        payment_transactions = self.find_all(
            PaymentTransaction, {
                "profile_id": profile_id,
                "status": PaymentTransactionStatus.PENDING_WITHDRAWN
            })

        extra = {
            "invoices": [i.to_dict() for i in invoices],
            "payment_transactions":
            [i.to_dict() for i in payment_transactions],
            "profile_id": profile_id,
            "result": result,
        }
        logger.info('get_fees_to_charge_sum', extra=extra)

        return result

    def get_collection_holding_value(self, profile_id: int,
                                     collection_id: int) -> Decimal:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "select actual_value from trading_profile_collection_status where profile_id = %(profile_id)s and collection_id = %(collection_id)s",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id,
                })
            row = cursor.fetchone()

        if row:
            return Decimal(row[0])

        return Decimal(0)

    def get_ticker_holding_value(self, profile_id: int,
                                 symbol: str) -> Decimal:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "select actual_value from trading_profile_ticker_status where profile_id = %(profile_id)s and symbol = %(symbol)s",
                {
                    "profile_id": profile_id,
                    "symbol": symbol,
                })
            row = cursor.fetchone()

        if row:
            return Decimal(row[0])

        return Decimal(0)

    def get_pending_orders_amounts(self,
                                   profile_id: int,
                                   symbol: str = None,
                                   collection_id: int = None):
        if collection_id:
            query = """select coalesce(sum(target_amount_delta), 0), 
                              coalesce(sum(target_amount_delta_relative), 0)
                from app.trading_collection_versions
                where profile_id = %(profile_id)s
                  and collection_id = %(collection_id)s
                  and status in %(statuses)s"""
            params = {
                "profile_id":
                profile_id,
                "collection_id":
                collection_id,
                "statuses": (TradingOrderStatus.PENDING.name,
                             TradingOrderStatus.PENDING_EXECUTION.name),
            }
        elif symbol:
            query = """select coalesce(sum(target_amount_delta), 0), 
                              coalesce(sum(target_amount_delta_relative), 0)
                from app.trading_orders
                where profile_id = %(profile_id)s
                  and symbol = %(symbol)s
                  and status in %(statuses)s"""
            params = {
                "profile_id":
                profile_id,
                "symbol":
                symbol,
                "statuses": (TradingOrderStatus.PENDING.name,
                             TradingOrderStatus.PENDING_EXECUTION.name),
            }
        else:
            raise Exception("You must specify either collection_id or symbol")

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()

        if row:
            return Decimal(row[0]), Decimal(row[1])

        return Decimal(0), Decimal(0)

    def calculate_executed_amount_sum(self,
                                      profile_id: int,
                                      min_date: datetime.date = None,
                                      collection_id: int = None,
                                      symbol: str = None) -> Decimal:
        params = {
            "profile_id": profile_id,
            "status": TradingOrderStatus.EXECUTED_FULLY.name,
        }

        if collection_id:
            query = """select sum(executed_amount)
                from app.trading_collection_versions
                where profile_id = %(profile_id)s
                  and collection_id = %(collection_id)s
                  and status = %(status)s"""
            params["collection_id"] = collection_id
        elif symbol:
            query = """select sum(executed_amount)
                from app.trading_orders
                where profile_id = %(profile_id)s
                  and symbol = %(symbol)s
                  and status = %(status)s"""
            params["symbol"] = symbol
        else:
            raise Exception("You must specify either collection_id or symbol")

        if min_date:
            query = query + " and pending_execution_since >= %(min_date)s"
            params["min_date"] = min_date

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()

        if row and row[0] is not None:
            return Decimal(row[0])

        return Decimal(0)

    def calculate_cash_flow_sum(self,
                                profile_id: int,
                                min_date: datetime.date = None,
                                collection_id: int = None,
                                symbol: str = None) -> Decimal:
        query = """select sum(cash_flow)
            from drivewealth_portfolio_historical_holdings
            where profile_id = %(profile_id)s"""
        params = {
            "profile_id": profile_id,
        }

        if collection_id:
            query = query + " and collection_id = %(collection_id)s"
            params["collection_id"] = collection_id
        elif symbol:
            query = query + " and collection_id is null and symbol = %(symbol)s"
            params["symbol"] = symbol
        else:
            raise Exception("You must specify either collection_id or symbol")

        if min_date:
            query = query + " and date >= %(min_date)s"
            params["min_date"] = min_date

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()

        if row and row[0] is not None:
            return Decimal(row[0])

        return Decimal(0)

    def get_last_selloff_date(self,
                              profile_id: int,
                              collection_id: int = None,
                              symbol: str = None) -> Optional[datetime.date]:
        query = """select last_selloff_date
            from drivewealth_portfolio_historical_holdings_marked
                     join drivewealth_holdings using (holding_id_v2)
            where profile_id = %(profile_id)s"""
        params = {
            "profile_id": profile_id,
        }

        if collection_id:
            query = query + " and collection_id = %(collection_id)s"
            params["collection_id"] = collection_id
        elif symbol:
            query = query + " and collection_id is null and symbol = %(symbol)s"
            params["symbol"] = symbol
        else:
            raise Exception("You must specify either collection_id or symbol")

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()

        if row and row[0] is not None:
            return row[0]

        return None
