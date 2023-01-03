from decimal import Decimal

import datetime

from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple, Iterable

from gainy.data_access.operators import OperatorLte
from gainy.data_access.repository import Repository
from gainy.trading.models import TradingOrderStatus, TradingCollectionVersion, TradingOrder


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

        yield from self.iterate_all(TradingCollectionVersion, params)

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

        yield from self.iterate_all(TradingOrder, params)

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
