import datetime

from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple, Iterable

from gainy.data_access.operators import OperatorLt, OperatorIsNull, OperatorOr
from gainy.data_access.repository import Repository
from gainy.trading.models import TradingOrderStatus, TradingCollectionVersion, TradingOrder


class TradingRepository(Repository):

    def get_collection_actual_weights(
            self,
            collection_id: int) -> Tuple[List[Dict[str, Any]], datetime.date]:
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select symbol, weight, optimized_at from collection_ticker_actual_weights where collection_id = %(collection_id)s",
                {
                    "collection_id": collection_id,
                })
            weights = cursor.fetchall()

        last_optimization_at = None
        if weights:
            last_optimization_at = max(i["optimized_at"] for i in weights)

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
            params["pending_execution_since"] = OperatorOr([
                OperatorLt(pending_execution_to),
                OperatorIsNull(),
            ])

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
            params["pending_execution_since"] = OperatorOr([
                OperatorLt(pending_execution_to),
                OperatorIsNull(),
            ])

        yield from self.iterate_all(TradingOrder, params)
