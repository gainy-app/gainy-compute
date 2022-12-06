import datetime

from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple

from gainy.data_access.repository import Repository


class TradingRepository(Repository):

    def get_collection_actual_weights(
            self,
            collection_id: int) -> Tuple[List[Dict[str, Any]], datetime.date]:
        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select symbol, weight, last_optimization_at from collection_ticker_actual_weights where collection_id = %(collection_id)s",
                {
                    "collection_id": collection_id,
                })
            weights = cursor.fetchall()

        last_optimization_at = max(i["optimized_at"] for i in weights)
        return weights, last_optimization_at
