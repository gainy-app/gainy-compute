from datetime import datetime
from operator import itemgetter
from typing import Iterable

import pandas as pd
from psycopg2._psycopg import connection
from psycopg2.extras import RealDictCursor


class CollectionOptimizerRepository:

    def __init__(self, db_conn: connection):
        self.db_conn = db_conn

    def get_collection_tickers(self, collection_id: int) -> list:
        query = """
            select distinct symbol 
            from raw_data.ticker_collections
                     join (select ttf_name, max(_sdc_extracted_at) as max_sdc_extracted_at from raw_data.ticker_collections group by ttf_name) ticker_collections_stats using (ttf_name)
                     join collections on collections.name = ticker_collections.ttf_name
            where collections.id = %(collection_id)s
              and ticker_collections._sdc_extracted_at >= ticker_collections_stats.max_sdc_extracted_at - interval '1 minute'
            """

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, {"collection_id": collection_id})
            return list(map(itemgetter(0), cursor.fetchall()))

    def get_metrics_df(self, symbols: list) -> pd.DataFrame:
        query = """
            SELECT symbol as ticker, 
                ticker_metrics.market_capitalization/1000000 as marketcap, 
                ticker_metrics.net_income_ttm/1000000 as net_income,
                ticker_metrics.avg_volume_90d /1000000 as avg_vol_mil, 
                ticker_metrics.price_change_1y as ret1y, 
                ticker_metrics.dividend_payout_ratio,
                tickers.gic_group, 
                tickers.gic_industry, 
                tickers.gic_sector, 
                tickers.gic_sub_industry
            FROM ticker_metrics
            INNER JOIN tickers using (symbol)
            WHERE symbol IN %(symbols)s
        """

        params = {"symbols": tuple(symbols)}

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()

        return pd.DataFrame(data)

    def get_last_ticker_price_df(self,
                                 symbols: list,
                                 max_date=None) -> pd.DataFrame:
        if max_date is None:
            max_date = datetime.today().strftime('%Y-%m-%d')

        query = """
            SELECT symbol as ticker, date as max_date, adjusted_close
            FROM historical_prices
            INNER JOIN (
                    SELECT symbol, MAX(date) as date
                    FROM historical_prices
                    WHERE symbol IN %(symbols)s AND date <= %(max_date)s
                    GROUP BY symbol
                ) d using (symbol, date)
        """

        params = {"symbols": tuple(symbols), "max_date": max_date}

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()

        return pd.DataFrame(data)

    def get_ticker_prices_df(self, symbols: list, start, end) -> pd.DataFrame:
        query = """
            SELECT symbol as ticker, date, adjusted_close as price
            FROM historical_prices
            WHERE symbol IN %(symbols)s 
              AND date between %(start)s AND %(end)s
        """

        params = {"symbols": tuple(symbols), "start": start, "end": end}

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()

        df = pd.DataFrame(data)
        df = df.pivot(index='date', columns='ticker',
                      values='price').rename_axis(None, axis=1)
        df.index = pd.to_datetime(df.index)

        return df

    def get_ticker_industry(self, symbols: list, ind_field: str) -> dict:
        query = f"select symbol as ticker, {ind_field} as industry from tickers where symbol in %(symbols)s"

        params = {"symbols": tuple(symbols)}

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()

        df = pd.DataFrame(data)
        return df.set_index('ticker').industry.to_dict()

    def enumerate_collection_ids(self) -> Iterable[int]:
        query = "select distinct id from collections where enabled = '1' and optimization_enabled = 1"

        with self.db_conn.cursor() as cursor:
            cursor.execute(query)
            yield from map(itemgetter(0), cursor)

    def get_collection_name(self, collection_id: int) -> str:
        query = f"select name from collections where id = %(id)s"

        params = {"id": collection_id}

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()[0]
