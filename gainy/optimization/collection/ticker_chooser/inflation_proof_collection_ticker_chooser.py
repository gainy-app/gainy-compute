import pandas as pd
from psycopg2.extras import RealDictCursor

from gainy.optimization.collection.ticker_chooser import AbstractCollectionTickerChooser

INFLATION_PROOF_COLLECTION_ID = 275


class InflationProofCollectionTickerChooser(AbstractCollectionTickerChooser):

    def get_tickers(self, collection_id: int) -> list[str]:
        groups_to_select = [
            'Utilities', 'Banks', 'Insurance', 'Real Estate', 'Energy',
            'Food & Staples Retailing'
        ]

        query = f"""
            SELECT ticker_metrics.symbol, 
                   ticker_metrics.market_capitalization, 
                   base_tickers.gic_group
            FROM base_tickers
                     LEFT JOIN ticker_metrics using (symbol)
            where base_tickers.gic_group IN %(groups_to_select)s
        """

        with self.repository.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {"groups_to_select": groups_to_select})
            data = cursor.fetchall()

        df = pd.DataFrame(data)
        df = df.sort_values(
            ['gic_group', 'market_capitalization'],
            ascending=False).groupby('gic_group').head(5).reset_index()
        return list(df.symbol)

    def supports_collection(self, collection_id: int) -> bool:
        return collection_id == INFLATION_PROOF_COLLECTION_ID
