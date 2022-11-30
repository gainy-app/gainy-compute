from operator import itemgetter

from gainy.optimization.collection.ticker_chooser import AbstractCollectionTickerChooser

INFLATION_PROOF_COLLECTION_ID = 275


class InflationProofCollectionTickerChooser(AbstractCollectionTickerChooser):

    def get_tickers(self, collection_id: int) -> list[str]:
        groups_to_select = [
            'Utilities', 'Banks', 'Insurance', 'Real Estate', 'Energy',
            'Food & Staples Retailing'
        ]

        query = f"""
            select distinct symbol
            from (
                     SELECT symbol,
                            row_number() over (partition by gic_group order by market_capitalization desc nulls last) as rn
                     FROM tickers
                              LEFT JOIN ticker_metrics using (symbol)
                     where tickers.gic_group IN %(groups_to_select)s
                 ) t
            where rn <= 5
        """

        with self.repository.db_conn.cursor() as cursor:
            cursor.execute(query,
                           {"groups_to_select": tuple(groups_to_select)})
            return list(map(itemgetter(0), cursor.fetchall()))

    def supports_collection(self, collection_id: int) -> bool:
        return collection_id == INFLATION_PROOF_COLLECTION_ID
