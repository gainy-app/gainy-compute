import numpy as np

from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class CollectionTickerFilter:

    def __init__(self, repository: CollectionOptimizerRepository):
        self.repository = repository

    def filter(self,
               tickers,
               min_market_cap=100,
               min_volume=2,
               min_price=1) -> list:
        """
        Filters stocks in a ttf given tickers and the following logic:

        1. MarketCap > 100 mln
        2. Average daily volume > $2mln (note Dollars not Shares)
        3. Price > $1 per share
        4. Lst available price date == max of all tickers
        """

        df = self.repository.get_metrics_df(tickers)
        df = df[['ticker', 'marketcap', 'avg_vol_mil']]

        lp = self.repository.get_last_ticker_price_df(tickers)
        df = df.merge(lp, on='ticker', how='inner', copy=False)
        df['vol_doll'] = df.avg_vol_mil * df.adjusted_close

        dw_ref_ids = self.repository.get_dw_ref_ids(tickers)
        df = df.merge(dw_ref_ids, on='ticker', how='left', copy=False)

        # Filtering logic
        df['Flag'] = ''
        df.loc[df.marketcap < min_market_cap, 'Flag'] += '-MC'
        df.loc[df.adjusted_close < min_price, 'Flag'] += '-Price'
        df.loc[df.vol_doll < min_volume, 'Flag'] += '-Volume'

        # df.loc[df.ref_id.isna(), 'Flag'] += '-DW'
        # while in DW UAT environment we hard-code the list of unsupported tickers
        # TODO remove after UAT
        missing_tickers = [
            'CTXS', 'WRE', 'NLOK', 'LFC', 'DRE', 'ZEN', 'WULF', 'TEN', 'ARBK',
            'HNRG'
        ]
        df.loc[df.ticker.isin(missing_tickers), 'Flag'] += '-DW'

        maxdt = df.max_date.max()
        df.loc[df.max_date < maxdt,
               'Flag'] = str(df.loc[df.max_date < maxdt, 'Flag']) + '-Date'

        logger.info("Filtering data",
                    extra={
                        "tickers": tickers,
                        "data": dict(zip(df.ticker, df.Flag))
                    })

        return df.loc[df.Flag == '', 'ticker'].tolist()
