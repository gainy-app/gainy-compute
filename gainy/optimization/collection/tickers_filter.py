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

        df = df.merge(lp, on='ticker', how='inner')
        df['vol_doll'] = df.avg_vol_mil * df.adjusted_close

        # Filtering logic
        df['Flag'] = np.NaN

        df.loc[df.marketcap < min_market_cap, 'Flag'] = str(
            df.loc[df.marketcap < min_market_cap, 'Flag'].values) + '- MC'
        df.loc[df.adjusted_close < min_price, 'Flag'] = str(
            df.loc[df.adjusted_close < min_price, 'Flag'].values) + '-Price'
        df.loc[df.vol_doll < min_volume, 'Flag'] = str(
            df.loc[df.vol_doll < min_volume, 'Flag'].values) + '-Volume'

        maxdt = df.max_date.max()
        df.loc[df.max_date < maxdt,
               'Flag'] = str(df.loc[df.max_date < maxdt, 'Flag']) + '-Date'

        # df.Flag = df.Flag.str.replace('\[nan\]-', '')
        logger.debug("Filtering data", extra={"data": df.to_dict('records')})

        return df.loc[df.Flag.isna(), 'ticker'].tolist()
