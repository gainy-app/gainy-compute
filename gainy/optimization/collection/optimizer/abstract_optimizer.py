import datetime
from abc import ABC

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from sklearn.linear_model import LinearRegression

from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class AbstractCollectionOptimizer(ABC):

    def __init__(self,
                 repository: CollectionOptimizerRepository,
                 date_today: datetime.date,
                 lookback=9,
                 benchmark='SPY',
                 industry_type='gic_sector',
                 penalties=None,
                 target_beta=1) -> None:
        self.repository = repository
        self.dt = date_today  # Date of optimization
        self.start_dt = self.dt - relativedelta(months=lookback)
        self.lookback = lookback
        self.benchmark = benchmark
        self.ind_type = industry_type
        self.penalties = penalties
        self.target_beta = target_beta

    def _get_stock_returns(self, tickers) -> pd.DataFrame:
        """
        Returns stock returns for the selected lookback window
        """

        tickers = tickers + [self.benchmark]

        rets = self.repository.get_ticker_prices_df(
            tickers, self.start_dt - relativedelta(days=5),
            self.dt).pct_change()
        rets = rets[str(self.start_dt):str(self.dt):]

        # Check that every ticker has at least 80% of non-nas
        min_obs = rets.shape[0]
        obs = rets.count()

        missing = obs[obs < min_obs]

        if missing.shape[0] > 0:
            logger.warning(
                "The following tickers have less than 80%% of price observations: %s. They will be dropped",
                missing.index.values)
            rets = rets.drop(missing.index, axis=1)

        # Check that we have data for all names
        missing_tickers = list(set(tickers) - set(rets.columns))
        if len(missing_tickers) > 0:
            logger.warning(
                "We do not support the following tickers %s. They will be dropped from the optimization",
                missing_tickers)

        return rets

    def _get_stock_metrics(self, tickers):
        """
        Get key metrics for optimization and create nested dictionary for optimization
        """

        rets = self._get_stock_returns(tickers + [self.benchmark])
        rets.index = rets.index.strftime('%Y-%m-%d')
        logger.info('rets', extra={"rets": rets.to_dict('index')})

        bm = rets[self.benchmark]
        rets = rets.drop(self.benchmark, axis=1)

        # Covariance
        cov = rets.cov() * 252

        industries = self.repository.get_ticker_industry(
            tickers, self.ind_type)

        # Get betas
        betas = dict()
        for ticker in rets.columns:
            tmp = rets[[ticker]].merge(bm,
                                       left_index=True,
                                       right_index=True,
                                       how='left').dropna()

            X = tmp.iloc[:, 1].values.reshape(-1, 1)
            y = tmp.iloc[:, 0].values

            coef = LinearRegression(fit_intercept=True).fit(X, y).coef_[0]
            truncate = (-3, 3)  # Truncate betas in case of crazy numbers
            coef = truncate[0] if coef < truncate[0] else truncate[
                1] if coef > truncate[1] else coef
            betas[ticker] = coef

        logger.info('betas', extra={"betas": betas})

        return {'Covariance': cov, 'Industry': industries, 'Betas': betas}

    def hhi_stock(self, weights):
        return np.sum(weights**
                      2) * self.penalties['hs']  # HHI concentration index

    def hhi_ind(self, weights, industries):
        tmp = pd.DataFrame({
            'W': weights,
            'Ind': industries
        },
                           index=range(len(industries)))
        tmp = tmp.groupby('Ind').W.sum().values

        return np.sum(tmp**2) * self.penalties['hi']  # HHI concentration index

    def beta_pen(self, weights, betas):
        port_beta = np.sum(betas * weights)
        return ((self.target_beta - port_beta)**2) * self.penalties['b']
