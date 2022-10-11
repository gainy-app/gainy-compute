import datetime
from abc import ABC

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
import scipy.optimize as sco
from sklearn.linear_model import LinearRegression

from gainy.optimization.collection.optimizer.abstract_optimizer import AbstractCollectionOptimizer
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class SharpeCollectionOptimizer(AbstractCollectionOptimizer):

    def __init__(self, repository: CollectionOptimizerRepository, tickers, date_today: datetime.date, lookback=9, benchmark='SPY',
                 industry_type='gic_sector', penalties=None, target_beta=1, bounds=(0, 1)) -> None:
        """
        Penalties - penalty coefficients dictionary
            - hs - HHI index penalty for stocks
            - hi - HHI index penalty for industries
            - b - beta over target penalty

        Bounds - tupple with minimum and maximum stock weight (default = (0,1))

        TargetBeta - float with target portfolio beta (default = 1)
        """

        penalties = penalties.copy() or {'hs': 1.0, 'hi': 1.0, 'b': 1.0}
        super().__init__(repository, tickers, date_today, lookback, benchmark, industry_type, penalties, target_beta)

        # Override params
        if bounds[1] * len(tickers) > 1:  # To avoid lack of solution for short list
            self.bounds = bounds
        else:
            self.bounds = (bounds[0], 1)

    def optimize(self):
        stock_metrics = self._get_stock_metrics()

        r = stock_metrics['Numerator']
        cov = stock_metrics['Covariance']
        industries = stock_metrics['Industry']
        betas = stock_metrics['Betas']

        # Check that all inputs are aligned and keep arrays
        tickers = cov.columns
        r = np.array([r[ticker] for ticker in tickers])
        industries = np.array([industries[ticker] for ticker in tickers])
        betas = np.array([betas[ticker] for ticker in tickers])
        sigma = cov.values

        # Define functions for optimization
        def numerator(weights):
            return np.sum(r * weights)

        def portfolio_sd(weights):
            return np.sqrt(np.transpose(weights) @ (sigma) @ weights)

        def obj_fun(weights):
            fnc = (numerator(weights) / portfolio_sd(weights)) - self.hhi_stock(weights) - self.hhi_ind(weights, industries) - self.beta_pen(weights, betas)
            return - fnc  # Minus to turn into minimization problem

        # Constraints
        constraints = {'type': 'eq', 'fun': lambda x: np.sum(x) - 1}  # Fully invested
        bounds = tuple([self.bounds] * len(r))

        opt_res = sco.minimize(
            fun=obj_fun,  # Objective
            x0=np.repeat(1 / len(r), len(r)),  # Initial guess - equal weighted
            method='SLSQP',
            bounds=bounds,
            constraints=constraints
        )
        out = pd.DataFrame({'Weight': opt_res.x}, index=tickers).sort_values('Weight', ascending=False)

        weights = np.repeat(1 / len(r), len(r))
        logger.info('Finished Sharpe optimization', extra={
                  "Success": opt_res.success,
                  "Weights": out.to_dict('records'),
                  "Objective function components with equal weights": {
                      "Value": numerator(weights) / portfolio_sd(weights),
                      "Stock HHI": self.hhi_stock(weights),
                      "Industry HHI": self.hhi_ind(weights, industries),
                      "Beta penalty": self.beta_pen(weights, betas),
                  },
                  "Objective function components with optimized weights": {
                      "Numerator": np.round(numerator(opt_res.x), 4),
                      "Denom": np.round(portfolio_sd(opt_res.x), 4),
                      "Value": numerator(opt_res.x) / portfolio_sd(opt_res.x),
                      "Stock HHI": self.hhi_stock(opt_res.x),
                      "Industry HHI": self.hhi_ind(opt_res.x, industries),
                      "Beta penalty": self.beta_pen(opt_res.x, betas),
                  }
        })

        return out.Weight.to_dict()
