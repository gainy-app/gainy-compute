import datetime

import pandas as pd
import numpy as np
import scipy.optimize as sco

from gainy.optimization.collection.optimizer.abstract_optimizer import AbstractCollectionOptimizer
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class PortfolioRiskBudgetCollectionOptimizer(AbstractCollectionOptimizer):

    def __init__(self,
                 repository: CollectionOptimizerRepository,
                 date_today: datetime.date,
                 lookback=9,
                 benchmark='SPY',
                 industry_type='gic_sector',
                 penalties=None,
                 target_beta=1,
                 bounds=(0, 1)) -> None:
        """
        Penalties - penalty coefficients dictionary
            - hs - HHI index penalty for stocks
            - hi - HHI index penalty for industries
            - b - beta over target penalty

        Bounds - tupple with minimum and maximum stock weight (default = (0,1))

        TargetBeta - float with target portfolio beta (default = 1)
        """

        penalties = penalties.copy() or {'hs': 1.0, 'hi': 1.0, 'b': 1.0}
        super().__init__(repository, date_today, lookback, benchmark,
                         industry_type, penalties, target_beta)

        self.bounds = bounds

    def optimize(self, tickers) -> dict:
        stock_metrics = self._get_stock_metrics(tickers)

        cov = stock_metrics['Covariance']
        industries = stock_metrics['Industry']
        betas = stock_metrics['Betas']

        # Check that all inputs are aligned and keep arrays
        tickers = cov.columns
        industries = np.array([industries[ticker] for ticker in tickers])
        betas = np.array([betas[ticker] for ticker in tickers])
        sigma = cov.values

        # Equal risk budget
        w_t = [1 / len(tickers)] * len(tickers)

        # Define functions for optimization

        def portfolio_sd(weights):
            return np.sqrt(np.transpose(weights) @ sigma @ weights)

        # Risk contribution of assets
        def risk_contribution(weights):
            # function that calculates asset contribution to total risk
            w = np.matrix(weights)
            portvol = portfolio_sd(weights)

            # Marginal Risk Contribution
            MRC = sigma * w.T

            # Risk Contribution
            RC = np.multiply(MRC, w.T)

            if abs(portvol) > 1e-10:
                RC /= portvol

            logger.info('risk_contribution',
                        extra={
                            "weights": weights,
                            "portvol": portvol,
                            "MRC": MRC,
                            "RC": RC,
                        })

            return RC

        def risk_budget_obj(weights):
            portvol = portfolio_sd(weights)
            risk_target = np.asmatrix(np.multiply(portvol, w_t))
            asset_RC = risk_contribution(weights)
            RB = sum(np.square(asset_RC -
                               risk_target.T))[0, 0]  # sum of squared error
            logger.info('risk_budget_obj',
                        extra={
                            "weights": weights,
                            "portvol": portvol,
                            "risk_target": risk_target,
                            "asset_RC": asset_RC,
                            "RB": RB,
                        })
            return RB

        def obj_fun(weights):
            RB = risk_budget_obj(weights)
            fnc = RB + self.hhi_stock(weights) + self.hhi_ind(
                weights, industries) + self.beta_pen(weights, betas)
            return fnc

        # Constraints

        constraints = {
            'type': 'eq',
            'fun': lambda x: np.sum(x) - 1
        }  # Fully invested

        bounds = tuple([self.bounds] * len(tickers))
        # To avoid lack of solution for short list
        if bounds[1] * len(tickers) <= 1:
            bounds = (bounds[0], 1)

        opt_res = sco.minimize(
            fun=obj_fun,  # Objective
            x0=np.repeat(1 / len(tickers),
                         len(tickers)),  # Initial guess - equal weighted
            method='SLSQP',
            bounds=bounds,
            constraints=constraints)
        out = pd.DataFrame({
            'Weight': opt_res.x
        }, index=tickers).sort_values('Weight', ascending=False)

        weights = np.repeat(1 / len(tickers), len(tickers))

        logger.info('Finished Risk budget optimization',
                    extra={
                        "Success": opt_res.success,
                        "Weights": out.Weight.to_dict(),
                        "Objective function components with equal weights": {
                            "Risk budget": risk_budget_obj(weights),
                            "Stock HHI": self.hhi_stock(weights),
                            "Industry HHI": self.hhi_ind(weights, industries),
                            "Beta penalty": self.beta_pen(weights, betas),
                        },
                        "Objective function components with optimized weights":
                        {
                            "Risk contribution": risk_contribution(opt_res.x),
                            "Risk budget": risk_budget_obj(opt_res.x),
                            "Stock HHI": self.hhi_stock(opt_res.x),
                            "Industry HHI":
                            self.hhi_ind(opt_res.x, industries),
                            "Beta penalty": self.beta_pen(opt_res.x, betas),
                        }
                    })

        return out.Weight.to_dict()
