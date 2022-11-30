import datetime

from gainy.optimization.collection.optimizer.portfolio_risk_budget_optimizer import PortfolioRiskBudgetCollectionOptimizer
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class InflationProofPortfolioRiskBudgetCollectionOptimizer(
        PortfolioRiskBudgetCollectionOptimizer):

    def __init__(self, repository: CollectionOptimizerRepository, tickers,
                 date_today: datetime.date, **kwargs) -> None:
        super().__init__(repository, tickers, date_today, **kwargs)

    def optimize(self, tickers) -> dict:
        # Keep optimizing while there are still weights less than weight_threshold

        weight_threshold = self.bounds[0]
        for i in range(len(tickers)):
            tickers_pre = tickers
            opt_res = super().optimize(tickers)
            min_weight = min(opt_res.values())

            if min_weight >= weight_threshold:
                return opt_res

            tickers = [
                k for k, weight in opt_res.items() if weight > weight_threshold
            ]
            logger.info('Inflation proof optimization iteration',
                        extra={
                            "iteration": i,
                            "opt_res": opt_res,
                            "tickers_pre": tickers_pre,
                            "tickers_after": tickers,
                            "min_weight": min_weight,
                        })

        raise Exception('Not able to optimize within iterations limit.')
