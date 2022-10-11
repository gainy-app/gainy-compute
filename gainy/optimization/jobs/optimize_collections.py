import argparse
import os
import traceback
import datetime

import dateutil.parser
import pandas as pd

from gainy.context_container import ContextContainer
from gainy.optimization.collection.optimizer.portfolio_risk_budget_optimizer import \
    PortfolioRiskBudgetCollectionOptimizer
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.optimization.collection import CollectionTickerFilter
from gainy.utils import get_logger

logger = get_logger(__name__)


class OptimizeCollectionsJob:
    params = {
        'bounds': (0.01, 0.3),
        'penalties': {
            'hs': 0.005,
            'hi': 0.005,
            'b': 0.05
        }
    }
    repository: CollectionOptimizerRepository

    def __init__(self, repository: CollectionOptimizerRepository):
        self.repository = repository
        self.tickers_filter = CollectionTickerFilter(repository)

    def run(self, collection_id: int, date: datetime.date,
            output_filename: str):
        if collection_id:
            collection_ids = [collection_id]
        else:
            collection_ids = self.repository.enumerate_collection_ids()

        for collection_id in collection_ids:
            opt_res = self._optimize_collection(collection_id, date)
            df = self._opt_res_to_df(collection_id, opt_res, date)
            df.to_csv(output_filename,
                      index=False,
                      mode='a',
                      header=(not os.path.exists(output_filename)))

    def _optimize_collection(self, collection_id: int, date: datetime.date):
        tickers = self.repository.get_collection_tickers(collection_id)
        logger.info("Using tickers %s", tickers)

        tickers = self.tickers_filter.filter(tickers)
        logger.info("Tickers after filtering %s", tickers)

        optimizer = PortfolioRiskBudgetCollectionOptimizer(self.repository,
                                                           tickers,
                                                           date,
                                                           benchmark='SPY',
                                                           lookback=9,
                                                           **self.params)
        opt_res = optimizer.optimize()
        logger.info("Optimization result %s", opt_res)

        return opt_res

    def _opt_res_to_df(self, collection_id: int, opt_res: dict,
                       date: datetime.date) -> pd.DataFrame:
        opt_res = pd.DataFrame.from_dict(opt_res, orient="index").reset_index()
        opt_res.columns = ['symbol', 'weight']
        opt_res['date'] = datetime.datetime.strftime(date, "%Y-%m-%d")
        opt_res['ttf_id'] = collection_id
        opt_res['optimized_at'] = datetime.datetime.now()

        return opt_res


def cli(args=None):
    parser = argparse.ArgumentParser(
        description='Update recommendations for all profiles.')
    parser.add_argument('--id',
                        dest='collection_id',
                        type=int,
                        help='Collection id')
    parser.add_argument('-d', '--max-date', dest='date', type=str)
    parser.add_argument('-o',
                        '--output',
                        dest='output_filename',
                        type=str,
                        required=True)
    args = parser.parse_args(args)

    collection_id = args.collection_id
    if args.date:
        date = dateutil.parser.parse(args.date)
    else:
        date = datetime.date.today().replace(day=1)
    output_filename = args.output_filename

    try:
        with ContextContainer() as context_container:
            job = OptimizeCollectionsJob(
                context_container.collection_optimizer_repository)
            job.run(collection_id=collection_id,
                    date=date,
                    output_filename=output_filename)

    except Exception as e:
        traceback.print_exc()
        raise e
