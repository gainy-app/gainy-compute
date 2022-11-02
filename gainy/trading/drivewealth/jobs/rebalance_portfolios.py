from typing import Iterable

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthApi, DriveWealthRepository
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from gainy.trading.drivewealth.models import DriveWealthPortfolio
from gainy.utils import get_logger

logger = get_logger(__name__)


class RebalancePortfoliosJob:

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider, api: DriveWealthApi):
        self.repo = repo
        self.provider = provider
        self.api = api

    def run(self):
        portfolios: Iterable[DriveWealthPortfolio] = self.repo.iterate_all(
            DriveWealthPortfolio)
        for portfolio in portfolios:
            start_time = time.time()

            try:
                # rebalance portfolio for it to have the wanted amount of cash
                self.provider.rebalance_portfolio_cash(portfolio)
                # rebalance portfolio for it to have the wanted amount of cash
                self.provider.rebalance_portfolio_cash(portfolio)

                self.api.update_portfolio(portfolio)
                portfolio.set_pending_rebalance()
                self.repo.persist(portfolio)

                logger.info("Rebalanced portfolio %s in %f", portfolio.ref_id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)


def cli():
    try:
        with ContextContainer() as context_container:
            job = RebalancePortfoliosJob(
                context_container.drivewealth_repository,
                context_container.drivewealth_provider,
                context_container.drivewealth_api)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
