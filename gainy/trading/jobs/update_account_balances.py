from typing import Iterable

import time

from gainy.context_container import ContextContainer
from gainy.trading.models import TradingAccount
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService
from gainy.utils import get_logger

logger = get_logger(__name__)


class UpdateAccountBalancesJob:

    def __init__(self, repo: TradingRepository, service: TradingService):
        self.repo = repo
        self.service = service

    def run(self):
        trading_accounts: Iterable[TradingAccount] = self.repo.iterate_all(
            TradingAccount)
        for account in trading_accounts:
            start_time = time.time()
            self.service.sync_trading_accounts(account.profile_id)

            logger.info("Synced accounts of profile %s in %f",
                        account.profile_id,
                        time.time() - start_time)


def cli():
    try:
        with ContextContainer() as context_container:
            job = UpdateAccountBalancesJob(
                context_container.trading_repository,
                context_container.trading_service)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
