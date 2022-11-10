from typing import Iterable

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from gainy.trading.models import TradingAccount, FundingAccount
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

            try:
                self.service.sync_balances(account)
                logger.info("Synced trading account %d in %f", account.id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)

        funding_accounts: Iterable[FundingAccount] = self.repo.iterate_all(
            FundingAccount)
        for account in funding_accounts:
            start_time = time.time()

            try:
                self.service.update_funding_accounts_balance([account])
                logger.info("Synced funding account %d in %f", account.id,
                            time.time() - start_time)
            except DriveWealthApiException as e:
                logger.exception(e)


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
