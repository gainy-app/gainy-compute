from typing import Iterable

import time

from gainy.context_container import ContextContainer
from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.exceptions import InvalidDriveWealthPortfolioStatusException, \
    TradingAccountNotOpenException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount
from gainy.trading.models import TradingAccount, FundingAccount
from gainy.trading.service import TradingService
from gainy.utils import get_logger

logger = get_logger(__name__)


class UpdateAccountBalancesJob:

    def __init__(self, repo: DriveWealthRepository, service: TradingService):
        self.repo = repo
        self.service = service

    def run(self):
        self._update_trading_accounts()
        self._update_funding_accounts()
        self._update_portfolios()

    def _update_trading_accounts(self):
        trading_accounts: Iterable[TradingAccount] = self.repo.iterate_all(
            TradingAccount)
        for account in trading_accounts:
            if account.is_artificial:
                continue
            start_time = time.time()

            try:
                self.service.sync_balances(account)
                logger.info("Synced trading account %d in %f", account.id,
                            time.time() - start_time)
            except Exception as e:
                logger.exception(e)

    def _update_funding_accounts(self):
        funding_accounts: Iterable[FundingAccount] = self.repo.iterate_all(
            FundingAccount)
        for account in funding_accounts:
            start_time = time.time()

            try:
                self.service.update_funding_accounts_balance([account])
                logger.info("Synced funding account %d in %f", account.id,
                            time.time() - start_time)
            except Exception as e:
                logger.exception(e)

    def _update_portfolios(self):
        portfolios: Iterable[DriveWealthPortfolio] = self.repo.iterate_all(
            DriveWealthPortfolio)

        for portfolio in portfolios:
            if portfolio.is_artificial:
                continue
            account: DriveWealthAccount = self.repo.find_one(
                DriveWealthAccount,
                {"ref_id": portfolio.drivewealth_account_id})
            if not account or not account.is_open():
                continue

            start_time = time.time()

            try:
                self.service.drivewealth_provider.sync_portfolio(portfolio)
                self.service.drivewealth_provider.sync_portfolio_status(
                    portfolio)
                logger.info("Synced portfolio %s in %f", portfolio.ref_id,
                            time.time() - start_time)
            except TradingAccountNotOpenException:
                pass
            except InvalidDriveWealthPortfolioStatusException as e:
                logger.warning(e,
                               extra={
                                   "profile_id": portfolio.profile_id,
                                   "account_id":
                                   portfolio.drivewealth_account_id
                               })
            except Exception as e:
                logger.exception(e)


def cli():
    try:
        with ContextContainer() as context_container:
            job = UpdateAccountBalancesJob(
                context_container.drivewealth_repository,
                context_container.trading_service)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
