from typing import List

from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthAccountMoney, DriveWealthAccountPositions, DriveWealthAccount, \
    DriveWealthUser, DriveWealthPortfolio, DriveWealthPortfolioStatus

from gainy.trading.drivewealth.api import DriveWealthApi
from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProvider(DriveWealthProviderBase):

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi):
        super().__init__(repository)
        self.api = api

    def sync_user(self, user_ref_id):
        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": user_ref_id}) or DriveWealthUser()

        data = self.api.get_user(user_ref_id)
        user.set_from_response(data)
        self.repository.persist(user)

    def sync_profile_trading_accounts(self, profile_id: int):
        repository = self.repository
        user_ref_id = self._get_user(profile_id).ref_id

        accounts_data = self.api.get_user_accounts(user_ref_id)
        for account_data in accounts_data:
            account_ref_id = account_data["id"]

            account: DriveWealthAccount = repository.find_one(
                DriveWealthAccount,
                {"ref_id": account_ref_id}) or DriveWealthAccount()
            account.set_from_response(account_data)
            repository.persist(account)

            self.sync_trading_account(account_ref_id=account_ref_id)

    def sync_balances(self, account: TradingAccount):
        self.sync_trading_account(trading_account_id=account.id,
                                  fetch_info=True)
        self.sync_portfolios(account.profile_id)

    def sync_trading_account(self,
                             account_ref_id: str = None,
                             trading_account_id: int = None,
                             fetch_info: bool = False):
        repository = self.repository

        _filter = {}
        if account_ref_id:
            _filter["ref_id"] = account_ref_id
        if trading_account_id:
            _filter["trading_account_id"] = trading_account_id
        if not _filter:
            raise Exception("At least one of the filters must be specified")
        account: DriveWealthAccount = repository.find_one(
            DriveWealthAccount, _filter)

        if account:
            account_ref_id = account.ref_id
        else:
            if not account_ref_id:
                return

            account = DriveWealthAccount()
            account.ref_id = account_ref_id
            fetch_info = True

        if fetch_info:
            self._sync_account(account)

        account_money = self._sync_account_money(account_ref_id)

        account_positions_data = self.api.get_account_positions(account_ref_id)
        account_positions = DriveWealthAccountPositions()
        account_positions.set_from_response(account_positions_data)
        repository.persist(account_positions)

        if account.trading_account_id is None:
            return

        trading_account = repository.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if trading_account is None:
            return

        account.update_trading_account(trading_account)
        account_money.update_trading_account(trading_account)
        account_positions.update_trading_account(trading_account)

        repository.persist(trading_account)

    def sync_portfolios(self, profile_id):
        repository = self.repository

        portfolios: List[DriveWealthPortfolio] = repository.find_all(
            DriveWealthPortfolio, {"profile_id": profile_id})
        for portfolio in portfolios:
            self._sync_portfolio(portfolio)
            self._get_portfolio_status(portfolio)

    def _sync_portfolio(self, portfolio: DriveWealthPortfolio):
        data = self.api.get_portfolio(portfolio)
        portfolio.set_from_response(data)
        self.repository.persist(portfolio)

    def _get_portfolio_status(
            self,
            portfolio: DriveWealthPortfolio) -> DriveWealthPortfolioStatus:
        data = self.api.get_portfolio_status(portfolio)
        portfolio_status = DriveWealthPortfolioStatus()
        portfolio_status.set_from_response(data)
        self.repository.persist(portfolio_status)
        return portfolio_status

    def _sync_account(self, account: DriveWealthAccount):
        account_data = self.api.get_account(account.ref_id)
        account.set_from_response(account_data)

        if not self.repository.find_one(
                DriveWealthUser, {"ref_id": account.drivewealth_user_id}):
            self.sync_user(account.drivewealth_user_id)

        self.repository.persist(account)

    def _sync_account_money(self,
                            account_ref_id: str) -> DriveWealthAccountMoney:
        account_money_data = self.api.get_account_money(account_ref_id)
        account_money = DriveWealthAccountMoney()
        account_money.set_from_response(account_money_data)
        self.repository.persist(account_money)
        return account_money
