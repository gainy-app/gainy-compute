from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthAccountMoney, DriveWealthAccountPositions, DriveWealthAccount

from gainy.trading.drivewealth.api import DriveWealthApi
from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProvider(DriveWealthProviderBase):

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi):
        super().__init__(repository)
        self.api = api

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

        if not account:
            if not account_ref_id:
                return

            account = DriveWealthAccount()
            account_ref_id = account.ref_id
            fetch_info = True

        if fetch_info:
            account_data = self.api.get_account(account_ref_id)
            account.set_from_response(account_data)
            repository.persist(account)

        account_money_data = self.api.get_account_money(account_ref_id)
        account_money = DriveWealthAccountMoney()
        account_money.set_from_response(account_money_data)
        repository.persist(account_money)

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
