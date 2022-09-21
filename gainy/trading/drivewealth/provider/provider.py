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

    def sync_trading_accounts(self, profile_id: int):
        repository = self.repository
        user_ref_id = self._get_user(profile_id).ref_id

        accounts_data = self.api.get_user_accounts(user_ref_id)
        for account_data in accounts_data:
            account_ref_id = account_data["id"]
            account_money_data = self.api.get_account_money(account_ref_id)
            account_money = DriveWealthAccountMoney()
            account_money.set_from_response(account_money_data)
            repository.persist(account_money)

            account_positions_data = self.api.get_account_positions(
                account_ref_id)
            account_positions = DriveWealthAccountPositions()
            account_positions.set_from_response(account_positions_data)
            repository.persist(account_positions)

            account: DriveWealthAccount = repository.find_one(
                DriveWealthAccount,
                {"ref_id": account_ref_id}) or DriveWealthAccount()
            account.drivewealth_user_id = user_ref_id
            account.set_from_response(account_data)
            repository.persist(account)

            if account.trading_account_id is None:
                continue

            trading_account = repository.find_one(
                TradingAccount, {"id": account.trading_account_id})
            if trading_account is None:
                continue

            account.update_trading_account(trading_account)
            account_money.update_trading_account(trading_account)
            account_positions.update_trading_account(trading_account)

            repository.persist(trading_account)
