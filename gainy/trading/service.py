from typing import Iterable, Dict

from gainy.plaid.models import PlaidAccessToken
from gainy.plaid.service import PlaidService
from gainy.trading.repository import TradingRepository
from gainy.trading.drivewealth.provider import DriveWealthProvider
from gainy.trading.models import TradingAccount, FundingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingService:
    drivewealth_provider: DriveWealthProvider

    def __init__(self, trading_repository: TradingRepository,
                 drivewealth_provider: DriveWealthProvider,
                 plaid_service: PlaidService):
        self.trading_repository = trading_repository
        self.drivewealth_provider = drivewealth_provider
        self.plaid_service = plaid_service

    def sync_profile_trading_accounts(self, profile_id):
        self._get_provider_service().sync_profile_trading_accounts(profile_id)

    def sync_balances(self, account: TradingAccount):
        self._get_provider_service().sync_balances(account)

    def update_funding_accounts_balance(
            self, funding_accounts: Iterable[FundingAccount]):
        by_at_id = {}
        for funding_account in funding_accounts:
            if not funding_account.plaid_access_token_id:
                continue
            if funding_account.plaid_access_token_id not in by_at_id:
                by_at_id[funding_account.plaid_access_token_id] = []
            by_at_id[funding_account.plaid_access_token_id].append(
                funding_account)

        for plaid_access_token_id, funding_accounts in by_at_id.items():
            access_token = self.trading_repository.find_one(
                PlaidAccessToken, {"id": plaid_access_token_id})
            funding_accounts_by_account_id: Dict[int, FundingAccount] = {
                funding_account.plaid_account_id: funding_account
                for funding_account in funding_accounts
                if funding_account.plaid_account_id
            }

            plaid_accounts = self.plaid_service.get_item_accounts(
                access_token.access_token,
                list(funding_accounts_by_account_id.keys()))
            for plaid_account in plaid_accounts:
                if plaid_account.account_id not in funding_accounts_by_account_id:
                    continue
                funding_accounts_by_account_id[
                    plaid_account.
                    account_id].balance = plaid_account.balance_available

            self.trading_repository.persist(
                funding_accounts_by_account_id.values())

    def _get_provider_service(self):
        return self.drivewealth_provider
