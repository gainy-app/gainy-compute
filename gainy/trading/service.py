from gainy.trading.drivewealth.provider import DriveWealthProvider
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingService:
    drivewealth_provider: DriveWealthProvider

    def __init__(self, drivewealth_provider: DriveWealthProvider):
        self.drivewealth_provider = drivewealth_provider

    def sync_profile_trading_accounts(self, profile_id):
        self._get_provider_service().sync_profile_trading_accounts(profile_id)

    def sync_balances(self, account: TradingAccount):
        self._get_provider_service().sync_balances(account)

    def _get_provider_service(self):
        return self.drivewealth_provider
