from abc import ABC
from typing import Optional

from gainy.analytics.service import AnalyticsService
from gainy.queue_processing.abstract_event_handler import EventHandlerInterface
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.models import TradingAccount
from gainy.services.notification import NotificationService
from gainy.trading.repository import TradingRepository


class AbstractDriveWealthEventHandler(EventHandlerInterface, ABC):

    def __init__(self,
                 repo: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 trading_repository: TradingRepository,
                 analytics_service: AnalyticsService,
                 notification_service: NotificationService = None):
        self.repo = repo
        self.provider = provider
        self.trading_repository = trading_repository
        self.analytics_service = analytics_service
        self.notification_service = notification_service

    def sync_trading_account_balances(
            self,
            trading_account_ref_id: str,
            force: bool = False) -> Optional[TradingAccount]:
        if not trading_account_ref_id:
            return

        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": trading_account_ref_id})
        if not account or not account.trading_account_id:
            return

        trading_account = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return

        self.provider.sync_balances(trading_account, force=force)
        return trading_account
