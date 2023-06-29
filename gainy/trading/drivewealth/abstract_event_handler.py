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
