import os
from typing import List

from gainy.analytics.service import AnalyticsService
from gainy.queue_processing.abstract_message_handler import AbstractMessageHandler
from gainy.queue_processing.models import QueueMessage
from gainy.services.notification import NotificationService
import gainy.trading.drivewealth.event_handlers
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.repository import TradingRepository

DRIVEWEALTH_SQS_ARN = os.getenv("DRIVEWEALTH_SQS_ARN")


class DriveWealthQueueMessageHandler(AbstractMessageHandler):
    handlers: List[AbstractDriveWealthEventHandler]

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 trading_repository: TradingRepository,
                 analytics_service: AnalyticsService,
                 notification_service: NotificationService):
        self.handlers = [
            cls(repo, provider, trading_repository, analytics_service,
                notification_service) for cls in self._iterate_module_classes(
                    gainy.trading.drivewealth.event_handlers)
            if issubclass(cls, AbstractDriveWealthEventHandler)
        ]

    def supports(self, message: QueueMessage) -> bool:
        return message.source_ref_id == DRIVEWEALTH_SQS_ARN

    def handle(self, message: QueueMessage):
        body = message.body

        message.source_event_ref_id = body["id"]
        event_type = body["type"]
        event_payload = body["payload"]

        self._get_handler(event_type).handle(event_payload)
        message.handled = True
