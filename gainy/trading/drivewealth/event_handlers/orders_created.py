from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthOrder
from gainy.utils import get_logger

logger = get_logger(__name__)


class OrderCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'orders.created'

    def handle(self, event_payload: dict):
        order = DriveWealthOrder()
        order.set_from_response(event_payload)
        self.repo.persist(order)

        self.provider.handle_order(order)
