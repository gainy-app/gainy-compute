from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.utils import get_logger

logger = get_logger(__name__)


class NoopEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in [
            'auth.tokens.created',
            'deposits.created',
            'kyc.created',
            'mam.allocationlist.accepted',
            'mam.allocationlist.complete',
        ]

    def handle(self, event_payload: dict):
        pass
