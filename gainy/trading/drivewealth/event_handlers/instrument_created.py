from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthInstrument
from gainy.utils import get_logger

logger = get_logger(__name__)


class InstrumentCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'instruments.created'

    def handle(self, event_payload: dict):
        instrument = DriveWealthInstrument()
        instrument.set_from_response(event_payload)
        self.repo.persist(instrument)
