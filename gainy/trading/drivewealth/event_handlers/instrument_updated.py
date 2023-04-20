from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.exceptions import InstrumentNotFoundException
from gainy.trading.drivewealth.models import DriveWealthInstrument
from gainy.utils import get_logger

logger = get_logger(__name__)


class InstrumentUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'instruments.updated'

    def handle(self, event_payload: dict):
        ref_id = event_payload["instrumentID"]

        instrument = self.repo.find_one(DriveWealthInstrument,
                                        {"ref_id": ref_id})
        if instrument:
            data = event_payload['current']
            if "symbol" in data:
                instrument.symbol = data["symbol"]
            if "status" in data:
                self.provider.handle_instrument_status_change(
                    instrument, data["status"])
                instrument.status = data["status"]

            self.repo.persist(instrument)
        else:
            try:
                self.provider.sync_instrument(ref_id=ref_id)
            except InstrumentNotFoundException:
                pass
