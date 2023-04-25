from gainy.tests.mocks.repository_mocks import mock_persist
from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthInstrument
from gainy.trading.drivewealth.event_handlers import InstrumentCreatedEventHandler


def test(monkeypatch):
    instrument_id = "instrument_id"
    symbol = "symbol"
    status = "INACTIVE"

    repo = DriveWealthRepository(None)

    persisted_objects = {}
    monkeypatch.setattr(repo, 'persist', mock_persist(persisted_objects))

    event_handler = InstrumentCreatedEventHandler(repo, None, None, None)

    message = {
        "instrumentID": instrument_id,
        "symbol": symbol,
        "status": status,
    }
    event_handler.handle(message)

    assert DriveWealthInstrument in persisted_objects
    assert persisted_objects[DriveWealthInstrument]

    instrument: DriveWealthInstrument = persisted_objects[
        DriveWealthInstrument][0]
    assert instrument.ref_id == instrument_id
    assert instrument.symbol == symbol
    assert instrument.status == status
