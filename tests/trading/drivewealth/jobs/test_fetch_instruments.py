from gainy.data_access.operators import OperatorNot, OperatorIn
from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_persist
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthApi
from gainy.trading.drivewealth.jobs.fetch_instruments import FetchInstrumentsJob
from gainy.trading.drivewealth.models import DriveWealthInstrumentStatus, DriveWealthInstrument

_instrument_ref_id = "instrument_ref_id"
_instrument_symbol = "symbol"
_instrument_status = DriveWealthInstrumentStatus.ACTIVE.name


def mock_get_instruments(status):
    assert status == _instrument_status
    return [{
        "instrumentID": _instrument_ref_id,
        "symbol": _instrument_symbol,
        "status": _instrument_status,
    }]


def test_fetch_instruments(monkeypatch):
    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))
    delete_by_calls = []
    monkeypatch.setattr(repository, "delete_by",
                        mock_record_calls(delete_by_calls))

    api = DriveWealthApi(None)
    monkeypatch.setattr(api, "get_instruments", mock_get_instruments)

    job = FetchInstrumentsJob(repository, api)

    job.run()

    assert DriveWealthInstrument in persisted_objects

    instrument: DriveWealthInstrument = persisted_objects[
        DriveWealthInstrument][0]
    assert instrument.ref_id == _instrument_ref_id
    assert instrument.symbol == _instrument_symbol
    assert instrument.status == _instrument_status

    assert (DriveWealthInstrument,
            [OperatorNot(OperatorIn([_instrument_ref_id])).to_sql("ref_id")
             ]) in [(args[0], [i.to_sql(k) for k, i in args[1].items()])
                    for args, kwargs in delete_by_calls]
