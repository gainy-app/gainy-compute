from gainy.trading.drivewealth.models import DriveWealthFund


def test_get_instrument_ids(monkeypatch):
    fund_instrument_ids = ["active_instrument_id", "inactive_instrument_id"]

    fund = DriveWealthFund()
    fund.holdings = [{"instrumentID": i} for i in fund_instrument_ids]

    assert fund.get_instrument_ids() == fund_instrument_ids


def test_remove_instrument_ids(monkeypatch):
    active_instrument_id = "active_instrument_id"
    inactive_instrument_id = "inactive_instrument_id"
    inactive_instrument_id2 = "inactive_instrument_id2"
    fund_instrument_ids = [
        active_instrument_id, inactive_instrument_id, inactive_instrument_id2
    ]
    inactive_instrument_ids = [inactive_instrument_id, inactive_instrument_id2]
    active_instrument_ids = [active_instrument_id]

    fund = DriveWealthFund()
    fund.holdings = [{"instrumentID": i} for i in fund_instrument_ids]

    assert fund.get_instrument_ids() == fund_instrument_ids
    assert len(fund.holdings) == len(fund_instrument_ids)

    fund.remove_instrument_ids(inactive_instrument_ids)

    assert fund.get_instrument_ids() == active_instrument_ids
    assert len(fund.holdings) == len(active_instrument_ids)
