import json

from _decimal import Decimal

import pytest

from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthAccountPositions

DATA = json.loads(
    '{"accountID": "b8454998-2639-4bce-a0f2-af39d2f78c78.1669379947853", "accountNo": "GYZE000003", "tradingType": "CASH", "equityValue": 2386.32, "updated": "2023-05-10T06:00:45.969Z", "equityPositions": [{"symbol": "MVST", "instrumentID": "bba0f97a-1e5d-4263-8bca-6aca9bdba323", "openQty": 936.68786091, "availableForWithdrawalQty": null, "costBasis": 1018.55, "marketValue": 1227.06, "side": "B", "priorClose": 1.19, "availableForTradingQty": 936.68786091, "avgPrice": 1.09, "mktPrice": 1.31, "unrealizedPL": 208.51, "unrealizedDayPLPercent": 10.08, "unrealizedDayPL": 112.4}, {"symbol": "QS", "instrumentID": "fc2623de-a762-4d65-b31b-79d50cdfcf8f", "openQty": 186.0762961, "availableForWithdrawalQty": null, "costBasis": 1347.26, "marketValue": 1159.26, "side": "B", "priorClose": 6.39, "availableForTradingQty": 186.0762961, "avgPrice": 7.24, "mktPrice": 6.23, "unrealizedPL": -188.0, "unrealizedDayPLPercent": -2.5, "unrealizedDayPL": -29.77}]}'
)


def test_get_symbol_market_price(monkeypatch):
    entity = DriveWealthAccountPositions()
    entity.set_from_response(DATA)

    assert entity.get_symbol_market_price('MVST') == Decimal(1.31)

    with pytest.raises(Exception) as e:
        entity.get_symbol_market_price('NONAME')
        assert isinstance(e, EntityNotFoundException)
