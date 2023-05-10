import json

import pytest

from gainy.trading.drivewealth.models import DriveWealthTransaction, \
    DriveWealthSpinOffTransaction, DriveWealthDividendTransaction


def get_transaction_test_data():
    return [
        (
            DriveWealthDividendTransaction,
            json.loads(
                '{"accountAmount": 0.82, "accountBalance": 1834.56, "comment": "SCHN dividend, $0.1875/share", "finTranID": "KE.599b13b2-386c-4855-8f9d-6983d17780a3", "wlpFinTranTypeID": "e10397ec-6fb8-41f6-86df-3b3f4745ffcb", "finTranTypeID": "DIV", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "instrument": {"id": "522f3cb8-ea8e-4085-b11c-3fbcd994ca42", "symbol": "SCHN", "name": "Schnitzer Steel Industries Inc"}, "dividend": {"type": "CASH", "amountPerShare": 0.1875, "taxCode": "FULLY_TAXABLE"}}'
            ),
        ),
        (
            DriveWealthDividendTransaction,
            json.loads(
                '{"accountAmount": -0.45, "accountBalance": 254.6, "comment": "SCHN tax, 30% withheld", "finTranID": "KE.354965e6-591c-42fa-8237-738c03fad812", "wlpFinTranTypeID": "1e2a112b-cea0-4c01-b82c-98cfca21d729", "finTranTypeID": "DIVTAX", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "instrument": {"id": "522f3cb8-ea8e-4085-b11c-3fbcd994ca42", "symbol": "SCHN", "name": "Schnitzer Steel Industries Inc"}, "dividendTax": {"type": "NON_RESIDENT_ALIEN", "rate": 0.3}}'
            ),
        ),
        (
            DriveWealthSpinOffTransaction,
            json.loads(
                '{"accountAmount": 0, "accountBalance": 1720.67, "accountType": "LIVE", "comment": "Spinoff from AUY to PAAS", "dnb": true, "finTranID": "KD.351bd540-e522-45a8-bfea-eb211e41bfb0", "finTranTypeID": "SPINOFF", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "fillQty": 0, "fillPx": 0, "instrument": {"id": "0420793a-59cf-4b25-a189-1725c54413f1", "symbol": "AUY", "name": "Yamana Gold, Inc."}, "positionDelta": 21.68930412, "sendCommissionToInteliclear": false, "systemAmount": 0, "tranAmount": 0, "tranSource": "INTE", "tranWhen": "2023-04-10T04:24:24.611Z", "wlpAmount": 0, "wlpFinTranTypeID": "e205e560-653c-4f86-a3e5-63aad45044a2"}'
            ),
        ),
    ]


@pytest.mark.parametrize("test_data", get_transaction_test_data())
def test_create_typed_transaction(monkeypatch, test_data):
    cls, data = test_data
    transaction = DriveWealthTransaction()
    transaction.set_from_response(data)

    entity = DriveWealthTransaction.create_typed_transaction(transaction)

    assert isinstance(entity, cls)
