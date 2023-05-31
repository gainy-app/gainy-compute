import json

import pytest

from gainy.trading.drivewealth.models import DriveWealthTransaction, \
    DriveWealthSpinOffTransaction, DriveWealthDividendTransaction, DriveWealthMergerAcquisitionTransaction


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
        (
            DriveWealthMergerAcquisitionTransaction,
            json.loads(
                '{"accountAmount": 36.71, "accountBalance": 253.54, "comment": "Exchanged 0.69263361 shares of MAXR for cash (merger/acquisition)", "finTranID": "KE.301f1b1d-f8da-48cb-aa5d-6c1c9075d9f3", "wlpFinTranTypeID": "89e1b2c9-8cf9-410f-afcc-f3fb78250d81", "finTranTypeID": "MERGER_ACQUISITION", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "positionDelta": -0.69263361, "instrument": {"id": "fd2dcc54-5b8b-4625-aebd-b3c938044bdb", "symbol": "MAXR", "name": "Maxar Technologies, Inc."}, "mergerAcquisition": {"type": "EXCHANGE_STOCK_CASH", "acquirer": {"id": "fd2dcc54-5b8b-4625-aebd-b3c938044bdb", "symbol": "MAXR", "name": "Maxar Technologies, Inc."}, "acquiree": {"id": "fd2dcc54-5b8b-4625-aebd-b3c938044bdb", "symbol": "MAXR", "name": "Maxar Technologies, Inc."}}}'
            ),
        ),
        (
            DriveWealthMergerAcquisitionTransaction,
            json.loads(
                '{"accountAmount": 0, "accountBalance": 9576.04, "comment": "Removed 36.8295206 shares of AUY (part of merger/acquisition of AUY by AEM)", "finTranID": "KD.d55ae8ac-5e21-4400-8fff-4be83e967421", "wlpFinTranTypeID": "89e1b2c9-8cf9-410f-afcc-f3fb78250d81", "finTranTypeID": "MERGER_ACQUISITION", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "positionDelta": -36.8295206, "instrument": {"id": "0420793a-59cf-4b25-a189-1725c54413f1", "symbol": "AUY", "name": "Yamana Gold, Inc."}, "mergerAcquisition": {"type": "REMOVE_SHARES", "acquirer": {"id": "2507874e-2027-4b1c-aeb2-bae68109253a", "symbol": "AEM", "name": "Agnico Eagle Mines Ltd"}, "acquiree": {"id": "0420793a-59cf-4b25-a189-1725c54413f1", "symbol": "AUY", "name": "Yamana Gold, Inc."}}}'
            ),
        ),
        (
            DriveWealthMergerAcquisitionTransaction,
            json.loads(
                '{"accountAmount": 38.32, "accountBalance": 9614.36, "comment": "Added 1.38478997 shares of AEM (part of merger/acquisition of AUY by AEM)", "finTranID": "KD.8931f8fc-7668-4af0-8c90-5f681eee3140", "wlpFinTranTypeID": "89e1b2c9-8cf9-410f-afcc-f3fb78250d81", "finTranTypeID": "MERGER_ACQUISITION", "feeSec": 0, "feeTaf": 0, "feeBase": 0, "feeXtraShares": 0, "feeExchange": 0, "positionDelta": 1.38478997, "instrument": {"id": "2507874e-2027-4b1c-aeb2-bae68109253a", "symbol": "AEM", "name": "Agnico Eagle Mines Ltd"}, "mergerAcquisition": {"type": "ADD_SHARES_CASH", "acquirer": {"id": "2507874e-2027-4b1c-aeb2-bae68109253a", "symbol": "AEM", "name": "Agnico Eagle Mines Ltd"}, "acquiree": {"id": "0420793a-59cf-4b25-a189-1725c54413f1", "symbol": "AUY", "name": "Yamana Gold, Inc."}}}'
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
