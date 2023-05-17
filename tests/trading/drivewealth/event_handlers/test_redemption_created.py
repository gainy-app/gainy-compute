from gainy.trading.drivewealth.models import DriveWealthRedemptionStatus, DriveWealthRedemption
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls, mock_find
from gainy.trading.drivewealth.event_handlers import RedemptionCreatedEventHandler
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository

message = {
    "paymentID": "GYEK000001-1666639501262-RY7T6",
    "statusMessage": DriveWealthRedemptionStatus.RIA_Pending,
    "accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557",
}


def test(monkeypatch):
    provider = DriveWealthProvider(None, None, None, None, None)
    handle_redemption_status_calls = []
    monkeypatch.setattr(provider, 'handle_redemption_status',
                        mock_record_calls(handle_redemption_status_calls))

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthRedemption, {
            "ref_id": message["paymentID"]
        }, None)]))

    event_handler = RedemptionCreatedEventHandler(repository, provider, None,
                                                  None)
    event_handler.handle(message)

    assert DriveWealthRedemption in persisted_objects
    redemption = persisted_objects[DriveWealthRedemption][0]
    assert redemption in [
        args[0] for args, kwards in handle_redemption_status_calls
    ]
