from gainy.analytics.service import AnalyticsService
from gainy.trading.drivewealth.models import DriveWealthRedemptionStatus, DriveWealthRedemption
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from gainy.services.notification import NotificationService
from gainy.trading.models import TradingMoneyFlow, TradingMoneyFlowStatus
from gainy.trading.drivewealth.event_handlers import RedemptionUpdatedEventHandler
from gainy.trading.drivewealth import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository

message = {
    "paymentID": "GYEK000001-1666639501262-RY7T6",
    "statusMessage": "Successful",
    "accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557",
}


def test_exists(monkeypatch):
    old_status = DriveWealthRedemptionStatus.RIA_Approved
    new_status = message["statusMessage"]
    profile_id = 1
    amount = 2

    money_flow = TradingMoneyFlow()
    money_flow.profile_id = profile_id
    money_flow.amount = -amount
    money_flow.status = TradingMoneyFlowStatus.SUCCESS

    provider = DriveWealthProvider(None, None, None, None, None)
    handle_redemption_status_calls = []
    monkeypatch.setattr(provider, 'handle_redemption_status',
                        mock_record_calls(handle_redemption_status_calls))
    on_new_transaction_calls = []
    monkeypatch.setattr(provider, 'on_new_transaction',
                        mock_record_calls(on_new_transaction_calls))

    def mock_update_money_flow_from_dw(_redemption):
        assert redemption == _redemption
        redemption.status = new_status
        return money_flow

    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_update_money_flow_from_dw)
    sync_redemption_calls = []

    def mock_sync_redemption(ref_id):
        mock_record_calls(sync_redemption_calls)(ref_id)
        redemption.set_from_response(message)
        return redemption

    monkeypatch.setattr(provider, 'sync_redemption', mock_sync_redemption)
    handle_money_flow_status_change_calls = []
    monkeypatch.setattr(
        provider, 'handle_money_flow_status_change',
        mock_record_calls(handle_money_flow_status_change_calls))

    redemption = DriveWealthRedemption()
    redemption.status = old_status

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthRedemption, {
            "ref_id": message["paymentID"]
        }, redemption)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    analytics_service = AnalyticsService(None, None, None)
    analytics_service_on_withdraw_success_calls = []
    monkeypatch.setattr(
        analytics_service, 'on_withdraw_success',
        mock_record_calls(analytics_service_on_withdraw_success_calls))

    notification_service = NotificationService(None, None)
    notification_service_on_withdraw_success_calls = []
    monkeypatch.setattr(
        notification_service, 'on_withdraw_success',
        mock_record_calls(notification_service_on_withdraw_success_calls))

    event_handler = RedemptionUpdatedEventHandler(repository, provider, None,
                                                  analytics_service,
                                                  notification_service)
    event_handler.handle(message)

    assert DriveWealthRedemption in persisted_objects
    assert redemption in persisted_objects[DriveWealthRedemption]
    assert redemption in [
        args[0] for args, kwards in handle_redemption_status_calls
    ]
    assert redemption.ref_id == message["paymentID"]
    assert redemption.trading_account_ref_id == message["accountID"]
    assert redemption.status == new_status
    assert (redemption, old_status) in [
        args for args, kwargs in handle_money_flow_status_change_calls
    ]
    assert ((redemption.trading_account_ref_id, ),
            {}) in on_new_transaction_calls
    assert ((profile_id, amount),
            {}) in analytics_service_on_withdraw_success_calls
    assert ((profile_id, amount),
            {}) in notification_service_on_withdraw_success_calls


def test_not_exists(monkeypatch):
    provider = DriveWealthProvider(None, None, None, None, None)
    handle_redemption_status_calls = []
    monkeypatch.setattr(provider, 'handle_redemption_status',
                        mock_record_calls(handle_redemption_status_calls))
    update_money_flow_from_dw_calls = []
    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_record_calls(update_money_flow_from_dw_calls))
    sync_redemption_calls = []
    monkeypatch.setattr(provider, 'sync_redemption',
                        mock_record_calls(sync_redemption_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthRedemption, {
            "ref_id": message["paymentID"]
        }, None)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = RedemptionUpdatedEventHandler(repository, provider, None,
                                                  None)
    event_handler.handle(message)

    assert DriveWealthRedemption in persisted_objects
    redemption = persisted_objects[DriveWealthRedemption][0]
    assert redemption in [
        args[0] for args, kwards in handle_redemption_status_calls
    ]
    assert redemption in [
        args[0] for args, kwargs in update_money_flow_from_dw_calls
    ]
    assert redemption.ref_id == message["paymentID"]
    assert redemption.trading_account_ref_id == message["accountID"]
    assert redemption.status == message["statusMessage"]
