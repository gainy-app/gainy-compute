from gainy.analytics.service import AnalyticsService
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from gainy.trading.drivewealth import DriveWealthRepository
from gainy.services.notification import NotificationService
from gainy.trading.drivewealth.event_handlers.kyc_updated import KycUpdatedEventHandler

from gainy.trading.models import ProfileKycStatus, KycStatus
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.repository import TradingRepository


def test(monkeypatch):
    user_id = "user_id"
    profile_id = 1

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    analytics_service = AnalyticsService(None, None, None)
    analytics_service_on_kyc_status_rejected_calls = []
    monkeypatch.setattr(
        analytics_service, 'on_kyc_status_rejected',
        mock_record_calls(analytics_service_on_kyc_status_rejected_calls))

    notification_service = NotificationService(None, None)
    notification_service_on_kyc_status_rejected_calls = []
    monkeypatch.setattr(
        notification_service, 'on_kyc_status_rejected',
        mock_record_calls(notification_service_on_kyc_status_rejected_calls))

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_get_profile_id_by_user_id(_user_id):
        assert _user_id == user_id
        return profile_id

    monkeypatch.setattr(provider, 'get_profile_id_by_user_id',
                        mock_get_profile_id_by_user_id)

    trading_repository = TradingRepository(None)
    update_kyc_form_calls = []
    monkeypatch.setattr(trading_repository, 'update_kyc_form',
                        mock_record_calls(update_kyc_form_calls))

    def mock_get_actual_kyc_status(_profile_id):
        assert _profile_id == profile_id
        return ProfileKycStatus()

    monkeypatch.setattr(trading_repository, 'get_actual_kyc_status',
                        mock_get_actual_kyc_status)

    event_handler = KycUpdatedEventHandler(repository, provider,
                                           trading_repository,
                                           analytics_service,
                                           notification_service)

    message = {
        "userID": user_id,
        "current": {
            "status": "KYC_DENIED",
            "statusMessage":
            "User\u2019s PII does not match. Please check kyc errors for details and resubmit the information",
            "details": ["SSN_NOT_MATCH"]
        },
        "previous": {
            "status": "KYC_PROCESSING",
            "statusMessage": "User is sent for KYC",
            "details": ["SSN_NOT_MATCH"]
        }
    }
    event_handler.handle(message)

    assert ProfileKycStatus in persisted_objects
    entity: ProfileKycStatus = persisted_objects[ProfileKycStatus][0]
    assert entity.profile_id == profile_id
    assert entity.status == KycStatus.DENIED
    assert entity.message == message["current"]["statusMessage"]
    assert entity.error_messages == [
        "No match found for Social Security Number"
    ]
    assert (profile_id, KycStatus.DENIED) in [
        args for args, kwargs in update_kyc_form_calls
    ]
    assert ((profile_id, ),
            {}) in analytics_service_on_kyc_status_rejected_calls
    assert ((profile_id, ),
            {}) in notification_service_on_kyc_status_rejected_calls
