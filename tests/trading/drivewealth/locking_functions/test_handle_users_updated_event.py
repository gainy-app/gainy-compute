from gainy.tests.mocks.repository_mocks import mock_record_calls
from gainy.trading.drivewealth.locking_functions.handle_users_updated_event import HandleUsersUpdatedEvent
from gainy.trading.drivewealth.models import DriveWealthUser
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    user_id = "user_id"

    user = DriveWealthUser()
    user.profile_id = 1

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_user(_user_id):
        assert _user_id == user_id
        return user

    monkeypatch.setattr(provider, 'sync_user', mock_sync_user)
    ensure_account_created_calls = []
    monkeypatch.setattr(provider, 'ensure_account_created',
                        mock_record_calls(ensure_account_created_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, 'refresh', lambda x: x)

    message = {
        "userID": user_id,
    }
    func = HandleUsersUpdatedEvent(repository, provider, None, message)

    func._do(None)

    assert (user, ) in [args for args, kwargs in ensure_account_created_calls]
