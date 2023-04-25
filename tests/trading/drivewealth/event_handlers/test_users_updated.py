from gainy.tests.mocks.repository_mocks import mock_record_calls
from gainy.trading.drivewealth.models import DriveWealthUser
from gainy.trading.drivewealth.event_handlers import UsersUpdatedEventHandler
from gainy.trading.drivewealth import DriveWealthProvider
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

    event_handler = UsersUpdatedEventHandler(repository, provider, None, None)

    message = {
        "userID": user_id,
    }
    event_handler.handle(message)

    assert (user, ) in [args for args, kwargs in ensure_account_created_calls]
