from gainy.models import AbstractEntityLock
from gainy.tests.mocks.repository_mocks import mock_record_calls, mock_persist
from gainy.trading.drivewealth.event_handlers import UsersUpdatedEventHandler
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    user_id = "user_id"
    event_payload = {"userID": user_id}

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))
    provider = DriveWealthProvider(None, None, None, None, None)
    event_handler = UsersUpdatedEventHandler(repository, provider, None, None)

    execute_calls = []

    def mock_execute(self):
        assert self.provider == provider
        assert self.entity_lock in persisted_objects[AbstractEntityLock]
        assert isinstance(self.entity_lock, AbstractEntityLock)
        assert self.entity_lock.object_id == user_id
        assert self.event_payload == event_payload
        mock_record_calls(execute_calls)()

    monkeypatch.setattr(
        "gainy.trading.drivewealth.locking_functions.handle_users_updated_event.HandleUsersUpdatedEvent.execute",
        mock_execute)

    event_handler.handle(event_payload)

    assert execute_calls
