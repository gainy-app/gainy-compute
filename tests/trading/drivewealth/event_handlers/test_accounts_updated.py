from gainy.analytics.service import AnalyticsService
from gainy.models import AbstractEntityLock
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from gainy.trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    account_id = "account_id"
    event_payload = {"accountID": account_id}

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))
    provider = DriveWealthProvider(None, None, None, None, None)
    analytics_service = AnalyticsService(None, None, None)
    event_handler = AccountsUpdatedEventHandler(repository, provider, None,
                                                analytics_service)

    execute_calls = []

    def mock_execute(self):
        assert self.provider == provider
        assert self.analytics_service == analytics_service
        assert self.entity_lock in persisted_objects[AbstractEntityLock]
        assert isinstance(self.entity_lock, AbstractEntityLock)
        assert self.entity_lock.object_id == account_id
        assert self.event_payload == event_payload
        mock_record_calls(execute_calls)()

    monkeypatch.setattr(
        "gainy.trading.drivewealth.locking_functions.handle_accounts_updated_event.HandleAccountsUpdatedEvent.execute",
        mock_execute)

    event_handler.handle(event_payload)

    assert execute_calls
