from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.locking_functions.handle_accounts_updated_event import HandleAccountsUpdatedEvent
from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class AccountsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in ["accounts.updated", "accounts.created"]

    def handle(self, event_payload: dict):
        ref_id = event_payload["accountID"]
        entity_lock = AbstractEntityLock(DriveWealthAccount, ref_id)
        self.repo.persist(entity_lock)

        func = HandleAccountsUpdatedEvent(self.repo, self.provider,
                                          self.analytics_service, entity_lock,
                                          event_payload)
        func.execute()
