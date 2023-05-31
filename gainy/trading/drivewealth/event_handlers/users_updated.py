from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.locking_functions.handle_users_updated_event import HandleUsersUpdatedEvent
from gainy.trading.drivewealth.models import DriveWealthUser
from gainy.utils import get_logger

logger = get_logger(__name__)


class UsersUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in ["users.updated", "users.created"]

    def handle(self, event_payload: dict):
        user_id = event_payload["userID"]
        entity_lock = AbstractEntityLock(DriveWealthUser, user_id)
        self.repo.persist(entity_lock)

        func = HandleUsersUpdatedEvent(self.repo, self.provider, entity_lock,
                                       event_payload)
        func.execute()
