from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.utils import get_logger

logger = get_logger(__name__)


class UsersUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in ["users.updated", "users.created"]

    def handle(self, event_payload: dict):
        # TODO: make thread safe for a user
        user_id = event_payload["userID"]
        user = self.provider.sync_user(user_id)
        user = self.repo.refresh(user)

        # create or update account
        if user.profile_id:
            self.provider.ensure_account_created(user)
