from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class HandleUsersUpdatedEvent(AbstractPessimisticLockingFunction):
    repo: DriveWealthRepository

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 entity_lock: AbstractEntityLock, event_payload):
        super().__init__(repo)
        self.provider = provider
        self.entity_lock = entity_lock
        self.event_payload = event_payload

    def execute(self, max_tries: int = 3):
        return super().execute(max_tries)

    def load_version(self) -> AbstractEntityLock:
        return self.repo.refresh(self.entity_lock)

    def _do(self, entity_lock: AbstractEntityLock):
        user_id = self.event_payload["userID"]
        user = self.provider.sync_user(user_id)
        user = self.repo.refresh(user)

        if user.profile_id:
            self.provider.ensure_account_created(user)
