from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.drivewealth.provider.interface import DriveWealthProviderInterface
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class EnsurePortfolio(AbstractPessimisticLockingFunction):
    repo: DriveWealthRepository

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProviderInterface,
                 entity_lock: AbstractEntityLock, profile_id: int,
                 account: DriveWealthAccount):
        super().__init__(repo)
        self.provider = provider
        self.entity_lock = entity_lock
        self.profile_id = profile_id
        self.account = account

    def execute(self, max_tries: int = 3):
        return super().execute(max_tries)

    def load_version(self) -> AbstractEntityLock:
        return self.repo.refresh(self.entity_lock)

    def _do(self, entity_lock: AbstractEntityLock):
        return self.provider.ensure_portfolio(self.profile_id, self.account)
