from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthAccount
from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class HandleNewTransaction(AbstractPessimisticLockingFunction):
    repo: DriveWealthRepository

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProviderBase,
                 entity_lock: AbstractEntityLock, account_ref_id):
        super().__init__(repo)
        self.provider = provider
        self.entity_lock = entity_lock
        self.account_ref_id = account_ref_id

    def execute(self, max_tries: int = 3):
        return super().execute(max_tries)

    def load_version(self) -> AbstractEntityLock:
        return self.repo.refresh(self.entity_lock)

    def _do(self, entity_lock: AbstractEntityLock):
        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": self.account_ref_id})
        if not account or not account.is_open():
            return

        portfolio: DriveWealthPortfolio = self.repo.find_one(
            DriveWealthPortfolio,
            {"drivewealth_account_id": self.account_ref_id})
        if not portfolio:
            return

        self.provider.sync_portfolio(portfolio)
        portfolio_status = self.provider.sync_portfolio_status(
            portfolio, force=True, allow_invalid=True)

        portfolio_changed = self.provider.actualize_portfolio(
            portfolio, portfolio_status)
        if not portfolio_changed:
            return

        self.provider.send_portfolio_to_api(portfolio)
