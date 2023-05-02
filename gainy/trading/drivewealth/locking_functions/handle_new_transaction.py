import datetime

from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth.exceptions import InvalidDriveWealthPortfolioStatusException
from gainy.trading.drivewealth.models import DriveWealthPortfolio
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
        portfolio: DriveWealthPortfolio = self.repo.find_one(
            DriveWealthPortfolio,
            {"drivewealth_account_id": self.account_ref_id})
        if not portfolio:
            return

        self.provider.sync_portfolio(portfolio)
        try:
            portfolio_status = self.provider.sync_portfolio_status(portfolio,
                                                                   force=True)
        except InvalidDriveWealthPortfolioStatusException as e:
            portfolio_status = self.provider.get_latest_portfolio_status(
                portfolio.ref_id)

            # in case we received an invalid portfolio status - look for a valid one, which is not more than an hour old
            min_created_at = datetime.datetime.now(
                datetime.timezone.utc) - datetime.timedelta(hours=1)
            if not portfolio_status or portfolio_status.created_at < min_created_at:
                logger.exception(e)
                # use invalid portfolio status anyway, what choice do we have?..
                portfolio_status = e.portfolio_status

        portfolio_changed = self.provider.actualize_portfolio(
            portfolio, portfolio_status)
        if not portfolio_changed:
            return

        portfolio.normalize_weights()
        self.provider.send_portfolio_to_api(portfolio)
