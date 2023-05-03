import datetime
from typing import Optional

from gainy.analytics.service import AnalyticsService
from gainy.billing.models import PaymentMethod, PaymentMethodProvider
from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.models import AbstractEntityLock
from gainy.trading.drivewealth import DriveWealthProvider
from gainy.trading.drivewealth.exceptions import TradingAccountNotOpenException
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser, DriveWealthAccountStatus, \
    DriveWealthPortfolio
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class HandleAccountsUpdatedEvent(AbstractPessimisticLockingFunction):
    repo: DriveWealthRepository

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 analytics_service: AnalyticsService,
                 entity_lock: AbstractEntityLock, event_payload):
        super().__init__(repo)
        self.provider = provider
        self.analytics_service = analytics_service
        self.entity_lock = entity_lock
        self.event_payload = event_payload

    def execute(self, max_tries: int = 3):
        return super().execute(max_tries)

    def load_version(self) -> AbstractEntityLock:
        return self.repo.refresh(self.entity_lock)

    def _do(self, entity_lock: AbstractEntityLock):
        ref_id = self.event_payload["accountID"]
        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": ref_id})
        if account:
            was_open = account.is_open()
            old_status = account.status
            data = self.event_payload.get('current', {})
            if "status" in data:
                account.status = data["status"]['name']
                self.provider.handle_account_status_change(account, old_status)
                self.repo.persist(account)
        else:
            was_open = False
            old_status = None
            account = self.provider.sync_trading_account(account_ref_id=ref_id,
                                                         fetch_info=True)

        if account and account.is_open() and account.drivewealth_user_id:
            portfolio = self.ensure_portfolio(account)

            user: DriveWealthUser = self.repo.find_one(
                DriveWealthUser, {"ref_id": account.drivewealth_user_id})
            if not user or not user.profile_id:
                return

            self.send_event(user.profile_id, was_open)
            self.create_payment_method(account, user.profile_id)

            if portfolio and old_status == DriveWealthAccountStatus.OPEN_NO_NEW_TRADES.name:
                # if account reopens, we set portfolio target weights to actual weights
                self.provider.sync_portfolio(portfolio)
                portfolio_status = self.provider.sync_portfolio_status(
                    portfolio, force=True, allow_invalid=True)
                self.provider.actualize_portfolio(portfolio, portfolio_status)
                self.provider.send_portfolio_to_api(portfolio)

    def send_event(self, profile_id: int, was_open: bool):
        logger.info("Considering sending event on_dw_brokerage_account_opened",
                    extra={
                        "was_open": was_open,
                        "profile_id": profile_id,
                    })
        if was_open:
            return

        self.analytics_service.on_dw_brokerage_account_opened(profile_id)

    def create_payment_method(self, account: DriveWealthAccount,
                              profile_id: int):
        if account.payment_method_id:
            return

        payment_method = PaymentMethod()
        payment_method.profile_id = profile_id
        payment_method.provider = PaymentMethodProvider.DRIVEWEALTH
        payment_method.name = f"Trading Account {account.ref_no}"
        payment_method.set_active_at = datetime.datetime.now()
        self.repo.persist(payment_method)

        account.payment_method_id = payment_method.id
        self.repo.persist(account)

    def ensure_portfolio(
            self,
            account: DriveWealthAccount) -> Optional[DriveWealthPortfolio]:
        if not account.trading_account_id:
            return None

        trading_account: TradingAccount = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return None

        try:
            return self.provider.ensure_portfolio(trading_account.profile_id,
                                                  trading_account.id)
        except TradingAccountNotOpenException:
            pass

        return None
