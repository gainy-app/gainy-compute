import datetime

from gainy.billing.models import PaymentMethod, PaymentMethodProvider
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.exceptions import TradingAccountNotOpenException
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class AccountsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in ["accounts.updated", "accounts.created"]

    def handle(self, event_payload: dict):
        ref_id = event_payload["accountID"]

        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": ref_id})
        if account:
            was_open = account.is_open()
            data = event_payload.get('current', {})
            if "status" in data:
                old_status = account.status
                account.status = data["status"]['name']
                self.provider.handle_account_status_change(account, old_status)
                self.repo.persist(account)
        else:
            was_open = False
            account = self.provider.sync_trading_account(account_ref_id=ref_id,
                                                         fetch_info=True)

        if account and account.is_open() and account.drivewealth_user_id:
            self.ensure_portfolio(account)

            user: DriveWealthUser = self.repo.find_one(
                DriveWealthUser, {"ref_id": account.drivewealth_user_id})
            if not user or not user.profile_id:
                return

            self.send_event(user.profile_id, was_open)
            self.create_payment_method(account, user.profile_id)

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

    def ensure_portfolio(self, account: DriveWealthAccount):
        if not account.trading_account_id:
            return

        trading_account: TradingAccount = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return

        try:
            self.provider.ensure_portfolio(trading_account.profile_id,
                                           trading_account.id)
        except TradingAccountNotOpenException:
            pass
