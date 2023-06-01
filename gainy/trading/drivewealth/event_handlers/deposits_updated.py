from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthDeposit
from gainy.trading.exceptions import TradingPausedException
from gainy.trading.models import TradingMoneyFlowStatus, FundingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class DepositsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "deposits.updated"

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        deposit = self.repo.find_one(DriveWealthDeposit, {"ref_id": ref_id})

        if not deposit:
            deposit = DriveWealthDeposit()

        deposit_pre = deposit.to_dict()

        old_mf_status = deposit.get_money_flow_status()
        old_status = deposit.status
        deposit.set_from_response(event_payload)

        self.provider.handle_money_flow_status_change(deposit, old_status)

        self.repo.persist(deposit)
        money_flow = self.provider.update_money_flow_from_dw(deposit)

        logger.info("Updated deposit",
                    extra={
                        "file": __file__,
                        "deposit_pre": deposit_pre,
                        "deposit": deposit.to_dict(),
                    })

        if not money_flow:
            return

        profile_id = money_flow.profile_id
        funding_account: FundingAccount = self.repo.find_one(
            FundingAccount, {"id": money_flow.funding_account_id})

        logger.info("Considering sending event on_deposit_success",
                    extra={
                        "money_flow":
                        money_flow.to_dict() if money_flow else None,
                        "current_mf_status": deposit.get_money_flow_status(),
                        "prev_mf_status": old_mf_status,
                    })
        if deposit.get_money_flow_status(
        ) == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
            self.analytics_service.on_deposit_success(money_flow)
            if funding_account:
                self.notification_service.on_deposit_success(
                    profile_id, money_flow.amount, funding_account.mask)

        if deposit.get_money_flow_status(
        ) == TradingMoneyFlowStatus.FAILED and old_mf_status != TradingMoneyFlowStatus.FAILED and funding_account:
            self.notification_service.on_deposit_failed(
                profile_id, money_flow.amount, funding_account.mask)

            try:
                self.trading_repository.check_profile_trading_not_paused(
                    profile_id)
                self.trading_repository.set_profile_trading_paused(profile_id)
                self.notification_service.on_profile_paused(
                    profile_id, "returned deposit")
            except TradingPausedException:
                pass
