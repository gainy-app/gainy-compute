from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthDeposit
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

        old_mf_status = deposit.get_money_flow_status()
        old_status = deposit.status
        deposit.set_from_response(event_payload)

        self.provider.handle_money_flow_status_change(deposit, old_status)

        self.repo.persist(deposit)
        money_flow = self.provider.update_money_flow_from_dw(deposit)

        if money_flow:
            funding_account: FundingAccount = self.repo.find_one(
                FundingAccount, {"id": money_flow.funding_account_id})
        else:
            funding_account = None

        logger.info("Considering sending event on_deposit_success",
                    extra={
                        "money_flow":
                        money_flow.to_dict() if money_flow else None,
                        "current_mf_status": deposit.get_money_flow_status(),
                        "prev_mf_status": old_mf_status,
                    })
        if money_flow and deposit.get_money_flow_status(
        ) == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
            self.analytics_service.on_deposit_success(money_flow)
            if funding_account:
                self.notification_service.on_deposit_success(
                    money_flow.profile_id, money_flow.amount,
                    funding_account.mask)

        if money_flow and deposit.get_money_flow_status(
        ) == TradingMoneyFlowStatus.FAILED and old_mf_status != TradingMoneyFlowStatus.FAILED and funding_account:
            self.notification_service.on_deposit_failed(
                money_flow.profile_id, money_flow.amount, funding_account.mask)
