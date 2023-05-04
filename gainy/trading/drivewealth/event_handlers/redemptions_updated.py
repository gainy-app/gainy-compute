from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthRedemption
from gainy.trading.models import TradingMoneyFlowStatus
from gainy.utils import get_logger

logger = get_logger(__name__)


class RedemptionUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'redemptions.updated'

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        redemption: DriveWealthRedemption = self.repo.find_one(
            DriveWealthRedemption, {"ref_id": ref_id})
        redemption_pre = redemption.to_dict()

        if redemption:
            was_approved = redemption.is_approved()
            old_mf_status = redemption.get_money_flow_status()
            old_status = redemption.status
            redemption = self.provider.sync_redemption(redemption.ref_id)
        else:
            redemption = DriveWealthRedemption()
            was_approved = redemption.is_approved()
            old_mf_status = redemption.get_money_flow_status()
            old_status = redemption.status
            redemption.set_from_response(event_payload)

        self.provider.handle_money_flow_status_change(redemption, old_status)
        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)
        logger.info("Updated redemption",
                    extra={
                        "file": __file__,
                        "redemption_pre": redemption_pre,
                        "redemption": redemption.to_dict(),
                    })

        if redemption.is_approved() != was_approved:
            # update cash weight in linked portfolio
            self.provider.on_new_transaction(redemption.trading_account_ref_id)

        if redemption.is_approved() and redemption.fees_total_amount is None:
            self.provider.sync_redemption(redemption.ref_id)

        self.provider.update_payment_transaction_from_dw(redemption)

        money_flow = self.provider.update_money_flow_from_dw(redemption)

        if money_flow:
            logger.info("Considering sending event on_withdraw_success",
                        extra={
                            "money_flow":
                            money_flow.to_dict() if money_flow else None,
                            "current_mf_status": money_flow.status,
                            "prev_mf_status": old_mf_status,
                        })
            if money_flow.status == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
                self.analytics_service.on_withdraw_success(
                    money_flow.profile_id, float(-money_flow.amount))
                self.notification_service.on_withdraw_success(
                    money_flow.profile_id, -money_flow.amount)
