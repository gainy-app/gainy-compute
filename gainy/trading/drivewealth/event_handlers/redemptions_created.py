from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthRedemption
from gainy.utils import get_logger

logger = get_logger(__name__)


class RedemptionCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'redemptions.created'

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        redemption: DriveWealthRedemption = self.repo.find_one(
            DriveWealthRedemption, {"ref_id": ref_id})

        if redemption:
            redemption = self.provider.sync_redemption(redemption.ref_id)
        else:
            redemption = DriveWealthRedemption()
            redemption.set_from_response(event_payload)
            self.repo.persist(redemption)
            logger.info("Updated redemption",
                        extra={
                            "file": __file__,
                            "redemption": redemption.to_dict(),
                        })

        self.provider.handle_redemption_status(redemption)
