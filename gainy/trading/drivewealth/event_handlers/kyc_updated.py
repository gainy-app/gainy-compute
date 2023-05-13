from gainy.exceptions import NotFoundException
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthKycStatus
from gainy.trading.models import ProfileKycStatus, KycStatus
from gainy.utils import get_logger

logger = get_logger(__name__)


def _get_profile_kyc_status(data) -> ProfileKycStatus:
    message = data['statusMessage']
    error_codes = data.get('details', [])
    status = DriveWealthKycStatus.map_dw_kyc_status(data['status'],
                                                    error_codes)

    entity = ProfileKycStatus()
    entity.status = status
    entity.message = message
    entity.error_codes = DriveWealthKycStatus.map_dw_error_codes(error_codes)
    entity.reset_error_messages()

    return entity


def _status_changed_to(entity: ProfileKycStatus, old_entity: ProfileKycStatus,
                       status: KycStatus):
    return entity.status == status and (not old_entity
                                        or old_entity.status != status)


class KycUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "kyc.updated"

    def handle(self, event_payload: dict):
        user_id = event_payload["userID"]
        try:
            profile_id = self.provider.get_profile_id_by_user_id(user_id)
        except NotFoundException:
            return

        try:
            old_entity = self.trading_repository.get_actual_kyc_status(
                profile_id)
        except NotFoundException:
            old_entity = None

        entity = _get_profile_kyc_status(event_payload['current'])
        entity.profile_id = profile_id
        self.repo.persist(entity)
        self.trading_repository.update_kyc_form(profile_id, entity.status)

        logger.info("KYC updated",
                    extra={
                        "profile_id": profile_id,
                        "current_status": entity.status,
                        "prev_status":
                        old_entity.status if old_entity else None,
                    })

        if _status_changed_to(entity, old_entity, KycStatus.APPROVED):
            self.notification_service.on_kyc_status_approved(profile_id)

        if _status_changed_to(entity, old_entity, KycStatus.DENIED):
            self.analytics_service.on_kyc_status_rejected(profile_id)
            self.notification_service.on_kyc_status_rejected(profile_id)

        if _status_changed_to(entity, old_entity, KycStatus.MANUAL_REVIEW):
            self.analytics_service.on_kyc_status_manual_review(profile_id)

        if _status_changed_to(entity, old_entity, KycStatus.INFO_REQUIRED):
            errors = ', '.join(i.lower().rstrip('.')
                               for i in entity.error_messages)
            self.analytics_service.on_kyc_status_info_required(
                profile_id, entity.error_messages)
            self.notification_service.on_kyc_status_info_required(
                profile_id, errors)

        if _status_changed_to(entity, old_entity, KycStatus.DOC_REQUIRED):
            self.analytics_service.on_kyc_status_doc_required(
                profile_id, entity.error_messages)
            self.notification_service.on_kyc_status_doc_required(profile_id)
