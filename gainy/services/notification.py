import json
import os

from gainy.data_access.repository import Repository
from gainy.exceptions import NotFoundException, EmailNotSentException
from gainy.services.sendgrid import SendGridService
from gainy.utils import get_logger, env, ENV_LOCAL
from gainy.models import Profile

APP_STORE_LINK = "https://go.gainy.app/ZOFw"
DW_MANAGER_EMAILS = os.getenv('DW_MANAGER_EMAILS', '').split(',')
NOTIFICATION_EMAILS_LOCAL = json.loads(
    os.environ.get('NOTIFICATION_EMAILS_LOCAL', '[]'))
SENDGRID_KYC_FORM_ABANDONED_TEMPLATE_ID = "d-0e4037b807aa4116a6b83fedff82c423"
SENDGRID_KYC_STATUS_APPROVED_TEMPLATE_ID = "d-c3f82e108bbf4eeb8959e4f5cca6320c"
SENDGRID_KYC_STATUS_REJECTED_TEMPLATE_ID = "d-d60264301c574fc59f30c89aab23e566"
SENDGRID_KYC_STATUS_INFO_REQUIRED_TEMPLATE_ID = "d-04c8631a74b2482eb314b29efe41e6a5"
SENDGRID_KYC_STATUS_DOC_REQUIRED_TEMPLATE_ID = "d-412897a13d08451ab9632681c5e29680"
SENDGRID_FUNDING_ACCOUNT_LINKED_TEMPLATE_ID = "d-17d364f6f036407ab1fcba903493009c"
SENDGRID_DEPOSIT_INITIATED_TEMPLATE_ID = "d-6b3eccc179324d1aaff09cd87ba9969e"
SENDGRID_DEPOSIT_SUCCESS_TEMPLATE_ID = "d-9df474dd0e254f2888ba95d5a1cdf6de"
SENDGRID_DEPOSIT_FAILED_TEMPLATE_ID = "d-42d45ca2ba3e40ef92ee201886b4eac4"
SENDGRID_WITHDRAW_SUCCESS_TEMPLATE_ID = "d-3c71485bfeb643eebb87fa519b2e31fd"

logger = get_logger(__name__)


def _format_amount(amount) -> str:
    import locale
    locale.setlocale(locale.LC_ALL, '')
    return locale.currency(amount, grouping=True)


class NotificationService:

    def __init__(self, repository: Repository, sendgrid: SendGridService):
        self.repository = repository
        self.sendgrid = sendgrid

    def notify_dw_instrument_status_changed(self, symbol, status, new_status):
        subject = 'DriveWealth ticker %s changed status' % symbol
        text = 'DriveWealth ticker %s changed status from %s to %s' % (
            symbol, status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_dw_money_flow_status_changed(self, money_flow_type,
                                            money_flow_ref_id, old_status,
                                            new_status):
        subject = 'DriveWealth %s %s changed status' % (money_flow_type,
                                                        money_flow_ref_id)
        text = 'DriveWealth %s %s changed status from %s to %s' % (
            money_flow_type, money_flow_ref_id, old_status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_dw_account_status_changed(self, account_ref_id, old_status,
                                         new_status):
        subject = 'DriveWealth account %s changed status' % account_ref_id
        text = 'DriveWealth account %s changed status from %s to %s' % (
            account_ref_id, old_status, new_status)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def notify_low_balance(self, profile_id, balance):
        subject = 'Profile %d has low trading balance' % profile_id
        text = 'Profile %d has low trading balance: %f' % (profile_id, balance)
        try:
            self.sendgrid.send_email(to=DW_MANAGER_EMAILS,
                                     subject=subject,
                                     content_plain=text)
        except EmailNotSentException as e:
            logger.exception(e, extra={"subject": subject, "text": text})

    def on_kyc_status_approved(self, profile_id):
        logger.info('Sending notification on_kyc_status_approved',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_KYC_STATUS_APPROVED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_kyc_status_rejected(self, profile_id: int):
        logger.info('Sending notification on_kyc_status_rejected',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_KYC_STATUS_REJECTED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    #TODO modify template
    def on_kyc_status_info_required(self, profile_id: int, errors: str):
        logger.info('Sending notification on_kyc_status_info_required',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "errors": errors,
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_KYC_STATUS_INFO_REQUIRED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_kyc_status_doc_required(self, profile_id: int):
        logger.info('Sending notification on_kyc_status_doc_required',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_KYC_STATUS_DOC_REQUIRED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_funding_account_linked(self, profile_id: int, account_mask: str,
                                  account_name: str):
        logger.info('Sending notification on_funding_account_linked',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "account_mask": account_mask,
            "account_name": account_name,
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_FUNDING_ACCOUNT_LINKED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_deposit_initiated(self, profile_id: int, amount):
        logger.info('Sending notification on_deposit_initiated',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "amount_formatted": _format_amount(amount),
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_DEPOSIT_INITIATED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_deposit_success(self, profile_id: int, amount, account_mask: str):
        logger.info('Sending notification on_deposit_success',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "account_mask": account_mask,
            "amount_formatted": _format_amount(amount),
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_DEPOSIT_SUCCESS_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_deposit_failed(self, profile_id: int, amount, account_mask: str):
        logger.info('Sending notification on_deposit_failed',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "account_mask": account_mask,
            "amount_formatted": _format_amount(amount),
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_DEPOSIT_FAILED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_withdraw_success(self, profile_id: int, amount):
        logger.info('Sending notification on_withdraw_success',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "amount_formatted": _format_amount(amount),
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_WITHDRAW_SUCCESS_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def on_kyc_form_abandoned(self, profile_id: int):
        logger.info('Sending notification on_kyc_form_abandoned',
                    extra={"profile_id": profile_id})
        dynamic_template_data = {
            "link": APP_STORE_LINK, # TODO change
            "first_name": self._get_profile(profile_id).first_name,
        }
        template_id = SENDGRID_KYC_FORM_ABANDONED_TEMPLATE_ID
        self._notify_user(profile_id, template_id, dynamic_template_data)

    def _notify_user(self, profile_id, template_id, dynamic_template_data):
        #TODO make async through sqs
        try:
            self.sendgrid.send_email(
                to=self._get_profile_notification_emails(profile_id),
                dynamic_template_data=dynamic_template_data,
                template_id=template_id)
        except EmailNotSentException as e:
            logger.exception(e,
                             extra={
                                 "template_id": template_id,
                                 "dynamic_template_data": dynamic_template_data
                             })

    def _get_profile_notification_emails(self, profile_id) -> list[str]:
        if env() == ENV_LOCAL and NOTIFICATION_EMAILS_LOCAL:
            return NOTIFICATION_EMAILS_LOCAL

        return [self._get_profile(profile_id).email]

    def _get_profile(self, profile_id) -> Profile:
        profile: Profile = self.repository.find_one(Profile,
                                                    {"id": profile_id})
        if not profile:
            raise NotFoundException()

        return profile
