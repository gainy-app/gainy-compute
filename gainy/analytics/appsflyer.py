import os

from appsflyer.rest import Client

from gainy.analytics.constants import EVENT_DW_BROKERAGE_ACCOUNT_OPENED, EVENT_DW_KYC_STATUS_REJECTED, \
    EVENT_DEPOSIT_SUCCESS, EVENT_WITHDRAW_SUCCESS, EVENT_PURCHASE_COMPLETED, EVENT_SELL_COMPLETED, \
    EVENT_COMMISSION_WITHDRAWN
from gainy.analytics.exceptions import InvalidAnalyticsMetadata, AnalyticsMetadataNotFound
from gainy.analytics.interfaces import AnalyticsSinkInterface
from gainy.analytics.repository import AnalyticsRepository, ANALYTICS_METADATA_SERVICE_APPSFLYER
from gainy.analytics.service import EVENT_PROPERTY_IS_FIRST_DEPOSIT, EVENT_PROPERTY_AMOUNT, EVENT_PROPERTY_ORDER_ID, \
    EVENT_PROPERTY_PRODUCT_TYPE, EVENT_PROPERTY_COLLECTION_ID, EVENT_PROPERTY_TICKER_SYMBOL, EVENT_PROPERTY_REVENUE
from gainy.utils import get_logger, env, ENV_PRODUCTION

APPSFLYER_APP_ID = os.getenv("APPSFLYER_APP_ID")
APPSFLYER_DEV_KEY = os.getenv("APPSFLYER_DEV_KEY")

EVENT_AF_FIRST_DEPOSIT_SUCCESS = "first_time_deposit"
EVENT_PROPERTY_AF_CURRENCY = "af_currency"
EVENT_PROPERTY_AF_ORDER_ID = "af_order_id"
EVENT_PROPERTY_AF_PRICE = "af_price"
EVENT_PROPERTY_AF_CONTENT_ID = "af_content_id"
EVENT_PROPERTY_AF_CONTENT_TYPE = "af_content_type"
EVENT_PROPERTY_AF_REVENUE = "af_revenue"
logger = get_logger(__name__)


def _get_user_id(profile_id: int) -> str:
    return '%05d' % profile_id


def add_currency(properties: dict, currency: str = "USD"):
    properties[EVENT_PROPERTY_AF_CURRENCY] = currency


class UnsupportedEventException(Exception):
    pass


def transform_event(event_name, properties):
    if event_name in [
            EVENT_DW_BROKERAGE_ACCOUNT_OPENED, EVENT_DW_KYC_STATUS_REJECTED
    ]:
        return event_name, properties

    if event_name == EVENT_DEPOSIT_SUCCESS:
        event_name = event_name
        if properties.get(EVENT_PROPERTY_IS_FIRST_DEPOSIT):
            event_name = EVENT_AF_FIRST_DEPOSIT_SUCCESS

        new_properties = {
            EVENT_PROPERTY_AMOUNT: properties[EVENT_PROPERTY_AMOUNT],
        }
        add_currency(new_properties)
        return event_name, new_properties

    if event_name == EVENT_WITHDRAW_SUCCESS:
        new_properties = {
            EVENT_PROPERTY_AMOUNT: properties[EVENT_PROPERTY_AMOUNT],
        }
        add_currency(new_properties)
        return event_name, new_properties

    if event_name in [EVENT_PURCHASE_COMPLETED, EVENT_SELL_COMPLETED]:
        new_properties = {
            EVENT_PROPERTY_AF_ORDER_ID:
            properties[EVENT_PROPERTY_ORDER_ID],
            EVENT_PROPERTY_AF_PRICE:
            abs(properties[EVENT_PROPERTY_AMOUNT]),
            EVENT_PROPERTY_AF_CONTENT_ID:
            properties.get(EVENT_PROPERTY_COLLECTION_ID)
            or properties.get(EVENT_PROPERTY_TICKER_SYMBOL),
            EVENT_PROPERTY_AF_CONTENT_TYPE:
            properties[EVENT_PROPERTY_PRODUCT_TYPE],
        }
        add_currency(new_properties)
        return event_name, new_properties

    if event_name == EVENT_COMMISSION_WITHDRAWN:
        new_properties = {
            EVENT_PROPERTY_AF_REVENUE: properties[EVENT_PROPERTY_REVENUE],
        }
        add_currency(new_properties)
        return event_name, new_properties

    raise UnsupportedEventException()


class AppsflyerService(AnalyticsSinkInterface):

    def __init__(self, analytics_repository: AnalyticsRepository):
        self.repository = analytics_repository

        self.client = None
        if APPSFLYER_APP_ID and APPSFLYER_DEV_KEY:
            self.client = Client(app_id=APPSFLYER_APP_ID,
                                 dev_key=APPSFLYER_DEV_KEY)

    def update_user_properties(self, profile_id: int, attributes: dict):
        pass

    def send_event(self, profile_id: int, event_name: str, properties: dict):
        if env() != ENV_PRODUCTION:
            return

        try:
            event_name, properties = transform_event(event_name, properties)
        except UnsupportedEventException:
            return

        try:
            appsflyer_id = self._get_profile_app_instance_id(profile_id)
        except (InvalidAnalyticsMetadata, AnalyticsMetadataNotFound) as e:
            logger.info(e, extra={"profile_id": profile_id})
            return

        logging_extra = {
            "profile_id": profile_id,
            "event_name": event_name,
            "properties": properties,
        }
        logger.info("Sending Appsflyer event", extra=logging_extra)

        self.client.generate_event(
            appsflyer_id=appsflyer_id,
            customer_user_id=_get_user_id(profile_id),
            event_name=event_name,
            event_value=properties,
        )

    def _get_profile_app_instance_id(self, profile_id):
        metadata = self.repository.get_analytics_metadata(
            profile_id, ANALYTICS_METADATA_SERVICE_APPSFLYER)

        if not metadata or not metadata["appsflyer_id"]:
            e = InvalidAnalyticsMetadata()
            logger.exception(e,
                             extra={
                                 "profile_id": profile_id,
                                 "metadata": metadata
                             })
            raise e

        return metadata["appsflyer_id"]
