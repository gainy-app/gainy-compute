from gainy.analytics.constants import PROPERTY_PROFILE_ID, PROPERTY_USER_EMAIL, PROPERTY_MATCH_SCORE_CREATED, PROPERTY_DW_ACCOUNT_OPENED, PROPERTY_FUNDING_ACC_CONNECTED, PROPERTY_NUMBER_OF_FUNDING_ACC, PROPERTY_BANK_FUNDING_ACC, PROPERTY_PRODUCT_TYPE_PURCHASED, PROPERTY_TOTAL_INVESTED_TTFS, PROPERTY_TOTAL_INVESTED_TICKERS, PROPERTY_TOTAL_TRANSACTIONS, PROPERTY_TOTAL_DEPOSITS, PROPERTY_TOTAL_INVESTS, PROPERTY_TOTAL_SELLS, PROPERTY_TOTAL_AMOUNT_DEPOSIT, PROPERTY_TOTAL_AMOUNT_INVEST, PROPERTY_TOTAL_AMOUNT_SELL, PROPERTY_FIRST_PURCHASED_DATE, PROPERTY_FIRST_SELL_DATE, PROPERTY_LAST_PURCHASED_DATE, PROPERTY_LAST_SELL_DATE
from gainy.analytics.exceptions import InvalidAnalyticsMetadata, AnalyticsMetadataNotFound
from gainy.analytics.firebase.sdk import FirebaseClient
from gainy.analytics.interfaces import AnalyticsSinkInterface
from gainy.utils import get_logger

logger = get_logger(__name__)

ENABLED_USER_PROPERTIES = {
    PROPERTY_PROFILE_ID,
    PROPERTY_USER_EMAIL,
    PROPERTY_MATCH_SCORE_CREATED,
    PROPERTY_DW_ACCOUNT_OPENED,
    PROPERTY_FUNDING_ACC_CONNECTED,
    PROPERTY_NUMBER_OF_FUNDING_ACC,
    PROPERTY_BANK_FUNDING_ACC,
    PROPERTY_PRODUCT_TYPE_PURCHASED,
    PROPERTY_TOTAL_INVESTED_TTFS,
    PROPERTY_TOTAL_INVESTED_TICKERS,
    PROPERTY_TOTAL_TRANSACTIONS,
    PROPERTY_TOTAL_DEPOSITS,
    PROPERTY_TOTAL_INVESTS,
    PROPERTY_TOTAL_SELLS,
    PROPERTY_TOTAL_AMOUNT_DEPOSIT,
    PROPERTY_TOTAL_AMOUNT_INVEST,
    PROPERTY_TOTAL_AMOUNT_SELL,
    PROPERTY_FIRST_PURCHASED_DATE,
    PROPERTY_FIRST_SELL_DATE,
    PROPERTY_LAST_PURCHASED_DATE,
    PROPERTY_LAST_SELL_DATE,
}


class FirebaseService(AnalyticsSinkInterface):

    def __init__(self, firebase_client: FirebaseClient):
        self.client = firebase_client

    def update_user_properties(self, profile_id: int, properties: dict):
        properties = {
            k: i
            for k, i in properties.items() if k in ENABLED_USER_PROPERTIES
        }
        try:
            self.client.send_user_properties(profile_id, properties)
        except (InvalidAnalyticsMetadata, AnalyticsMetadataNotFound) as e:
            logger.info(e, extra={"profile_id": profile_id})
            pass

    def send_event(self,
                   profile_id: int,
                   event_name: str,
                   properties: dict = None):
        try:
            self.client.send_event(profile_id, event_name, properties)
        except (AnalyticsMetadataNotFound, InvalidAnalyticsMetadata) as e:
            logger.info(e, extra={"profile_id": profile_id})
            pass
