from gainy.analytics.exceptions import InvalidAnalyticsMetadata, AnalyticsMetadataNotFound
from gainy.analytics.firebase.sdk import FirebaseClient
from gainy.analytics.interfaces import AnalyticsSinkInterface
from gainy.utils import get_logger

logger = get_logger(__name__)


class FirebaseService(AnalyticsSinkInterface):

    def __init__(self, firebase_client: FirebaseClient):
        self.client = firebase_client

    def update_profile_attribution(self, profile_id: int, attributes: dict):
        try:
            self.client.send_user_properties(profile_id, attributes)
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
