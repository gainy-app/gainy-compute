from gainy.analytics.firebase.sdk import FirebaseSDK
from gainy.analytics.interfaces import AnalyticsSinkInterface
from gainy.utils import get_logger

logger = get_logger(__name__)


class FirebaseService(AnalyticsSinkInterface):

    def __init__(self):
        self.client = FirebaseSDK()

    def update_profile_attribution(self, profile_id: int, attributes: dict):
        self.client.send_user_properties(profile_id, attributes)

    def send_event(self, profile_id: int, event_name: str, properties: dict):
        self.client.send_event(profile_id, event_name, properties)
