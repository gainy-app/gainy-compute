import os

from appsflyer.rest import Client

from gainy.analytics.interfaces import AnalyticsSinkInterface

APPSFLYER_APP_ID = os.getenv("APPSFLYER_APP_ID")
APPSFLYER_DEV_KEY = os.getenv("APPSFLYER_DEV_KEY")


def _get_user_id(profile_id: int) -> str:
    return '%05d' % profile_id


class AppsflyerService(AnalyticsSinkInterface):

    def __init__(self):
        self.client = Client(app_id=APPSFLYER_APP_ID,
                             dev_key=APPSFLYER_DEV_KEY)

    def update_profile_attribution(self, profile_id: int, attributes: dict):
        pass

    def send_event(self, profile_id: int, event_name: str, properties: dict):
        self.client.generate_event(
            # TODO determine user id param
            # appsflyer_id=_get_user_id(profile_id),
            # customer_user_id="example_customer_id_123",
            event_name=event_name,
            event_value=properties,
        )
