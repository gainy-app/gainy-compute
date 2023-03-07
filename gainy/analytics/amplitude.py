import datetime

import os

from amplitude import Amplitude, Identify, EventOptions, BaseEvent, Config

from gainy.analytics.interfaces import AnalyticsSinkInterface
from gainy.utils import DATETIME_ISO8601_FORMAT_TZ, get_logger

AMPLITUDE_API_KEY = os.getenv("AMPLITUDE_API_KEY")
logger = get_logger(__name__)

def event_cb(event: BaseEvent, code: int, message: str):
    extra = {
        "event": event,
        "response": {
            "code": code,
            "message": message,
        }
    }
    if code == 200:
        logger.info('Amplitude event sent', extra=extra)
    else:
        logger.error('Amplitude event sent', extra=extra)


def _get_user_id(profile_id: int) -> str:
    return '%05d' % profile_id


class AmplitudeService(AnalyticsSinkInterface):

    def __init__(self):
        config = Config(logger=logger, server_zone="US", callback=event_cb)
        self.client = Amplitude(AMPLITUDE_API_KEY, config)

    def update_profile_attribution(self, profile_id: int, attributes: dict):
        identify_obj = Identify()
        for k, i in attributes.items():
            if isinstance(i, datetime.datetime):
                i = i.strftime(DATETIME_ISO8601_FORMAT_TZ)
            identify_obj.set(k, i)

        self.client.identify(identify_obj,
                             EventOptions(user_id=_get_user_id(profile_id)))

    def send_event(self, profile_id: int, event_name: str, properties: dict):
        event = BaseEvent(event_type=event_name,
                          user_id=_get_user_id(profile_id),
                          event_properties=properties)
        self.client.track(event)

        result = self.client.flush()
        print(result)
        logger.info('Flush Amplitude events', {"result": result})
