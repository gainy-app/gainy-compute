import datetime

import os

from gainy.analytics.amplitude.sdk import AmplitudeClient, AmplitudeSequentialDestinationPlugin
from amplitude import Identify, EventOptions, BaseEvent, Config
from amplitude.plugin import AmplitudeDestinationPlugin

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
        self.client = AmplitudeClient(AMPLITUDE_API_KEY, config)
        for plugin in self.client.plugins:
            if isinstance(plugin, AmplitudeDestinationPlugin):
                self.client.remove(plugin)
        self.client.add(AmplitudeSequentialDestinationPlugin())

    def update_user_properties(self, profile_id: int, properties: dict):
        identify_obj = Identify()
        for k, i in properties.items():
            if isinstance(i, datetime.datetime):
                i = i.strftime(DATETIME_ISO8601_FORMAT_TZ)
            identify_obj.set(k, i)

        self.client.identify(identify_obj,
                             EventOptions(user_id=_get_user_id(profile_id)))

    def send_event(self, profile_id: int, event_name: str, properties: dict):
        event = BaseEvent(event_type=event_name,
                          user_id=_get_user_id(profile_id),
                          event_properties=properties)

        logger.info('Emitting amplitude event %s',
                    event_name,
                    extra={
                        "profile_id": profile_id,
                        "event_name": event_name,
                        "properties": properties,
                        "event": event.get_event_body(),
                    })

        self.client.track(event)
        self.client.flush()
