import os

from amplitude import Amplitude, Identify, EventOptions

from analytics.interfaces import AnalyticsSinkInterface

AMPLITUDE_API_KEY = os.getenv("AMPLITUDE_API_KEY")


def _get_user_id(profile_id: int) -> str:
    return '%05d' % profile_id


class AmplitudeService(AnalyticsSinkInterface):
    def __init__(self):
        self.client = Amplitude(AMPLITUDE_API_KEY)

    def update_profile_attribution(self, profile_id: int, attributes: dict):
        identify_obj = Identify()
        for k, i in attributes.items():
            identify_obj.set(k, i)

        self.client.identify(identify_obj, EventOptions(user_id=_get_user_id(profile_id)))
