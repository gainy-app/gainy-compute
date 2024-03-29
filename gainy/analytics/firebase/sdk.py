import json
import os

import backoff
import requests
from backoff import full_jitter

from gainy.analytics import format_properties
from gainy.analytics.exceptions import InvalidAnalyticsMetadata
from gainy.analytics.repository import AnalyticsRepository, ANALYTICS_METADATA_SERVICE_FIREBASE
from gainy.data_access.models import DecimalEncoder
from gainy.exceptions import HttpException
from gainy.utils import get_logger

logger = get_logger(__name__)

GA_API_URL = "https://www.google-analytics.com"
FIREBASE_APP_ID = os.getenv("FIREBASE_APP_ID")
FIREBASE_API_SECRET = os.getenv("FIREBASE_API_SECRET")


class FirebaseClient:

    # https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events?client_type=firebase
    def __init__(self, analytics_repository: AnalyticsRepository):
        self.repository = analytics_repository

    def send_event(self, profile_id, name, params: dict):
        app_instance_id = self._get_profile_app_instance_id(profile_id)
        events = [{
            "name": name,
            "params": format_properties(params),
        }]
        return self._make_request("POST",
                                  f"/mp/collect",
                                  post_data={
                                      "app_instance_id": app_instance_id,
                                      "user_id": str(profile_id),
                                      "events": events
                                  })

    # https://developers.google.com/analytics/devguides/collection/protocol/ga4/user-properties?client_type=firebase
    def send_user_properties(self, profile_id, properties: dict):
        app_instance_id = self._get_profile_app_instance_id(profile_id)
        properties = {
            k: {
                "value": format_properties(i)
            }
            for k, i in properties.items()
        }
        return self._make_request("POST",
                                  f"/mp/collect",
                                  post_data={
                                      "app_instance_id": app_instance_id,
                                      "user_id": str(profile_id),
                                      "user_properties": properties,
                                  })

    def _make_request(self, method, url, post_data=None, get_data=None):
        headers = {}

        if post_data:
            post_data_json = json.dumps(post_data, cls=DecimalEncoder)
            headers["content-type"] = "application/json"
        else:
            post_data_json = None

        if not get_data:
            get_data = {}
        get_data["firebase_app_id"] = FIREBASE_APP_ID
        get_data["api_secret"] = FIREBASE_API_SECRET

        response = self._backoff_request(method,
                                         GA_API_URL + url,
                                         params=get_data,
                                         data=post_data_json,
                                         headers=headers)

        try:
            response_data = response.json()
        except:
            response_data = None

        status_code = response.status_code
        logging_extra = {
            "headers": headers,
            "get_data": get_data,
            "post_data": post_data,
            "status_code": status_code,
            "response_data": response_data,
        }

        if status_code is None or status_code < 200 or status_code > 299:
            logger.error("[FIREBASE] %s %s" % (method, url),
                         extra=logging_extra)

            raise HttpException(status_code, "Firebase request failed")

        logger.info("[FIREBASE] %s %s" % (method, url), extra=logging_extra)

        return response_data

    @backoff.on_predicate(backoff.expo,
                          predicate=lambda res: res.status_code == 429,
                          max_tries=10,
                          jitter=lambda v: v / 2 + full_jitter(v / 2))
    def _backoff_request(self,
                         method,
                         url,
                         params=None,
                         data=None,
                         headers=None):
        return requests.request(method,
                                url,
                                params=params,
                                data=data,
                                headers=headers)

    def _get_profile_app_instance_id(self, profile_id):
        metadata = self.repository.get_analytics_metadata(
            profile_id, ANALYTICS_METADATA_SERVICE_FIREBASE)

        if not metadata or not metadata["app_instance_id"]:
            e = InvalidAnalyticsMetadata()
            logger.exception(e,
                             extra={
                                 "profile_id": profile_id,
                                 "metadata": metadata
                             })
            raise e

        return metadata["app_instance_id"]
