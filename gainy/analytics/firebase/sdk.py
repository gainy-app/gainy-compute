import json

import backoff
import requests
from backoff import full_jitter

from gainy.data_access.models import DecimalEncoder
from gainy.exceptions import HttpException
from gainy.utils import get_logger

logger = get_logger(__name__)

GA_API_URL = "https://www.google-analytics.com"
FIREBASE_APP_ID = "1:378836078681:ios:96f00acc54c24486106148"
FIREBASE_API_SECRET = "dL-voVM0Tka0YfDqqMbpVw"
FIREBASE_APP_INSTANCE_ID = None #todo


class FirebaseSDK:

    # https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events?client_type=firebase
    def send_event(self, profile_id, name, params: dict):
        return self._make_request("POST", f"/mp/collect", post_data={
            "app_instance_id": FIREBASE_APP_INSTANCE_ID,
            "user_id": profile_id,
            "events": [{
              "name": name,
              "params": params,
            }]
          })

    # https://developers.google.com/analytics/devguides/collection/protocol/ga4/user-properties?client_type=firebase
    def send_user_properties(self, profile_id, properties: dict):
        return self._make_request("POST", f"/mp/collect", post_data={
            "app_instance_id": FIREBASE_APP_INSTANCE_ID,
            "user_id": profile_id,
              "user_properties": {

                k: {
                  "value": i
                } for k, i in properties.items()
              },

          })

    def _make_request(self,
                      method,
                      url,
                      post_data=None,
                      get_data=None):
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
        response = requests.request(method,
                                    url,
                                    params=params,
                                    data=data,
                                    headers=headers)

        status_code = response.status_code
        logging_extra = {
            "headers": headers,
            "get_data": params,
            "post_data": data,
            "status_code": status_code,
            "response_data": response.text,
        }

        logger.info("[FIREBASE] %s %s" % (method, url), extra=logging_extra)

        return response


