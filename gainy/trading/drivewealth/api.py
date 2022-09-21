import os
import requests

from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.exceptions import ApiException
from gainy.trading.drivewealth.locking_functions.update_drive_wealth_auth_token import UpdateDriveWealthAuthToken
from gainy.trading.drivewealth.models import DriveWealthAuthToken
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

DRIVEWEALTH_APP_KEY = os.getenv("DRIVEWEALTH_APP_KEY")
DRIVEWEALTH_WLP_ID = os.getenv("DRIVEWEALTH_WLP_ID")
DRIVEWEALTH_PARENT_IBID = os.getenv("DRIVEWEALTH_PARENT_IBID")
DRIVEWEALTH_RIA_ID = os.getenv("DRIVEWEALTH_RIA_ID")
DRIVEWEALTH_RIA_PRODUCT_ID = os.getenv("DRIVEWEALTH_RIA_PRODUCT_ID")
DRIVEWEALTH_API_USERNAME = os.getenv("DRIVEWEALTH_API_USERNAME")
DRIVEWEALTH_API_PASSWORD = os.getenv("DRIVEWEALTH_API_PASSWORD")
DRIVEWEALTH_API_URL = os.getenv("DRIVEWEALTH_API_URL")


class DriveWealthApi:
    _token_data = None

    def __init__(self, repository: DriveWealthRepository):
        self.repository = repository

    def get_account_money(self, account_id: str):
        return self._make_request("GET",
                                  f"/accounts/{account_id}/summary/money")

    def get_account_positions(self, account_id: str):
        return self._make_request("GET",
                                  f"/accounts/{account_id}/summary/positions")

    def get_user_accounts(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/accounts")

    def get_auth_token(self):
        return self._make_request(
            "POST", "/auth", {
                "appTypeID": 4,
                "username": DRIVEWEALTH_API_USERNAME,
                "password": DRIVEWEALTH_API_PASSWORD
            })

    def _get_token(self, force_token_refresh: bool = False):
        token = self.repository.get_latest_auth_token()

        if force_token_refresh or not token or token.is_expired():
            token = self._refresh_token(force_token_refresh)

        return token.auth_token

    def _refresh_token(self, force: bool) -> DriveWealthAuthToken:
        func = UpdateDriveWealthAuthToken(self.repository, self, force)
        try:
            return func.execute()
        except LockAcquisitionTimeout as e:
            logger.exception(e)
            raise e

    def _make_request(self,
                      method,
                      url,
                      post_data=None,
                      force_token_refresh=False):
        headers = {"dw-client-app-key": DRIVEWEALTH_APP_KEY}

        if url != "/auth":
            headers["dw-auth-token"] = self._get_token(force_token_refresh)

        response = requests.request(method,
                                    DRIVEWEALTH_API_URL + url,
                                    json=post_data,
                                    headers=headers)

        try:
            response_data = response.json()
        except:
            response_data = None

        status_code = response.status_code
        logging_extra = {
            "post_data": post_data,
            "status_code": status_code,
            "response_data": response_data,
            "requestId": response.headers.get("dw-request-id"),
        }

        if status_code is None or status_code < 200 or status_code > 299:
            if status_code == 401 and response_data.get(
                    "errorCode") == "L075" and not force_token_refresh:
                logger.info('[DRIVEWEALTH] token expired', extra=logging_extra)
                return self._make_request(method, url, post_data, True)

            logger.error("[DRIVEWEALTH] %s %s" % (method, url),
                         extra=logging_extra)

            if response_data is not None and 'message' in response_data:
                raise ApiException(
                    "%s: %s" %
                    (response_data["errorCode"], response_data["message"]))
            else:
                raise ApiException("Failed: %d" % status_code)

        logger.info("[DRIVEWEALTH] %s %s" % (method, url), extra=logging_extra)

        return response_data
