import json

from gainy.plaid.client import PlaidClient

import plaid

from gainy.plaid.exceptions import AccessTokenLoginRequiredException, AccessTokenApiException
from gainy.plaid.models import PlaidAccessToken


class PlaidService:

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.plaid_client = PlaidClient()

    def get_item_accounts(self,
                          access_token: PlaidAccessToken,
                          account_ids=None):
        try:
            return self.plaid_client.get_item_accounts(
                access_token.access_token, account_ids)
        except plaid.ApiException as e:
            self._handle_api_exception(e, access_token)

    def _handle_api_exception(self, exc: plaid.ApiException,
                              access_token: PlaidAccessToken):

        if exc.body and isinstance(exc.body, dict):
            body = exc.body
        elif exc.body and isinstance(exc.body, str):
            body = json.loads(exc.body)
        else:
            body = {}

        if body.get("error_code") == "ITEM_LOGIN_REQUIRED":
            self._set_access_token_reauth(access_token)
            raise AccessTokenLoginRequiredException(exc,
                                                    access_token.to_dict())

        raise AccessTokenApiException(exc, access_token.to_dict())

    def _set_access_token_reauth(self, access_token: PlaidAccessToken):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_plaid_access_tokens set needs_reauth_since = now() where id = %(access_token_id)s",
                {"access_token_id": access_token.id})
