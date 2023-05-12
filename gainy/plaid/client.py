import datetime
from functools import cached_property

import plaid
from plaid.model.accounts_balance_get_request import AccountsBalanceGetRequest
from plaid.model.accounts_balance_get_request_options import AccountsBalanceGetRequestOptions
from typing import List

from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions
from plaid.model.identity_get_request import IdentityGetRequest
from plaid.model.identity_get_request_options import IdentityGetRequestOptions

from gainy.plaid.common import get_plaid_client
from gainy.plaid.models import PlaidAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class PlaidClient:

    @cached_property
    def _default_client(self):
        return get_plaid_client()

    @cached_property
    def _sandbox_client(self):
        return get_plaid_client('sandbox')

    @cached_property
    def _development_client(self):
        return get_plaid_client('development')

    def get_item_accounts(self,
                          access_token: str,
                          account_ids: List = None) -> List[PlaidAccount]:
        if account_ids:
            options = AccountsGetRequestOptions(account_ids=account_ids)
            request = AccountsGetRequest(access_token=access_token,
                                         options=options)
        else:
            request = AccountsGetRequest(access_token=access_token)

        logging_extra = {
            "access_token": access_token,
            "account_ids": account_ids,
        }

        try:
            response = self.get_client(access_token).accounts_get(request)

            logging_extra["response_data"] = response.to_dict()
            logging_extra["requestId"] = response.request_id
        except plaid.ApiException as e:
            logger.exception("[PLAID] get_item_accounts",
                             e,
                             extra=logging_extra)
            raise e

        logger.info("[PLAID] get_item_accounts", extra=logging_extra)

        return [PlaidAccount(i) for i in response['accounts']]

    def get_identity(self,
                     access_token: str,
                     account_ids: List = None) -> List[PlaidAccount]:
        if account_ids:
            options = IdentityGetRequestOptions(account_ids=account_ids)
            request = IdentityGetRequest(access_token=access_token,
                                         options=options)
        else:
            request = IdentityGetRequest(access_token=access_token)

        logging_extra = {
            "access_token": access_token,
            "account_ids": account_ids,
        }

        try:
            response = self.get_client(access_token).identity_get(request)

            logging_extra["response_data"] = response.to_dict()
            logging_extra["requestId"] = response.request_id
        except plaid.ApiException as e:
            logger.exception("[PLAID] get_identity", e, extra=logging_extra)
            raise e

        logger.info("[PLAID] get_identity", extra=logging_extra)

        return [PlaidAccount(i) for i in response['accounts']]

    def get_item_accounts_balances(
            self,
            access_token: str,
            account_ids: List = None) -> List[PlaidAccount]:
        min_last_updated_datetime = datetime.datetime.now(
            tz=datetime.timezone.utc) - datetime.timedelta(days=7)
        options = AccountsBalanceGetRequestOptions(
            min_last_updated_datetime=min_last_updated_datetime)
        if account_ids:
            options.account_ids = account_ids
        request = AccountsBalanceGetRequest(access_token=access_token,
                                            options=options)

        logging_extra = {
            "access_token": access_token,
            "account_ids": account_ids,
        }

        try:
            response = self.get_client(access_token).accounts_balance_get(
                request)

            logging_extra["response_data"] = response.to_dict()
            logging_extra["requestId"] = response.request_id
        except plaid.ApiException as e:
            logger.exception("[PLAID] get_item_accounts_balances",
                             e,
                             extra=logging_extra)
            raise e

        logger.info("[PLAID] get_item_accounts_balances", extra=logging_extra)

        return [PlaidAccount(i) for i in response['accounts']]

    def get_client(self, access_token):
        if access_token and access_token.find('sandbox') > -1:
            return self._sandbox_client
        if access_token and access_token.find('development') > -1:
            return self._development_client

        return self._default_client
