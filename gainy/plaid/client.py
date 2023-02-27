from functools import cached_property
from typing import List

from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions

from gainy.plaid.common import get_plaid_client
from gainy.plaid.models import PlaidAccount


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
        # todo cache
        if account_ids:
            options = AccountsGetRequestOptions(account_ids=account_ids)
            request = AccountsGetRequest(access_token=access_token,
                                         options=options)
        else:
            request = AccountsGetRequest(access_token=access_token)

        response = self.get_client(access_token).accounts_get(request)

        return [PlaidAccount(i) for i in response['accounts']]

    def get_client(self, access_token):
        if access_token and access_token.find('sandbox') > -1:
            return self._sandbox_client
        if access_token and access_token.find('development') > -1:
            return self._development_client

        return self._default_client
