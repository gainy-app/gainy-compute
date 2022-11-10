from typing import List

from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.accounts_get_request_options import AccountsGetRequestOptions

from gainy.plaid.common import get_plaid_client
from gainy.plaid.models import PlaidAccount


class PlaidClient:

    def __init__(self):
        self.client = get_plaid_client()
        self.sandbox_client = get_plaid_client('sandbox')
        self.development_client = get_plaid_client('development')

    def get_item_accounts(self,
                          access_token,
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
            return self.sandbox_client
        if access_token and access_token.find('development') > -1:
            return self.development_client

        return self.client