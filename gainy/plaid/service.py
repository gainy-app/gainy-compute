from gainy.plaid.client import PlaidClient

import plaid

from gainy.plaid.common import handle_error


class PlaidService:

    def __init__(self):
        self.plaid_client = PlaidClient()

    def get_item_accounts(self, access_token, account_ids=None):
        try:
            return self.plaid_client.get_item_accounts(access_token,
                                                       account_ids)
        except plaid.ApiException as e:
            handle_error(e)
