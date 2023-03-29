from decimal import Decimal

import json

import backoff
import requests
from backoff import full_jitter

from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.data_access.models import DecimalEncoder
from gainy.trading.drivewealth.config import DRIVEWEALTH_APP_KEY, DRIVEWEALTH_RIA_ID, DRIVEWEALTH_API_USERNAME, \
    DRIVEWEALTH_API_PASSWORD, DRIVEWEALTH_API_URL
from gainy.trading.drivewealth.exceptions import DriveWealthApiException
from gainy.trading.drivewealth.locking_functions.update_drive_wealth_auth_token import UpdateDriveWealthAuthToken
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthPortfolio, DriveWealthFund, \
    DriveWealthAccount, DriveWealthBankAccount, DriveWealthRedemption
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthApi:
    _token_data = None

    def __init__(self, repository: DriveWealthRepository):
        self.repository = repository

    def get_user(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}")

    def create_redemption(self,
                          amount: Decimal,
                          account: DriveWealthAccount,
                          bank_account: DriveWealthBankAccount = None,
                          _type='ACH',
                          transaction_code=None,
                          partner_account_no=None,
                          note=None) -> DriveWealthRedemption:
        params = {
            'accountNo': account.ref_no,
            'amount': amount,
            'currency': 'USD',
            'type': _type,
        }

        if bank_account:
            params['bankAccountID'] = bank_account.ref_id
        if transaction_code:
            params['transactionCode'] = transaction_code
        if note:
            params['note'] = note
        if partner_account_no:
            params['details'] = {"partnerAccountNo": partner_account_no}

        response = self._make_request("POST", "/funding/redemptions", params)
        entity = DriveWealthRedemption()
        entity.set_from_response(response)
        return entity

    def get_countries(self, status: str = None):
        get_data = {}
        if status:
            get_data["status"] = status
        return self._make_request("GET", "/countries", get_data=get_data)

    # Accounts

    def get_account(self, account_id: str):
        return self._make_request("GET", f"/accounts/{account_id}")["account"]

    def get_account_money(self, account_id: str):
        return self._make_request("GET",
                                  f"/accounts/{account_id}/summary/money")

    def get_account_positions(self, account_id: str):
        return self._make_request("GET",
                                  f"/accounts/{account_id}/summary/positions")

    def update_account(self, account_ref_id, portfolio_ref_id):
        return self._make_request("PATCH", f"/accounts/{account_ref_id}",
                                  {"ria": {
                                      "portfolioID": portfolio_ref_id
                                  }})

    def get_user_accounts(self, user_id: str):
        return self._make_request("GET", f"/users/{user_id}/accounts")

    # Portfolios
    def get_portfolio(self, portfolio: DriveWealthPortfolio):
        return self._make_request("GET",
                                  f"/managed/portfolios/{portfolio.ref_id}")

    def get_portfolio_status(self, portfolio: DriveWealthPortfolio):
        return self._make_request(
            "GET", f"/accounts/{portfolio.drivewealth_account_id}/portfolio")

    def create_portfolio(self, portfolio: DriveWealthPortfolio, name,
                         client_portfolio_id, description):
        data = self._make_request(
            "POST", "/managed/portfolios", {
                'userID':
                DRIVEWEALTH_RIA_ID,
                'name':
                name,
                'clientPortfolioID':
                client_portfolio_id,
                'description':
                description,
                'holdings': [{
                    "type": "CASH_RESERVE",
                    "target": 1
                }],
                "triggers": [{
                    "child": None,
                    "maxAllowed": 0.1,
                    "lowerBound": None,
                    "upperBound": None,
                    "type": "TOTAL_DRIFT"
                }]
            })
        portfolio.set_from_response(data)

    def update_portfolio(self, portfolio: DriveWealthPortfolio):
        holdings = []

        if portfolio.cash_target_weight > Decimal(0):
            holdings.append({
                "type": "CASH_RESERVE",
                "target": portfolio.cash_target_weight,
            })

        holdings += [{
            "type": "FUND",
            "id": fund_id,
            "target": weight,
        } for fund_id, weight in portfolio.holdings.items()]

        data = self._make_request(
            "PATCH", f"/managed/portfolios/{portfolio.ref_id}", {
                'holdings': holdings,
                "triggers": [{
                    "maxAllowed": 0.1,
                    "type": "TOTAL_DRIFT"
                }],
            })
        portfolio.set_from_response(data)

    def create_autopilot_run(self, account_ids: list):
        return self._make_request(
            "POST", f"/managed/autopilot/{DRIVEWEALTH_RIA_ID}", {
                'reviewOnly': False,
                'forceRebalance': True,
                'subAccounts': account_ids,
            })

    # Funds

    def create_fund(self, fund: DriveWealthFund, name, client_fund_id,
                    description):
        data = self._make_request(
            "POST", "/managed/funds", {
                'userID':
                DRIVEWEALTH_RIA_ID,
                'name':
                name,
                'clientFundID':
                client_fund_id,
                'description':
                description,
                'holdings':
                fund.holdings,
                'triggers': [{
                    "child": None,
                    "maxAllowed": 0.1,
                    "lowerBound": None,
                    "upperBound": None,
                    "type": "TOTAL_DRIFT"
                }]
            })
        fund.set_from_response(data)

    def update_fund(self, fund: DriveWealthFund):
        data = self._make_request(
            "PATCH", f"/managed/funds/{fund.ref_id}", {
                'holdings':
                fund.holdings,
                'triggers': [{
                    "child": None,
                    "maxAllowed": 0.1,
                    "lowerBound": None,
                    "upperBound": None,
                    "type": "TOTAL_DRIFT"
                }]
            })
        fund.set_from_response(data)

    # Instrument

    def get_instrument_details(self, ref_id: str = None, symbol: str = None):
        if ref_id:
            return self._make_request("GET", f"/instruments/{ref_id}")
        if symbol:
            return self._make_request("GET", f"/instruments/{symbol}")

        raise Exception('Either ref_id or symbol must be specified.')

    def get_instruments(self, status: str = "ACTIVE"):
        return self._make_request("GET",
                                  f"/instruments",
                                  get_data={"status": status})

    # Token

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
                      get_data=None,
                      force_token_refresh=False):
        headers = {"dw-client-app-key": DRIVEWEALTH_APP_KEY}

        if url != "/auth":
            headers["dw-auth-token"] = self._get_token(force_token_refresh)

        if post_data:
            post_data_json = json.dumps(post_data, cls=DecimalEncoder)
            headers["content-type"] = "application/json"
        else:
            post_data_json = None

        response = self._backoff_request(method,
                                         DRIVEWEALTH_API_URL + url,
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
            "requestId": response.headers.get("dw-request-id"),
        }

        if status_code is None or status_code < 200 or status_code > 299:
            if status_code == 401 and response_data.get(
                    "errorCode") == "L075" and not force_token_refresh:
                logger.info('[DRIVEWEALTH] token expired', extra=logging_extra)
                return self._make_request(method,
                                          url,
                                          post_data,
                                          force_token_refresh=True)

            logger.error("[DRIVEWEALTH] %s %s" % (method, url),
                         extra=logging_extra)

            raise DriveWealthApiException.create_from_response(
                response_data, status_code)

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
            "requestId": response.headers.get("dw-request-id"),
        }

        logger.info("[DRIVEWEALTH] %s %s" % (method, url), extra=logging_extra)

        return response
