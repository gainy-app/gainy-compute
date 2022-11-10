import os
import json
import plaid
from plaid.api import plaid_api

from gainy.exceptions import HttpException
from gainy.utils import get_logger

logger = get_logger(__name__)

PURPOSE_PORTFOLIO = "portfolio"
PURPOSE_TRADING = "trading"

PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
PLAID_SECRET = os.getenv('PLAID_SECRET')
PLAID_DEVELOPMENT_SECRET = os.getenv('PLAID_DEVELOPMENT_SECRET')
PLAID_SANDBOX_SECRET = os.getenv('PLAID_SANDBOX_SECRET')
PLAID_ENV = os.getenv('PLAID_ENV')
PLAID_HOSTS = {
    'sandbox': plaid.Environment.Sandbox,
    'development': plaid.Environment.Development,
    'production': plaid.Environment.Production,
}

if PLAID_ENV not in PLAID_HOSTS:
    raise Exception('Wrong plaid env %s, available options are: %s' %
                    (PLAID_ENV, ",".join(PLAID_HOSTS.keys())))

DEFAULT_ENV = "development"


def get_purpose(input_params):
    purpose = input_params.get("purpose") or PURPOSE_PORTFOLIO
    if purpose not in [PURPOSE_PORTFOLIO, PURPOSE_TRADING]:
        raise Exception('Wrong purpose')
    return purpose


def get_purpose_products(purpose):
    if purpose == PURPOSE_PORTFOLIO:
        return ['investments']
    elif purpose == PURPOSE_TRADING:
        return ['auth']
    else:
        raise Exception('Wrong purpose')


def get_plaid_client(env=None):
    if env is None:
        env = PLAID_ENV

    host = PLAID_HOSTS[env]
    if env == 'development' and PLAID_DEVELOPMENT_SECRET:
        secret = PLAID_DEVELOPMENT_SECRET
    elif env == 'sandbox' and PLAID_SANDBOX_SECRET:
        secret = PLAID_SANDBOX_SECRET
    else:
        secret = PLAID_SECRET

    configuration = plaid.Configuration(host=host,
                                        api_key={
                                            'clientId': PLAID_CLIENT_ID,
                                            'secret': secret,
                                        })

    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    return client


def handle_error(e):
    logger.error('Plaid Error: %s' % (e.body))
    error = json.loads(e.body)

    raise HttpException(
        400, "Plaid error: %s" %
        (error['display_message'] or error['error_message']))
