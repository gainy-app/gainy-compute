from decimal import Decimal

from gainy.trading.drivewealth.models import DriveWealthAccountStatus

USER_ID = "41dde78c-e31b-43e5-9418-44ae08098738"
CASH_TARGET_WEIGHT = Decimal(0.11)
CASH_ACTUAL_WEIGHT = Decimal(0.1)
CASH_ACTUAL_VALUE = 11001
CASH_TARGET_VALUE = CASH_ACTUAL_VALUE / CASH_ACTUAL_WEIGHT * CASH_TARGET_WEIGHT

PORTFOLIO_REF_ID = "portfolio_24338197-62da-4ac8-a0c9-3204e396f9c7"
PORTFOLIO_STATUS_EQUITY_VALUE = Decimal(110045.62)

FUND1_ID = "fund_b567f1d3-486e-4e5f-aacd-0d551113ebf6"
FUND1_ACTUAL_WEIGHT = Decimal(0.3054)
FUND1_TARGET_WEIGHT = Decimal(0.3)
FUND1_ACTUAL_VALUE = Decimal(33604.12)
FUND1_TARGET_VALUE = FUND1_ACTUAL_VALUE / FUND1_ACTUAL_WEIGHT * FUND1_TARGET_WEIGHT
FUND2_ID = "fund_9a51b36f-faac-41a7-8c9d-b15bb29a05fc"
FUND2_VALUE = Decimal(65440.5)
FUND2_TARGET_WEIGHT = Decimal(0.59)
FUND2_ACTUAL_WEIGHT = Decimal(0.5947)
PORTFOLIO_STATUS = {
    "id":
    PORTFOLIO_REF_ID,
    "lastPortfolioRebalance":
    None,
    "nextPortfolioRebalance":
    None,
    "equity":
    PORTFOLIO_STATUS_EQUITY_VALUE,
    "holdings": [{
        "id": None,
        "type": "CASH_RESERVE",
        "target": CASH_TARGET_WEIGHT,
        "actual": CASH_ACTUAL_WEIGHT,
        "value": CASH_ACTUAL_VALUE,
    }, {
        "id":
        FUND1_ID,
        "type":
        "FUND",
        "target":
        FUND1_TARGET_WEIGHT,
        "actual":
        FUND1_ACTUAL_WEIGHT,
        "value":
        FUND1_ACTUAL_VALUE,
        "holdings": [{
            "instrumentID": "5b85fabb-d57c-44e6-a7f6-a3efc760226c",
            "symbol": "TSLA",
            "target": 0.55,
            "actual": 0.6823,
            "openQty": 62.5213,
            "value": 22928.9
        }, {
            "instrumentID": "a67422af-8504-43df-9e63-7361eb0bd99e",
            "symbol": "AAPL",
            "target": 0.45,
            "actual": 0.3177,
            "openQty": 62.4942,
            "value": 10675.22
        }]
    }, {
        "id": FUND2_ID,
        "type": "FUND",
        "target": FUND2_TARGET_WEIGHT,
        "actual": FUND2_ACTUAL_WEIGHT,
        "value": FUND2_VALUE,
        "holdings": []
    }]
}

PORTFOLIO = {
    "id":
    PORTFOLIO_REF_ID,
    "userID":
    USER_ID,
    "holdings": [{
        "instrumentID": None,
        "type": "CASH_RESERVE",
        "target": CASH_TARGET_WEIGHT,
    }, {
        "instrumentID": FUND1_ID,
        "type": "FUND",
        "target": FUND1_TARGET_WEIGHT,
    }, {
        "instrumentID": FUND2_ID,
        "type": "FUND",
        "target": FUND2_TARGET_WEIGHT,
    }]
}


def _get_account_data(account_ref_id,
                      user_id="user_id",
                      cash_balance=None,
                      cash_available_for_trade=None,
                      cash_available_for_withdrawal=None):
    return {
        "id": account_ref_id,
        "accountNo": "GYEK000001",
        "accountType": {
            "name": "LIVE",
            "description": "Live Account"
        },
        "accountMgmtType": {
            "name": "RIA_MANAGED",
            "description": "Robo Advisor Managed Account"
        },
        "status": {
            "name": DriveWealthAccountStatus.OPEN.name,
            "description": "Open"
        },
        "tradingType": {
            "name": "CASH",
            "description": "Cash account"
        },
        "leverage": 1,
        "nickname": "Mikhail's Robo Advisor Managed Account",
        "parentIB": {
            "id": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2",
            "name": "Gainy"
        },
        "taxProfile": {
            "taxStatusCode": "W-9",
            "taxRecipientCode": "INDIVIDUAL"
        },
        "commissionID": "4dafc263-f73a-4972-bed0-3af9a6ee3d7d",
        "beneficiaries": False,
        "userID": user_id,
        "restricted": False,
        "goodFaithViolations": 0,
        "patternDayTrades": 0,
        "freeTradeBalance": 0,
        "gfvPdtExempt": False,
        "buyingPowerOverride": False,
        "bod": {
            "moneyMarket": 0,
            "equityValue": 0,
            "cashAvailableForWithdrawal": cash_available_for_withdrawal,
            "cashAvailableForTrading": cash_available_for_trade,
            "cashBalance": cash_balance
        },
        "ria": {
            "advisorID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494",
            "productID": "product_e5046072-eefc-47ed-90d4-60654c33cf92"
        },
        "sweepInd": True,
        "interestFree": False,
        "createdWhen": "2022-09-05T11:25:45.557Z",
        "openedWhen": "2022-09-13T05:29:45.689Z",
        "updatedWhen": "2022-09-14T13:03:16.911Z",
        "ignoreMarketHoursForTest": True,
        "flaggedForACATS": False,
        "extendedHoursEnrolled": False
    }


def mock_get_user_accounts(user_ref_id,
                           account_ref_id,
                           cash_balance=None,
                           cash_available_for_trade=None,
                           cash_available_for_withdrawal=None):

    def mock(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return [
            _get_account_data(
                account_ref_id=account_ref_id,
                cash_balance=cash_balance,
                cash_available_for_trade=cash_available_for_trade,
                cash_available_for_withdrawal=cash_available_for_withdrawal)
        ]

    return mock


def mock_get_account(account_ref_id,
                     user_id="user_id",
                     cash_balance=None,
                     cash_available_for_trade=None,
                     cash_available_for_withdrawal=None):

    def mock(_account_ref_id):
        assert _account_ref_id == account_ref_id
        return _get_account_data(
            account_ref_id=account_ref_id,
            user_id=user_id,
            cash_balance=cash_balance,
            cash_available_for_trade=cash_available_for_trade,
            cash_available_for_withdrawal=cash_available_for_withdrawal)

    return mock


def mock_get_account_money(account_ref_id,
                           cash_balance=None,
                           cash_available_for_trade=None,
                           cash_available_for_withdrawal=None):

    def mock(_account_ref_id):
        assert _account_ref_id == account_ref_id
        return {
            "accountID": account_ref_id,
            "cash": {
                "cashAvailableForTrade": cash_available_for_trade,
                "cashAvailableForWithdrawal": cash_available_for_withdrawal,
                "cashBalance": cash_balance,
            }
        }

    return mock


def mock_get_account_positions(account_ref_id, equity_value=None):

    def mock(_account_ref_id):
        assert _account_ref_id == account_ref_id
        return {"accountID": account_ref_id, "equityValue": equity_value}

    return mock
