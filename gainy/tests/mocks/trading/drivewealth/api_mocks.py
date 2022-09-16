def mock_get_user_accounts(user_ref_id,
                           account_ref_id,
                           cash_balance=None,
                           cash_available_for_trade=None,
                           cash_available_for_withdrawal=None):

    def mock(_user_ref_id):
        assert _user_ref_id == user_ref_id
        return [{
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
                "name": "OPEN",
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
            "userID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af",
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
                "advisorID":
                "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494",
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
        }]

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
