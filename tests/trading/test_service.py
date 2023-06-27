from _decimal import Decimal

from gainy.plaid.models import PlaidAccessToken, PlaidAccount
from gainy.plaid.service import PlaidService
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist
from gainy.trading.drivewealth.models import DriveWealthDeposit
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.service import TradingService
from gainy.trading.repository import TradingRepository
from gainy.trading.models import FundingAccount, TradingAccount, TradingMoneyFlow, TradingMoneyFlowStatus, \
    TradingMoneyFlowType


def test_update_funding_accounts_balance(monkeypatch):
    plaid_access_token_id = 1
    plaid_account_id = 2
    balance_current = 3
    access_token = "access_token"

    funding_account = FundingAccount()
    monkeypatch.setattr(funding_account, "plaid_access_token_id",
                        plaid_access_token_id)
    monkeypatch.setattr(funding_account, "plaid_account_id", plaid_account_id)

    plaid_access_token = PlaidAccessToken()
    monkeypatch.setattr(plaid_access_token, "access_token", access_token)

    plaid_account = PlaidAccount()
    monkeypatch.setattr(plaid_account, "account_id", plaid_account_id)
    monkeypatch.setattr(plaid_account, "balance_current", balance_current)

    trading_repository = TradingRepository(None)
    monkeypatch.setattr(
        trading_repository, "find_one",
        mock_find([
            (PlaidAccessToken, {
                "id": plaid_access_token_id
            }, plaid_access_token),
        ]))
    persisted_objects = {}
    monkeypatch.setattr(trading_repository, "persist",
                        mock_persist(persisted_objects))

    plaid_service = PlaidService(None)

    def mock_get_item_accounts(_access_token, plaid_account_ids):
        assert _access_token == plaid_access_token
        assert plaid_account_ids == [plaid_account_id]
        return [plaid_account]

    monkeypatch.setattr(plaid_service, "get_item_accounts_balances",
                        mock_get_item_accounts)

    trading_service = TradingService(trading_repository, None, plaid_service)
    trading_service.update_funding_accounts_balance([funding_account])

    assert funding_account in persisted_objects[FundingAccount]
    assert funding_account.balance == balance_current


def test_reward_with_cash(monkeypatch):
    amount = Decimal(1)
    trading_account_id = 2
    profile_id = 3
    money_flow_id = 4

    trading_account = TradingAccount()
    trading_account.profile_id = profile_id
    trading_account.id = trading_account_id

    drivewealth_deposit = DriveWealthDeposit()

    trading_repository = TradingRepository(None)
    persisted_objects = {}

    def custom_mock_persist(*args):
        mock_persist(persisted_objects)(*args)

        if isinstance(args[0], TradingMoneyFlow):
            args[0].id = money_flow_id

    monkeypatch.setattr(trading_repository, "persist", custom_mock_persist)

    drivewealth_provider = DriveWealthProvider(None, None, None, None, None)

    def mock_reward_with_cash(_trading_account_id, _amount):
        assert _trading_account_id == trading_account_id
        assert _amount == amount
        return drivewealth_deposit

    monkeypatch.setattr(drivewealth_provider, "reward_with_cash",
                        mock_reward_with_cash)

    trading_service = TradingService(trading_repository, drivewealth_provider,
                                     None)
    money_flow = trading_service.reward_with_cash(trading_account, amount)

    assert money_flow in persisted_objects[TradingMoneyFlow]
    assert drivewealth_deposit in persisted_objects[DriveWealthDeposit]
    assert money_flow.profile_id == trading_account.profile_id
    assert money_flow.status == TradingMoneyFlowStatus.PENDING
    assert money_flow.type == TradingMoneyFlowType.CASH_REWARD
    assert money_flow.amount == amount
    assert money_flow.trading_account_id == trading_account.id
    assert drivewealth_deposit.money_flow_id == money_flow_id
