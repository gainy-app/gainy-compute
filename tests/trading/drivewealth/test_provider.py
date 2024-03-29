import datetime
import pytest

from decimal import Decimal

from gainy.analytics.service import AnalyticsService
from gainy.data_access.operators import OperatorIn, OperatorNot
from gainy.services.notification import NotificationService
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_noop, mock_record_calls
from gainy.tests.mocks.trading.drivewealth.api_mocks import mock_get_user_accounts, mock_get_account_money, \
    mock_get_account_positions, mock_get_account, PORTFOLIO_STATUS, FUND1_ID, USER_ID, PORTFOLIO, PORTFOLIO_REF_ID, \
    FUND1_TARGET_WEIGHT, FUND2_ID, CASH_ACTUAL_VALUE, PORTFOLIO_STATUS_EQUITY_VALUE, CASH_TARGET_WEIGHT, \
    FUND2_TARGET_WEIGHT
from gainy.trading.drivewealth.provider.base import DRIVE_WEALTH_PORTFOLIO_REBALANCE_TIME_LAG
from gainy.trading.drivewealth.provider.misc import normalize_symbol
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.models import TradingAccount, TradingCollectionVersion, TradingOrder, TradingOrderStatus
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository

from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser, DriveWealthAccountMoney, \
    DriveWealthAccountPositions, DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthInstrument, \
    DriveWealthPortfolioStatus, DriveWealthFund, PRECISION, DriveWealthPortfolioHolding, DriveWealthAccountStatus, \
    DriveWealthTransaction, DriveWealthRedemption, DriveWealthOrder
from gainy.trading.repository import TradingRepository
from gainy.utils import ENV_PRODUCTION

_ACCOUNT_ID = "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557"


def test_sync_profile_trading_accounts(monkeypatch):
    cash_balance_list = 10
    cash_available_for_trade_list = 20
    cash_available_for_withdrawal_list = 30
    profile_id = 5
    account_id = "account_ref_id"
    user_ref_id = "user_ref_id"
    trading_account_id = "trading_account_id"

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "trading_account_id", trading_account_id)

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", user_ref_id)

    persisted_objects = {}
    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "ref_id": account_id
            }, account),
            (DriveWealthUser, {
                "profile_id": profile_id
            }, user),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "get_user_accounts",
        mock_get_user_accounts(
            user_ref_id,
            account_id,
            cash_balance=cash_balance_list,
            cash_available_for_trade=cash_available_for_trade_list,
            cash_available_for_withdrawal=cash_available_for_withdrawal_list))

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)

    def mock_sync_trading_account(account_ref_id):
        assert account_ref_id == account_id

    monkeypatch.setattr(service, "sync_trading_account",
                        mock_sync_trading_account)
    service.sync_profile_trading_accounts(profile_id)

    assert DriveWealthAccount in persisted_objects

    assert persisted_objects[DriveWealthAccount][0] == account
    assert account.ref_id == account_id
    assert account.cash_balance == cash_balance_list
    assert account.cash_available_for_trade == cash_available_for_trade_list
    assert account.cash_available_for_withdrawal == cash_available_for_withdrawal_list


def test_sync_trading_account(monkeypatch):
    cash_balance_list = 10
    cash_available_for_trade_list = 20
    cash_available_for_withdrawal_list = 30
    cash_balance = 1
    cash_available_for_trade = 2
    cash_available_for_withdrawal = 3
    equity_value = 4
    account_ref_id = "account_ref_id"
    user_ref_id = "user_ref_id"
    trading_account_id = 5

    trading_account = TradingAccount()

    account = DriveWealthAccount()
    monkeypatch.setattr(account, "trading_account_id", trading_account_id)
    monkeypatch.setattr(account, "ref_id", account_ref_id)
    monkeypatch.setattr(account, "drivewealth_user_id", user_ref_id)

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", user_ref_id)

    persisted_objects = {}
    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    def _mock_find(options):
        _mock = mock_find(options)

        def mock(_cls, _fltr=None, _order=None):
            if _cls == DriveWealthAccountMoney:
                return None
            if _cls == DriveWealthAccountPositions:
                return None
            return _mock(_cls, _fltr, _order)

        return mock

    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        _mock_find([
            (DriveWealthUser, {
                "ref_id": user_ref_id,
            }, user),
            (DriveWealthAccount, {
                "ref_id": account_ref_id,
                "trading_account_id": trading_account_id,
            }, account),
            (TradingAccount, {
                "id": trading_account_id
            }, trading_account),
        ]))

    api = DriveWealthApi(drivewealth_repository)
    monkeypatch.setattr(
        api, "get_account",
        mock_get_account(
            account_ref_id,
            user_ref_id,
            cash_balance=cash_balance_list,
            cash_available_for_trade=cash_available_for_trade_list,
            cash_available_for_withdrawal=cash_available_for_withdrawal_list))
    monkeypatch.setattr(
        api, "get_account_money",
        mock_get_account_money(
            account_ref_id,
            cash_balance=cash_balance,
            cash_available_for_trade=cash_available_for_trade,
            cash_available_for_withdrawal=cash_available_for_withdrawal))
    monkeypatch.setattr(
        api, "get_account_positions",
        mock_get_account_positions(account_ref_id, equity_value=equity_value))

    service = DriveWealthProvider(drivewealth_repository, api, None, None,
                                  None)
    service.sync_trading_account(account_ref_id=account_ref_id,
                                 trading_account_id=trading_account_id,
                                 fetch_info=True)

    assert DriveWealthAccount in persisted_objects
    assert DriveWealthAccountMoney in persisted_objects
    assert DriveWealthAccountPositions in persisted_objects
    assert TradingAccount in persisted_objects

    assert persisted_objects[DriveWealthAccount][0] == account
    assert account.ref_id == account_ref_id
    assert account.cash_balance == cash_balance_list
    assert account.cash_available_for_trade == cash_available_for_trade_list
    assert account.cash_available_for_withdrawal == cash_available_for_withdrawal_list

    account_money: DriveWealthAccountMoney = persisted_objects[
        DriveWealthAccountMoney][0]
    assert account_money.drivewealth_account_id == account_ref_id
    assert account_money.cash_balance == cash_balance
    assert account_money.cash_available_for_trade == cash_available_for_trade
    assert account_money.cash_available_for_withdrawal == cash_available_for_withdrawal

    account_positions: DriveWealthAccountPositions = persisted_objects[
        DriveWealthAccountPositions][0]
    assert account_positions.drivewealth_account_id == account_ref_id
    assert account_positions.equity_value == equity_value

    assert persisted_objects[TradingAccount][0] == trading_account
    assert trading_account.cash_balance == cash_balance
    assert trading_account.cash_available_for_trade == cash_available_for_trade
    assert trading_account.cash_available_for_withdrawal == cash_available_for_withdrawal
    assert trading_account.equity_value == equity_value


def test_sync_instrument(monkeypatch):
    instrument_ref_id = "instrument_ref_id"
    instrument_symbol = "symbol"
    instrument_status = str(DriveWealthInstrumentStatus.ACTIVE)
    instrument_data = {
        "instrumentID": instrument_ref_id,
        "symbol": instrument_symbol,
        "status": instrument_status,
    }

    drivewealth_repository = DriveWealthRepository(None)
    api = DriveWealthApi(None)

    def mock_get_instrument_details(ref_id: str = None, symbol: str = None):
        assert ref_id == instrument_ref_id
        assert symbol == instrument_symbol
        return instrument_data

    persisted_objects = {}
    monkeypatch.setattr(api, "get_instrument_details",
                        mock_get_instrument_details)
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    provider = DriveWealthProvider(drivewealth_repository, api, None, None,
                                   None)

    instrument = provider.sync_instrument(ref_id=instrument_ref_id,
                                          symbol=instrument_symbol)

    assert DriveWealthInstrument in persisted_objects
    assert persisted_objects[DriveWealthInstrument]

    assert instrument in persisted_objects[DriveWealthInstrument]
    assert instrument.ref_id == instrument_ref_id
    assert instrument.symbol == instrument_symbol
    assert instrument.status == instrument_status
    assert instrument.data == instrument_data


def test_ensure_portfolio(monkeypatch):
    profile_id = 1

    user = DriveWealthUser()
    monkeypatch.setattr(user, "ref_id", USER_ID)
    account = DriveWealthAccount()
    monkeypatch.setattr(account, "ref_id", _ACCOUNT_ID)
    monkeypatch.setattr(account, "status", DriveWealthAccountStatus.OPEN.name)

    def mock_get_user(_profile_id):
        assert _profile_id == profile_id
        return user

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)
    monkeypatch.setattr(drivewealth_repository, "get_user", mock_get_user)
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([(DriveWealthPortfolio, {
            "profile_id": profile_id,
            "drivewealth_account_id": _ACCOUNT_ID,
        }, None)]))

    api = DriveWealthApi(None)

    def mock_create_portfolio(portfolio, _name, _client_portfolio_id,
                              _description):
        assert _client_portfolio_id == profile_id
        portfolio.set_from_response(PORTFOLIO)

    monkeypatch.setattr(api, "create_portfolio", mock_create_portfolio)

    def mock_update_account(_account_ref_id, _portfolio_ref_id):
        assert _account_ref_id == _ACCOUNT_ID
        assert _portfolio_ref_id == PORTFOLIO_REF_ID

    monkeypatch.setattr(api, "update_account", mock_update_account)

    provider = DriveWealthProvider(drivewealth_repository, api, None, None,
                                   None)
    portfolio = provider.ensure_portfolio(profile_id, account)

    assert portfolio.ref_id == PORTFOLIO_REF_ID
    assert portfolio.drivewealth_account_id == _ACCOUNT_ID
    assert portfolio.data == PORTFOLIO
    assert portfolio.get_fund_weight(FUND1_ID) == FUND1_TARGET_WEIGHT


def test_send_portfolio_to_api(monkeypatch):
    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)

    drivewealth_repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(drivewealth_repository, "persist",
                        mock_persist(persisted_objects))

    fund_ref_ids = portfolio.get_fund_ref_ids()
    funds = []
    for ref_id in fund_ref_ids:
        fund = DriveWealthFund()
        fund.ref_id = ref_id
        funds.append(fund)
    monkeypatch.setattr(
        drivewealth_repository, "find_all",
        mock_find([(DriveWealthFund, {
            "ref_id": OperatorIn(portfolio.get_fund_ref_ids())
        }, funds)]))

    api = DriveWealthApi(None)
    update_portfolio_calls = []
    monkeypatch.setattr(api, "update_portfolio",
                        mock_record_calls(update_portfolio_calls))
    update_fund_calls = []
    monkeypatch.setattr(api, "update_fund",
                        mock_record_calls(update_fund_calls))

    provider = DriveWealthProvider(drivewealth_repository, api, None, None,
                                   None)
    remove_inactive_instruments_calls = []
    monkeypatch.setattr(provider, "remove_inactive_instruments",
                        mock_record_calls(remove_inactive_instruments_calls))
    provider.send_portfolio_to_api(portfolio)

    assert portfolio in [args[0] for args, kwargs in update_portfolio_calls]
    assert portfolio in persisted_objects[DriveWealthPortfolio]
    assert set(funds) == set([args[0] for args, kwargs in update_fund_calls])
    assert set(funds) == set(
        [args[0] for args, kwargs in remove_inactive_instruments_calls])


def test_remove_inactive_instruments(monkeypatch):
    active_instrument_id = "active_instrument_id"
    inactive_instrument_id = "inactive_instrument_id"
    fund_instrument_ids = [active_instrument_id, inactive_instrument_id]

    active_instrument = DriveWealthInstrument()
    active_instrument.ref_id = active_instrument_id
    active_instruments = [active_instrument]

    fund = DriveWealthFund()
    monkeypatch.setattr(fund, "get_instrument_ids",
                        lambda: fund_instrument_ids)
    remove_instrument_ids_calls = []
    monkeypatch.setattr(fund, "remove_instrument_ids",
                        mock_record_calls(remove_instrument_ids_calls))

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    monkeypatch.setattr(
        repository, "find_all",
        mock_find([(DriveWealthInstrument, {
            "ref_id": OperatorIn(fund_instrument_ids),
            "status": DriveWealthInstrumentStatus.ACTIVE.name,
        }, active_instruments)]))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.remove_inactive_instruments(fund)


def test_execute_order_in_portfolio(monkeypatch):
    profile_id = 1
    target_amount_delta = 2
    trading_account_id = 3

    trading_order = TradingOrder()
    monkeypatch.setattr(trading_order, "profile_id", profile_id)
    monkeypatch.setattr(trading_order, "trading_account_id",
                        trading_account_id)
    monkeypatch.setattr(trading_order, "target_amount_delta",
                        target_amount_delta)

    portfolio = DriveWealthPortfolio()
    fund = DriveWealthFund()

    repository = DriveWealthRepository(None)

    def mock_get_profile_portfolio(*args):
        assert args[0] == profile_id
        assert args[1] == trading_account_id
        return portfolio

    monkeypatch.setattr(repository, "get_profile_portfolio",
                        mock_get_profile_portfolio)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    def mock_ensure_fund(*args):
        if args[1] == profile_id and args[2] == trading_order:
            return fund
        raise Exception(f"unknown args {args}")

    monkeypatch.setattr(DriveWealthProviderRebalanceHelper, "ensure_fund",
                        mock_ensure_fund)

    handle_cash_amount_change_calls = []
    monkeypatch.setattr(DriveWealthProviderRebalanceHelper,
                        "handle_cash_amount_change",
                        mock_record_calls(handle_cash_amount_change_calls))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.execute_order_in_portfolio(portfolio, trading_order)
    assert (trading_order, portfolio, fund) in [
        args[1:] for args, kwargs in handle_cash_amount_change_calls
    ]
    assert portfolio in persisted_objects[DriveWealthPortfolio]
    assert trading_order in persisted_objects[TradingOrder]
    assert trading_order.status == TradingOrderStatus.PENDING_EXECUTION


def test_actualize_portfolio(monkeypatch):
    portfolio_changed = True

    portfolio = DriveWealthPortfolio()
    set_target_weights_from_status_actual_weights_calls = []
    monkeypatch.setattr(
        portfolio, "set_target_weights_from_status_actual_weights",
        mock_record_calls(set_target_weights_from_status_actual_weights_calls))

    portfolio_status = DriveWealthPortfolioStatus()
    monkeypatch.setattr(portfolio_status, "is_pending_rebalance",
                        lambda: False)

    portfolio_status.set_from_response(PORTFOLIO_STATUS)
    fund_ref_ids = portfolio_status.get_fund_ref_ids()
    funds = []
    for ref_id in fund_ref_ids:
        fund = DriveWealthFund()
        fund.ref_id = ref_id
        funds.append(fund)

    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))
    monkeypatch.setattr(
        repository, "find_all",
        mock_find([(DriveWealthFund, {
            "ref_id": OperatorIn(fund_ref_ids)
        }, funds)]))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.actualize_portfolio(portfolio, portfolio_status)

    assert portfolio_status in [
        args[0]
        for args, kwargs in set_target_weights_from_status_actual_weights_calls
    ]
    assert portfolio in persisted_objects[DriveWealthPortfolio]
    assert set(funds) == set(persisted_objects[DriveWealthFund])


def test_rebalance_portfolio_cash(monkeypatch):
    account_id = "account_id"
    last_transaction_id = 1
    new_transaction_id = 2
    new_equity_value = PORTFOLIO_STATUS_EQUITY_VALUE
    transaction_account_amount_delta = PORTFOLIO_STATUS_EQUITY_VALUE / 10
    redemption_amount = Decimal(-10)
    last_equity_value = new_equity_value - transaction_account_amount_delta - redemption_amount

    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)
    monkeypatch.setattr(portfolio, "drivewealth_account_id", account_id)
    monkeypatch.setattr(portfolio, "last_equity_value", last_equity_value)
    monkeypatch.setattr(portfolio, "last_transaction_id", last_transaction_id)
    monkeypatch.setattr(portfolio, "pending_redemptions_amount_sum",
                        Decimal(0))

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)

    redemption = DriveWealthRedemption()
    monkeypatch.setattr(DriveWealthRedemption, "amount",
                        property(lambda x: redemption_amount))

    transaction = DriveWealthTransaction()
    monkeypatch.setattr(transaction, "id", new_transaction_id)
    monkeypatch.setattr(transaction, "account_amount_delta",
                        transaction_account_amount_delta)

    repository = DriveWealthRepository(None)

    def mock_get_new_transactions(_account_id, _last_transaction_id):
        assert _last_transaction_id == last_transaction_id
        assert _account_id == account_id
        return [transaction]

    monkeypatch.setattr(repository, "get_new_transactions",
                        mock_get_new_transactions)
    monkeypatch.setattr(repository, "get_pending_redemptions",
                        lambda x: [redemption])
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None, None, None, None)

    assert portfolio.cash_target_weight == CASH_TARGET_WEIGHT
    assert portfolio.get_fund_weight(FUND1_ID) == FUND1_TARGET_WEIGHT
    assert portfolio.get_fund_weight(FUND2_ID) == FUND2_TARGET_WEIGHT

    provider.rebalance_portfolio_cash(portfolio, portfolio_status)

    assert abs(portfolio.cash_target_weight * new_equity_value -
               (CASH_TARGET_WEIGHT * last_equity_value +
                transaction_account_amount_delta +
                redemption_amount)) < PRECISION
    assert abs(
        portfolio.get_fund_weight(FUND1_ID) * new_equity_value -
        FUND1_TARGET_WEIGHT * last_equity_value) < PRECISION
    assert abs(
        portfolio.get_fund_weight(FUND2_ID) * new_equity_value -
        FUND2_TARGET_WEIGHT * last_equity_value) < PRECISION

    assert portfolio.last_transaction_id == new_transaction_id
    assert portfolio.last_equity_value == new_equity_value
    assert portfolio.pending_redemptions_amount_sum == redemption_amount

    assert DriveWealthPortfolio in persisted_objects
    assert portfolio in persisted_objects[DriveWealthPortfolio]


def get_test_rebalance_portfolio_cash_transaction_exists():
    return [True, False]


@pytest.mark.parametrize(
    "transaction_exists",
    get_test_rebalance_portfolio_cash_transaction_exists())
def test_rebalance_portfolio_cash_noop(monkeypatch, transaction_exists):
    account_id = "account_id"
    last_transaction_id = 1
    new_transaction_id = 2
    transaction_account_amount_delta = Decimal(0)
    last_equity_value = PORTFOLIO_STATUS_EQUITY_VALUE
    pending_redemptions_amount_sum = Decimal(0)

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.set_from_response(PORTFOLIO_STATUS)

    portfolio = DriveWealthPortfolio()
    portfolio.set_from_response(PORTFOLIO)
    monkeypatch.setattr(portfolio, "drivewealth_account_id", account_id)
    monkeypatch.setattr(portfolio, "last_equity_value", last_equity_value)
    monkeypatch.setattr(portfolio, "last_transaction_id", last_transaction_id)
    monkeypatch.setattr(portfolio, "pending_redemptions_amount_sum",
                        Decimal(0))

    transaction = DriveWealthTransaction()
    monkeypatch.setattr(transaction, "id", new_transaction_id)
    monkeypatch.setattr(transaction, "account_amount_delta",
                        transaction_account_amount_delta)

    repository = DriveWealthRepository(None)

    monkeypatch.setattr(
        repository, "get_new_transactions", lambda x, y: [transaction]
        if transaction_exists else [])

    monkeypatch.setattr(repository, "get_pending_redemptions", lambda x: [])
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider.rebalance_portfolio_cash(portfolio, portfolio_status)

    assert DriveWealthPortfolio in persisted_objects
    assert portfolio in persisted_objects[DriveWealthPortfolio]
    if transaction_exists:
        assert portfolio.last_transaction_id == new_transaction_id
    else:
        assert portfolio.last_transaction_id == last_transaction_id

    assert portfolio.last_equity_value == last_equity_value
    assert portfolio.pending_redemptions_amount_sum == pending_redemptions_amount_sum


def test_sync_portfolio(monkeypatch):
    data = {}

    portfolio = DriveWealthPortfolio()
    set_from_response_calls = []
    monkeypatch.setattr(portfolio, "set_from_response",
                        mock_record_calls(set_from_response_calls))

    persisted_objects = {}
    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    api = DriveWealthApi(None)

    def mock_get_portfolio(*args):
        if args[0] == portfolio:
            return data
        raise Exception(f"unknown args {args}")

    monkeypatch.setattr(api, "get_portfolio", mock_get_portfolio)

    provider = DriveWealthProvider(repository, api, None, None, None)
    provider.sync_portfolio(portfolio)
    assert data in [args[0] for args, kwargs in set_from_response_calls]
    assert portfolio in persisted_objects[DriveWealthPortfolio]


def test_sync_portfolio_status(monkeypatch):
    portfolio_status = DriveWealthPortfolioStatus()

    portfolio = DriveWealthPortfolio()
    update_from_status_calls = []
    monkeypatch.setattr(portfolio, "update_from_status",
                        mock_record_calls(update_from_status_calls))

    persisted_objects = {}
    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(repository, None, None, None, None)

    def mock_get_portfolio_status(*args, **kwargs):
        if args[0] == portfolio:
            return portfolio_status
        raise Exception(f"unknown args {args}")

    monkeypatch.setattr(provider, "_get_portfolio_status",
                        mock_get_portfolio_status)

    assert portfolio_status == provider.sync_portfolio_status(portfolio)

    assert portfolio_status in [
        args[0] for args, kwargs in update_from_status_calls
    ]
    assert portfolio in persisted_objects[DriveWealthPortfolio]


def test_get_portfolio_status(monkeypatch):
    portfolio = DriveWealthPortfolio()

    persisted_objects = {}
    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, "find_one", mock_noop)
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    api = DriveWealthApi(None)

    def mock_get_portfolio_status(*args, **kwargs):
        if args[0] == portfolio:
            return PORTFOLIO_STATUS
        raise Exception(f"unknown args {args}")

    monkeypatch.setattr(api, "get_portfolio_status", mock_get_portfolio_status)

    provider = DriveWealthProvider(repository, api, None, None, None)
    create_portfolio_holdings_from_status_calls = []
    monkeypatch.setattr(
        provider, "_create_portfolio_holdings_from_status",
        mock_record_calls(create_portfolio_holdings_from_status_calls))
    portfolio_status = provider._get_portfolio_status(portfolio)

    assert portfolio_status.drivewealth_portfolio_id == PORTFOLIO_STATUS["id"]
    assert DriveWealthPortfolioStatus in persisted_objects
    assert portfolio_status in persisted_objects[DriveWealthPortfolioStatus]
    assert portfolio_status in [
        args[0] for args, kwargs in create_portfolio_holdings_from_status_calls
    ]


def test_create_portfolio_holdings_from_status(monkeypatch):
    profile_id = 1
    collection_id = 2
    portfolio_status_id = 3
    symbol = "symbol"

    fund1 = DriveWealthFund()
    fund1.collection_id = collection_id
    fund2 = DriveWealthFund()
    fund2.symbol = symbol

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.id = portfolio_status_id
    portfolio_status.set_from_response(PORTFOLIO_STATUS)

    portfolio = DriveWealthPortfolio()
    portfolio.profile_id = profile_id

    persisted_objects = {}
    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([
            (DriveWealthPortfolio, {
                "ref_id": PORTFOLIO_REF_ID
            }, portfolio),
            (DriveWealthFund, {
                "ref_id": FUND1_ID
            }, fund1),
            (DriveWealthFund, {
                "ref_id": FUND2_ID
            }, fund2),
        ]))
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))
    delete_by_calls = []
    monkeypatch.setattr(repository, "delete_by",
                        mock_record_calls(delete_by_calls))

    provider = DriveWealthProvider(repository, None, None, None, None)
    provider._create_portfolio_holdings_from_status(portfolio_status)

    assert DriveWealthPortfolioHolding in persisted_objects
    holdings_by_id: dict[str, DriveWealthPortfolioHolding] = {
        i.holding_id_v2: i
        for i in persisted_objects[DriveWealthPortfolioHolding]
    }
    assert "dw_ttf_1_2_TSLA" in holdings_by_id
    assert "dw_ttf_1_2_AAPL" in holdings_by_id
    assert "dw_ticker_1_AAPL" in holdings_by_id
    assert "1_cash_CUR:USD" in holdings_by_id
    assert len(holdings_by_id) == 4

    for holding in holdings_by_id.values():
        assert holding.portfolio_status_id == portfolio_status_id
        assert holding.profile_id == profile_id

        if holding.holding_id_v2 == "dw_ticker_1_AAPL":
            continue
        if holding.holding_id_v2 == "1_cash_CUR:USD":
            assert holding.actual_value == Decimal(CASH_ACTUAL_VALUE)
            assert holding.quantity == Decimal(CASH_ACTUAL_VALUE)
            assert holding.symbol == "CUR:USD"
            continue

        idx = int(holding.holding_id_v2 == "dw_ttf_1_2_AAPL")
        assert holding.actual_value == Decimal(
            PORTFOLIO_STATUS["holdings"][1]["holdings"][idx]["value"])
        assert holding.quantity == Decimal(
            PORTFOLIO_STATUS["holdings"][1]["holdings"][idx]["openQty"])
        assert holding.symbol == normalize_symbol(
            PORTFOLIO_STATUS["holdings"][1]["holdings"][idx]["symbol"])
        assert holding.collection_uniq_id == f"0_{collection_id}"
        assert holding.collection_id == collection_id

    assert len(delete_by_calls) == 1
    call = delete_by_calls[0][0]
    assert call[0] == DriveWealthPortfolioHolding
    assert call[1]["profile_id"] == profile_id
    operator = call[1]["holding_id_v2"]
    assert isinstance(operator, OperatorNot)
    assert isinstance(operator.operator, OperatorIn)
    assert set(operator.operator.param) == set(holdings_by_id.keys())


def test_update_trading_orders_pending_execution_from_portfolio_status(
        monkeypatch):
    last_portfolio_rebalance_at = datetime.datetime.now()
    profile_id = 1
    trading_account_id = 2
    collection_id = 3
    symbol = "symbol"

    trading_order = TradingOrder()
    trading_order.symbol = symbol
    trading_order.status = TradingOrderStatus.EXECUTED_FULLY

    trading_collection_version = TradingCollectionVersion()
    trading_collection_version.collection_id = collection_id
    trading_collection_version.status = TradingOrderStatus.EXECUTED_FULLY

    trading_account = TradingAccount()
    trading_account.profile_id = profile_id
    trading_account.id = trading_account_id

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.last_portfolio_rebalance_at = last_portfolio_rebalance_at

    repository = TradingRepository(None)

    def mock_iterate_trading_orders(**kwargs):
        assert kwargs["profile_id"] == profile_id
        assert kwargs["trading_account_id"] == trading_account_id
        assert kwargs["status"] == TradingOrderStatus.PENDING_EXECUTION
        assert kwargs[
            "pending_execution_to"] == last_portfolio_rebalance_at - datetime.timedelta(
                seconds=DRIVE_WEALTH_PORTFOLIO_REBALANCE_TIME_LAG)
        return [trading_order, trading_collection_version]

    monkeypatch.setattr(repository, "iterate_trading_orders",
                        mock_iterate_trading_orders)

    def mock_get_trading_account_by_portfolio_status(*args):
        assert args[0] == portfolio_status
        return trading_account

    analytics_service = AnalyticsService(None, None, None)
    on_order_executed_calls = []
    monkeypatch.setattr(analytics_service, "on_order_executed",
                        mock_record_calls(on_order_executed_calls))

    provider = DriveWealthProvider(None, None, repository, None,
                                   analytics_service)
    monkeypatch.setattr(provider, "get_trading_account_by_portfolio_status",
                        mock_get_trading_account_by_portfolio_status)
    fill_executed_amount_calls = []
    monkeypatch.setattr(provider, "_fill_executed_amount",
                        mock_record_calls(fill_executed_amount_calls))

    provider.update_trading_orders_pending_execution_from_portfolio_status(
        portfolio_status)

    assert ((profile_id, [trading_order], portfolio_status), {
        "symbol": symbol
    }) in fill_executed_amount_calls
    assert ((profile_id, [trading_collection_version], portfolio_status), {
        "collection_id": collection_id
    }) in fill_executed_amount_calls
    assert ((trading_order, ), {}) in on_order_executed_calls
    assert ((trading_collection_version, ), {}) in on_order_executed_calls


def get_fill_executed_amount_data():
    pending_execution_since = datetime.datetime.now(
        tz=datetime.timezone.utc) - datetime.timedelta(minutes=1)

    def assert_persisted(orders, persisted_objects):
        assert TradingOrder in persisted_objects
        assert len(
            set(orders).intersection(set(
                persisted_objects[TradingOrder]))) == len(orders)

    def test_set1():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(100)
        trading_order1.pending_execution_since = pending_execution_since
        orders = [trading_order1]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert_persisted(orders, persisted_objects)

        return (1000, 1100, orders, assert_func)

    yield test_set1()

    def test_set2():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.PENDING_EXECUTION
            assert trading_order2.executed_amount == Decimal(110)
            assert_persisted(orders, persisted_objects)

        return (1000, 1160, orders, assert_func)

    yield test_set2()

    def test_set3():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(90)
            assert_persisted(orders, persisted_objects)

        return (1000, 1140, orders, assert_func)

    yield test_set3()

    def test_set4():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(100)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(-50)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(-40)
            assert_persisted(orders, persisted_objects)

        return (1000, 1060, orders, assert_func)

    yield test_set4()

    def test_set5():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(100)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(-50)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(-60)
            assert_persisted(orders, persisted_objects)

        return (1000, 1040, orders, assert_func)

    yield test_set5()

    def test_set6():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(-50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(110)
            assert_persisted(orders, persisted_objects)

        return (1000, 1060, orders, assert_func)

    yield test_set6()

    def test_set7():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(-50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(90)
            assert_persisted(orders, persisted_objects)

        return (1000, 1040, orders, assert_func)

    yield test_set7()

    def test_set8():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(-50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(-100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(-110)
            assert_persisted(orders, persisted_objects)

        return (1000, 840, orders, assert_func)

    yield test_set8()

    def test_set9():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(-50)
        trading_order1.pending_execution_since = pending_execution_since
        trading_order2 = TradingOrder()
        trading_order2.target_amount_delta = Decimal(-100)
        trading_order2.pending_execution_since = pending_execution_since
        orders = [trading_order1, trading_order2]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.status != TradingOrderStatus.EXECUTED_FULLY
            assert trading_order2.executed_amount == Decimal(-90)
            assert_persisted(orders, persisted_objects)

        return (1000, 860, orders, assert_func)

    yield test_set9()

    def test_set10():
        trading_order1 = TradingOrder()
        trading_order1.target_amount_delta = Decimal(-100)
        trading_order1.pending_execution_since = pending_execution_since
        orders = [trading_order1]

        def assert_func(persisted_objects):
            assert trading_order1.status == TradingOrderStatus.EXECUTED_FULLY
            assert_persisted(orders, persisted_objects)

        return (1000, 900, orders, assert_func)

    yield test_set10()


@pytest.mark.parametrize(
    "executed_amount_sum, cash_flow_sum, orders, assert_func",
    get_fill_executed_amount_data())
def test_fill_executed_amount(monkeypatch, executed_amount_sum, cash_flow_sum,
                              orders, assert_func):
    last_portfolio_rebalance_at = datetime.datetime.now(
        tz=datetime.timezone.utc)
    profile_id = 1
    collection_id = 2
    executed_amount_sum = Decimal(executed_amount_sum)
    cash_flow_sum = Decimal(cash_flow_sum)
    min_date = datetime.date.today()
    fund = DriveWealthFund()

    dw_repository = DriveWealthRepository(None)

    def mock_get_profile_fund(*args, **kwargs):
        assert args[0] == profile_id
        assert kwargs["collection_id"] == collection_id
        return fund

    monkeypatch.setattr(dw_repository, "get_profile_fund",
                        mock_get_profile_fund)

    repository = TradingRepository(None)

    def mock_get_last_selloff_date(*args, **kwargs):
        assert args[0] == profile_id
        assert kwargs["collection_id"] == collection_id
        return min_date

    monkeypatch.setattr(repository, "get_last_selloff_date",
                        mock_get_last_selloff_date)

    def mock_calculate_executed_amount_sum(*args, **kwargs):
        assert args[0] == profile_id
        assert kwargs["collection_id"] == collection_id
        assert kwargs["min_date"] == min_date
        return executed_amount_sum

    monkeypatch.setattr(repository, "calculate_executed_amount_sum",
                        mock_calculate_executed_amount_sum)

    def mock_calculate_cash_flow_sum(*args, **kwargs):
        assert args[0] == profile_id
        assert kwargs["collection_id"] == collection_id
        assert kwargs["min_date"] == min_date
        return cash_flow_sum

    monkeypatch.setattr(repository, "calculate_cash_flow_sum",
                        mock_calculate_cash_flow_sum)

    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    provider = DriveWealthProvider(dw_repository, None, repository, None, None)

    portfolio_status = DriveWealthPortfolioStatus()
    portfolio_status.equity_value = Decimal(100)
    portfolio_status.last_portfolio_rebalance_at = last_portfolio_rebalance_at
    monkeypatch.setattr(portfolio_status, "is_pending_rebalance", lambda: True)

    provider._fill_executed_amount(profile_id,
                                   orders,
                                   portfolio_status,
                                   collection_id=collection_id)

    assert_func(persisted_objects)


def test_handle_instrument_status_change(monkeypatch):
    symbol = "symbol"
    status = DriveWealthInstrumentStatus.ACTIVE
    new_status = "new_status"

    instrument = DriveWealthInstrument()
    monkeypatch.setattr(instrument, "status", status)
    monkeypatch.setattr(instrument, "symbol", symbol)

    repository = DriveWealthRepository(None)

    def mock_symbol_is_in_collection(_symbol):
        assert symbol == _symbol
        return True

    monkeypatch.setattr(repository, "symbol_is_in_collection",
                        mock_symbol_is_in_collection)

    notification_service = NotificationService(None, None)
    calls = []
    monkeypatch.setattr(notification_service,
                        "notify_dw_instrument_status_changed",
                        mock_record_calls(calls))

    provider = DriveWealthProvider(repository, None, None,
                                   notification_service, None)

    monkeypatch.setenv("ENV", ENV_PRODUCTION)
    provider.handle_instrument_status_change(instrument, new_status)

    assert (symbol, status, new_status) in [args for args, kwargs in calls]
