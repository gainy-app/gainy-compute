from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_noop
from gainy.tests.mocks.trading.drivewealth.api_mocks import mock_get_user_accounts, mock_get_account_money, \
    mock_get_account_positions, mock_get_account, PORTFOLIO_STATUS, CASH_VALUE, FUND1_ID, FUND2_ID, FUND2_VALUE, \
    FUND1_VALUE
from gainy.trading.models import TradingAccount
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository, DriveWealthProvider

from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser, DriveWealthAccountMoney, \
    DriveWealthAccountPositions, DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthInstrument, \
    DriveWealthPortfolioStatus


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

    service = DriveWealthProvider(drivewealth_repository, api)

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
    monkeypatch.setattr(
        drivewealth_repository, "find_one",
        mock_find([
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

    service = DriveWealthProvider(drivewealth_repository, api)
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


def test_update_portfolio(monkeypatch):
    portfolio = DriveWealthPortfolio()

    drivewealth_repository = DriveWealthRepository(None)
    monkeypatch.setattr(drivewealth_repository, "persist", mock_noop)

    provider = DriveWealthProvider(drivewealth_repository, None)

    def mock_get_portfolio_status(_portfolio):
        assert _portfolio == portfolio
        status = DriveWealthPortfolioStatus()
        status.set_from_response(PORTFOLIO_STATUS)
        return status

    monkeypatch.setattr(provider, "get_portfolio_status",
                        mock_get_portfolio_status)

    portfolio_status = provider.get_portfolio_status(portfolio)

    assert portfolio_status
    assert portfolio_status.cash_value == CASH_VALUE
    assert portfolio_status.get_fund_value(FUND1_ID) == FUND1_VALUE
    assert portfolio_status.get_fund_value(FUND2_ID) == FUND2_VALUE


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

    provider = DriveWealthProvider(drivewealth_repository, api)

    instrument = provider.sync_instrument(ref_id=instrument_ref_id,
                                          symbol=instrument_symbol)

    assert DriveWealthInstrument in persisted_objects
    assert persisted_objects[DriveWealthInstrument]

    assert instrument in persisted_objects[DriveWealthInstrument]
    assert instrument.ref_id == instrument_ref_id
    assert instrument.symbol == instrument_symbol
    assert instrument.status == instrument_status
    assert instrument.data == instrument_data
