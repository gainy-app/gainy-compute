from gainy.analytics.service import AnalyticsService
from gainy.billing.models import PaymentMethod, PaymentMethodProvider
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls, mock_noop
from gainy.trading.drivewealth.locking_functions.handle_accounts_updated_event import HandleAccountsUpdatedEvent
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser, DriveWealthPortfolio
from gainy.trading.models import TradingAccount
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth.repository import DriveWealthRepository


def test_exists(monkeypatch):
    account_id = "account_id"
    status_name = "status_name"
    old_status = "old_status"
    was_open = True
    drivewealth_user_id = "drivewealth_user_id"
    profile_id = 1

    drivewealth_user = DriveWealthUser()
    drivewealth_user.profile_id = profile_id

    account = DriveWealthAccount()
    account.status = old_status
    account.drivewealth_user_id = drivewealth_user_id
    monkeypatch.setattr(account, 'is_open', lambda: was_open)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthAccount, {
            "ref_id": account_id
        }, account),
                   (DriveWealthUser, {
                       "ref_id": drivewealth_user_id
                   }, drivewealth_user)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))
    monkeypatch.setattr(repository, 'commit', mock_noop)

    provider = DriveWealthProvider(None, None, None, None, None)
    handle_account_status_change_calls = []
    monkeypatch.setattr(provider, 'handle_account_status_change',
                        mock_record_calls(handle_account_status_change_calls))
    ensure_trading_account_created_calls = []
    monkeypatch.setattr(
        provider, 'ensure_trading_account_created',
        mock_record_calls(ensure_trading_account_created_calls))

    message = {
        "accountID": account_id,
        "current": {
            "status": {
                'name': status_name
            },
        },
    }

    func = HandleAccountsUpdatedEvent(repository, provider, None, None,
                                      message)
    ensure_portfolio_calls = []
    monkeypatch.setattr(func, 'ensure_portfolio',
                        mock_record_calls(ensure_portfolio_calls))
    send_event_calls = []
    monkeypatch.setattr(func, 'send_event',
                        mock_record_calls(send_event_calls))
    create_payment_method_calls = []
    monkeypatch.setattr(func, 'create_payment_method',
                        mock_record_calls(create_payment_method_calls))
    func._do(None)

    assert DriveWealthAccount in persisted_objects
    assert account in persisted_objects[DriveWealthAccount]
    assert (account, ) in [args for args, kwargs in ensure_portfolio_calls]
    assert account.status == status_name
    assert (account, old_status) in [
        args for args, kwargs in handle_account_status_change_calls
    ]
    assert (account, profile_id) in [
        args for args, kwargs in ensure_trading_account_created_calls
    ]
    assert (profile_id,
            was_open) in [args for args, kwargs in send_event_calls]
    assert (account, profile_id) in [
        args for args, kwargs in create_payment_method_calls
    ]


def test_not_exists(monkeypatch):
    account_id = "account_id"
    drivewealth_user_id = "drivewealth_user_id"
    profile_id = 1

    drivewealth_user = DriveWealthUser()
    drivewealth_user.profile_id = profile_id

    account = DriveWealthAccount()
    account.drivewealth_user_id = drivewealth_user_id
    monkeypatch.setattr(account, "is_open", lambda: True)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthAccount, {
            "ref_id": account_id
        }, None),
                   (DriveWealthUser, {
                       "ref_id": drivewealth_user_id
                   }, drivewealth_user)]))
    monkeypatch.setattr(repository, 'commit', mock_noop)

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_trading_account(account_ref_id, fetch_info):
        assert fetch_info
        assert account_ref_id == account_id
        return account

    monkeypatch.setattr(provider, 'sync_trading_account',
                        mock_sync_trading_account)
    ensure_trading_account_created_calls = []
    monkeypatch.setattr(
        provider, 'ensure_trading_account_created',
        mock_record_calls(ensure_trading_account_created_calls))

    message = {
        "accountID": account_id,
    }
    func = HandleAccountsUpdatedEvent(repository, provider, None, None,
                                      message)
    ensure_portfolio_calls = []
    monkeypatch.setattr(func, 'ensure_portfolio',
                        mock_record_calls(ensure_portfolio_calls))
    send_event_calls = []
    monkeypatch.setattr(func, 'send_event',
                        mock_record_calls(send_event_calls))
    create_payment_method_calls = []
    monkeypatch.setattr(func, 'create_payment_method',
                        mock_record_calls(create_payment_method_calls))

    func._do(None)

    assert (account, ) in [args for args, kwargs in ensure_portfolio_calls]
    assert (account, profile_id) in [
        args for args, kwargs in ensure_trading_account_created_calls
    ]
    assert (profile_id, False) in [args for args, kwargs in send_event_calls]
    assert (account, profile_id) in [
        args for args, kwargs in create_payment_method_calls
    ]


def test_ensure_portfolio(monkeypatch):
    trading_account_id = 1
    profile_id = 2

    portfolio = DriveWealthPortfolio()

    account = DriveWealthAccount()
    account.trading_account_id = trading_account_id
    monkeypatch.setattr(account, "is_open", lambda: True)

    trading_account = TradingAccount()
    trading_account.profile_id = profile_id
    trading_account.id = trading_account_id

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(TradingAccount, {
            "id": trading_account_id
        }, trading_account)]))

    provider = DriveWealthProvider(None, None, None, None, None)
    ensure_portfolio_calls = []

    def mock_ensure_portfolio(*args, **kwargs):
        mock_record_calls(ensure_portfolio_calls)(*args, **kwargs)
        return portfolio

    monkeypatch.setattr(provider, 'ensure_portfolio', mock_ensure_portfolio)

    func = HandleAccountsUpdatedEvent(repository, provider, None, None, None)
    func.ensure_portfolio(account)

    assert (profile_id, trading_account_id) in [
        args for args, kwargs in ensure_portfolio_calls
    ]


def test_send_event(monkeypatch):
    profile_id = 2

    analytics_service = AnalyticsService(None, None, None)
    on_dw_brokerage_account_opened_calls = []
    monkeypatch.setattr(
        analytics_service, 'on_dw_brokerage_account_opened',
        mock_record_calls(on_dw_brokerage_account_opened_calls))

    func = HandleAccountsUpdatedEvent(None, None, analytics_service, None,
                                      None)
    func.send_event(profile_id, False)

    assert ((profile_id, ), {}) in on_dw_brokerage_account_opened_calls


def test_create_payment_method(monkeypatch):
    profile_id = 1
    payment_method_id = 2
    account_ref_no = "account_ref_no"

    account = DriveWealthAccount()
    account.ref_no = account_ref_no

    repository = DriveWealthRepository(None)
    persisted_objects = {}

    def _mock_persist(entity):
        mock_persist(persisted_objects)(entity)
        if isinstance(entity, PaymentMethod):
            entity.id = payment_method_id

    monkeypatch.setattr(repository, 'persist', _mock_persist)

    func = HandleAccountsUpdatedEvent(repository, None, None, None, None)
    func.create_payment_method(account, profile_id)

    assert PaymentMethod in persisted_objects
    payment_method = persisted_objects[PaymentMethod][0]
    assert payment_method.profile_id == profile_id
    assert payment_method.provider == PaymentMethodProvider.DRIVEWEALTH
    assert payment_method.name == f"Trading Account {account.ref_no}"
    assert payment_method.set_active_at is not None

    assert DriveWealthAccount in persisted_objects
    assert account in persisted_objects[DriveWealthAccount]
    assert account.payment_method_id == payment_method_id
