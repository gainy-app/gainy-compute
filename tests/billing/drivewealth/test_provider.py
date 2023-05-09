from _decimal import Decimal

import pytest

from gainy.billing.drivewealth.provider import DriveWealthPaymentProvider
from gainy.billing.models import Invoice, PaymentMethod, PaymentTransaction

from gainy.tests.mocks.repository_mocks import mock_persist as base_mock_persist, mock_find
from gainy.trading.drivewealth.provider.provider import DriveWealthProvider
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository
from gainy.trading.drivewealth.config import DRIVEWEALTH_HOUSE_ACCOUNT_NO
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthRedemption, DriveWealthAccountMoney, \
    DriveWealthPortfolio, DriveWealthPortfolioStatus
from gainy.trading.exceptions import InsufficientFundsException


def get_test_charge_is_pending_rebalance():
    return [True, False]


def get_test_charge_sufficient_funds():
    return [True, False]


@pytest.mark.parametrize("is_pending_rebalance",
                         get_test_charge_is_pending_rebalance())
@pytest.mark.parametrize("sufficient_funds",
                         get_test_charge_sufficient_funds())
def test_charge(monkeypatch, is_pending_rebalance, sufficient_funds):
    payment_method_id = 1
    invoice_id = 2
    profile_id = 3
    amount = 4
    transaction_id = 5
    description = "description"
    trading_account_id = 6

    account_money = DriveWealthAccountMoney()
    account_money.cash_balance = Decimal(amount)

    invoice = Invoice()
    invoice.id = invoice_id
    invoice.profile_id = profile_id
    invoice.amount = Decimal(amount)
    invoice.description = description

    dw_account = DriveWealthAccount()
    dw_account.trading_account_id = trading_account_id
    payment_method = PaymentMethod()
    monkeypatch.setattr(payment_method, "id", payment_method_id)
    monkeypatch.setattr(payment_method, "profile_id", profile_id)

    portfolio = DriveWealthPortfolio()
    portfolio_status = DriveWealthPortfolioStatus()
    monkeypatch.setattr(portfolio_status, "is_pending_rebalance",
                        lambda: is_pending_rebalance)

    if is_pending_rebalance:
        cash_target_weight = Decimal(0.1)
        if sufficient_funds:
            equity_value = Decimal(amount) / cash_target_weight
        else:
            equity_value = Decimal(amount) / cash_target_weight - Decimal(1)

        monkeypatch.setattr(portfolio, "cash_target_weight",
                            cash_target_weight)
        monkeypatch.setattr(portfolio_status, "equity_value", equity_value)
    else:
        if sufficient_funds:
            cash_value = Decimal(amount)
        else:
            cash_value = Decimal(amount - 1)
        monkeypatch.setattr(portfolio_status, "cash_value", cash_value)

    redemption = DriveWealthRedemption()

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_ensure_portfolio(_profile_id, _trading_account_id):
        assert _profile_id == profile_id
        assert _trading_account_id == trading_account_id
        return portfolio

    monkeypatch.setattr(provider, "ensure_portfolio", mock_ensure_portfolio)

    def mock_sync_portfolio_status(_portfolio, force):
        assert _portfolio == portfolio
        assert force
        return portfolio_status

    monkeypatch.setattr(provider, "sync_portfolio_status",
                        mock_sync_portfolio_status)

    repo = DriveWealthRepository(None)
    persisted_objects = {}

    def mock_persist(object):
        if isinstance(object, PaymentTransaction):
            object.id = transaction_id
        return base_mock_persist(persisted_objects)(object)

    monkeypatch.setattr(repo, "persist", mock_persist)
    monkeypatch.setattr(
        repo, "find_one",
        mock_find([
            (DriveWealthAccount, {
                "payment_method_id": payment_method_id
            }, dw_account),
        ]))

    api = DriveWealthApi(None)

    def mock_create_redemption(_amount, _account, **kwargs):
        assert _amount == amount
        assert _account == dw_account
        assert kwargs["_type"] == "CASH_TRANSFER"
        assert kwargs["transaction_code"] == "FEE_AUM"
        assert kwargs["partner_account_no"] == DRIVEWEALTH_HOUSE_ACCOUNT_NO
        assert kwargs["note"] == f"AUM FEE {invoice.description} #{invoice.id}"
        return redemption

    monkeypatch.setattr(api, "create_redemption", mock_create_redemption)

    payment_provider = DriveWealthPaymentProvider(provider, repo, api)

    if not sufficient_funds:
        with pytest.raises(Exception) as e:
            payment_provider.charge(invoice, payment_method)
            assert isinstance(e, InsufficientFundsException)
        assert not persisted_objects
        return

    transaction = payment_provider.charge(invoice, payment_method)

    assert transaction in persisted_objects[PaymentTransaction]
    assert transaction.payment_method_id == payment_method_id
    assert transaction.invoice_id == invoice_id
    assert transaction.profile_id == profile_id

    assert redemption in persisted_objects[DriveWealthRedemption]
