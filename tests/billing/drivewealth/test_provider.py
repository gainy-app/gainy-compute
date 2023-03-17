from _decimal import Decimal

from gainy.billing.drivewealth.provider import DriveWealthPaymentProvider
from gainy.billing.models import Invoice, PaymentMethod, PaymentTransaction, PaymentTransactionStatus

from gainy.tests.mocks.repository_mocks import mock_persist as base_mock_persist, mock_find, mock_record_calls
from gainy.trading.drivewealth import DriveWealthApi, DriveWealthRepository, DriveWealthProvider
from gainy.trading.drivewealth.config import DRIVEWEALTH_HOUSE_ACCOUNT_NO
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthRedemption, DriveWealthAccountMoney


def test_charge(monkeypatch):
    payment_method_id = 1
    invoice_id = 2
    profile_id = 3
    amount = 4
    transaction_id = 5
    description = "description"
    account_ref_id = "account_ref_id"

    account_money = DriveWealthAccountMoney()
    account_money.cash_balance = Decimal(amount)

    invoice = Invoice()
    invoice.id = invoice_id
    invoice.profile_id = profile_id
    invoice.amount = Decimal(amount)
    invoice.description = description

    dw_account = DriveWealthAccount()
    dw_account.ref_id = account_ref_id
    dw_account.cash_balance = amount
    payment_method = PaymentMethod()
    monkeypatch.setattr(payment_method, "id", payment_method_id)

    redemption = DriveWealthRedemption()

    provider = DriveWealthProvider(None, None, None, None)
    sync_account_money_calls = []

    def mock_sync_account_money(*args, **kwargs):
        mock_record_calls(sync_account_money_calls)(*args, **kwargs)
        return account_money

    monkeypatch.setattr(provider, "sync_account_money",
                        mock_sync_account_money)

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
    transaction = payment_provider.charge(invoice, payment_method)

    assert ((account_ref_id, ), {}) in sync_account_money_calls

    assert transaction in persisted_objects[PaymentTransaction]
    assert transaction.payment_method_id == payment_method_id
    assert transaction.invoice_id == invoice_id
    assert transaction.profile_id == profile_id

    assert redemption in persisted_objects[DriveWealthRedemption]
