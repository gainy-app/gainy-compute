from gainy.billing.models import Invoice, PaymentMethod, PaymentTransaction, TransactionStatus
from gainy.billing.stripe.api import StripeApi
from gainy.billing.stripe.models import StripePaymentMethod, StripePaymentIntent
from gainy.billing.stripe.provider import StripePaymentProvider
from gainy.billing.stripe.repository import StripeRepository

from gainy.tests.mocks.repository_mocks import mock_persist as base_mock_persist, mock_find


def test_charge(monkeypatch):
    payment_method_id = 1
    invoice_id = 2
    profile_id = 3
    amount = 4
    transaction_id = 5
    transaction_status = TransactionStatus.SUCCESS

    invoice = Invoice()
    monkeypatch.setattr(invoice, "id", invoice_id)
    monkeypatch.setattr(invoice, "profile_id", profile_id)
    monkeypatch.setattr(invoice, "amount", amount)

    stripe_payment_method = StripePaymentMethod()
    payment_method = PaymentMethod()
    monkeypatch.setattr(payment_method, "id", payment_method_id)

    stripe_payment_intent = StripePaymentIntent()
    monkeypatch.setattr(stripe_payment_intent, "status", transaction_status)

    repo = StripeRepository(None)
    persisted_objects = {}

    def mock_persist(object):
        if isinstance(object, PaymentTransaction):
            object.id = transaction_id
        return base_mock_persist(persisted_objects)(object)

    monkeypatch.setattr(repo, "persist", mock_persist)
    monkeypatch.setattr(
        repo, "find_one",
        mock_find([
            (StripePaymentMethod, {
                "payment_method_id": payment_method_id
            }, stripe_payment_method),
        ]))

    api = StripeApi()

    def mock_create_payment_intent(_stripe_payment_method, _amount):
        assert _stripe_payment_method == stripe_payment_method
        assert _amount == amount
        return stripe_payment_intent

    monkeypatch.setattr(api, "create_payment_intent",
                        mock_create_payment_intent)

    provider = StripePaymentProvider(repo, api)
    transaction = provider.charge(invoice, payment_method)
    monkeypatch.setattr(transaction, "id", transaction_id)

    assert transaction in persisted_objects[PaymentTransaction]
    assert transaction.payment_method_id == payment_method_id
    assert transaction.invoice_id == invoice_id
    assert transaction.profile_id == profile_id

    assert stripe_payment_intent in persisted_objects[StripePaymentIntent]
    assert stripe_payment_intent.payment_transaction_id == transaction_id
    assert stripe_payment_intent.status == transaction_status
