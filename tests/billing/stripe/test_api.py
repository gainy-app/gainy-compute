from decimal import Decimal

import stripe
from stripe.api_resources.payment_intent import PaymentIntent

from gainy.billing.models import PaymentTransactionStatus
from gainy.billing.stripe.api import StripeApi
from gainy.billing.stripe.models import StripePaymentMethod


def test_charge(monkeypatch):
    amount = Decimal(1)
    payment_method_ref_id = "payment_method_ref_id"
    customer_ref_id = "customer_ref_id"
    payment_intent_ref_id = "payment_intent_ref_id"

    stripe_payment_method = StripePaymentMethod()
    monkeypatch.setattr(stripe_payment_method, "ref_id", payment_method_ref_id)
    monkeypatch.setattr(stripe_payment_method, "customer_ref_id",
                        customer_ref_id)

    def mock_api_create(**kwargs):
        assert kwargs["amount"] == round(amount * 100)
        assert kwargs["currency"] == 'usd'
        assert kwargs["customer"] == customer_ref_id
        assert kwargs["payment_method"] == payment_method_ref_id
        assert kwargs["off_session"]
        assert kwargs["confirm"]

        return PaymentIntent.construct_from(
            {
                "id": payment_intent_ref_id,
                "status": "succeeded"
            }, "key")

    monkeypatch.setattr(stripe.PaymentIntent, "create", mock_api_create)

    api = StripeApi()
    stripe_payment_intent = api.create_payment_intent(stripe_payment_method,
                                                      amount)

    assert stripe_payment_intent.ref_id == payment_intent_ref_id
    assert stripe_payment_intent.status == PaymentTransactionStatus.SUCCESS
