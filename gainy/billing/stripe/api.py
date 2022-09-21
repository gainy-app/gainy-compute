import os
from decimal import Decimal

import stripe
import stripe.error

from gainy.billing.stripe.models import StripePaymentMethod, StripePaymentIntent
from gainy.utils import get_logger

logger = get_logger(__name__)

STRIPE_API_KEY = os.getenv('STRIPE_API_KEY')


class StripeApi:

    def __init__(self):
        stripe.api_key = STRIPE_API_KEY

    def create_payment_intent(self, payment_method: StripePaymentMethod,
                              amount: Decimal) -> StripePaymentIntent:
        payment_method_id = payment_method.ref_id
        customer_id = payment_method.customer_ref_id

        if not customer_id:
            raise Exception('empty customer_ref_id')

        try:
            payment_intent = stripe.PaymentIntent.create(
                amount=round(amount * 100),
                currency='usd',
                customer=customer_id,
                payment_method=payment_method_id,
                off_session=True,
                confirm=True,
            )
        except stripe.error.CardError as e:
            logger.warning(e)
            err = e.error
            # err.code will be authentication_required if authentication is needed
            payment_intent_id = err.payment_intent['id']
            payment_intent = stripe.PaymentIntent.retrieve(payment_intent_id)

        stripe_payment_intent = StripePaymentIntent()
        stripe_payment_intent.set_from_response(payment_intent)
        return stripe_payment_intent
