import json

import stripe
from stripe.api_resources.payment_intent import PaymentIntent
from stripe.api_resources.payment_method import PaymentMethod

from gainy.billing.models import PaymentTransactionStatus, PaymentTransaction
from gainy.data_access.models import BaseModel, classproperty


class BaseStripeModel(BaseModel):

    @classproperty
    def schema_name(self) -> str:
        return "app"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data),
        }


class StripePaymentMethod(BaseStripeModel):
    ref_id = None
    payment_method_id = None
    customer_ref_id = None
    name = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data: PaymentMethod):
        self.ref_id = data["id"]
        self.customer_ref_id = data.get("customer")
        self.data = stripe.util.convert_to_dict(data)
        self.name = self._get_payment_method_name()

    @classproperty
    def table_name(self) -> str:
        return "stripe_payment_methods"

    def _get_payment_method_name(self):
        pieces = [self.data["type"]]

        params = self.data[self.data["type"]]
        for field in [
                "bank_name", "bank", "bsb_number", "brand", "tax_id", "last4",
                "email"
        ]:
            if field not in params:
                continue
            pieces.append(params[field])

        return ' '.join(pieces)


class StripePaymentIntent(BaseStripeModel):
    ref_id = None
    status: PaymentTransactionStatus = None
    authentication_client_secret: str = None
    to_refund = None
    is_refunded = None,
    data = None
    refund_data = None
    payment_transaction_id = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["status"]:
            self.status = PaymentTransactionStatus(row["status"])
        return self

    def set_from_response(self, data: PaymentIntent):
        self.ref_id = data.id
        self.data = stripe.util.convert_to_dict(data)

        if data.status == 'succeeded':
            self.status = PaymentTransactionStatus.SUCCESS
        elif data.status == 'requires_action':
            self.status = PaymentTransactionStatus.REQUIRES_AUTHENTICATION
            self.authentication_client_secret = data.client_secret

            # TODO notify the app?
        else:
            raise Exception('Invalid PaymentIntent status')

    @classproperty
    def table_name(self) -> str:
        return "stripe_payment_intents"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "refund_data": json.dumps(self.refund_data),
        }

    def update_transaction(self, transaction: PaymentTransaction):
        transaction.status = self.status
