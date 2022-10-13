from gainy.billing.models import Invoice, PaymentMethod, PaymentTransaction, PaymentMethodProvider
from gainy.billing.provider import AbstractPaymentProvider
from gainy.billing.stripe.api import StripeApi
from gainy.billing.stripe.models import StripePaymentMethod
from gainy.billing.stripe.repository import StripeRepository


class StripePaymentProvider(AbstractPaymentProvider):

    @property
    def provider_id(self):
        return PaymentMethodProvider.STRIPE

    def __init__(self, repo: StripeRepository, api: StripeApi):
        self.repo = repo
        self.api = api

    def supports(self, payment_method: PaymentMethod) -> bool:
        return payment_method.provider == self.provider_id

    def charge(self, invoice: Invoice,
               payment_method: PaymentMethod) -> PaymentTransaction:
        stripe_payment_method = self.repo.find_one(
            StripePaymentMethod, {"payment_method_id": payment_method.id})
        if not stripe_payment_method:
            raise Exception('StripePaymentMethod not found')

        transaction = self._create_transaction(invoice, payment_method)
        self.repo.persist(transaction)

        stripe_payment_intent = self.api.create_payment_intent(
            stripe_payment_method, invoice.amount)
        stripe_payment_intent.payment_transaction_id = transaction.id
        self.repo.persist(stripe_payment_intent)

        stripe_payment_intent.update_transaction(transaction)
        self.repo.persist(transaction)

        return transaction
