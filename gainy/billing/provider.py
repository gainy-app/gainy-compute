from abc import ABC, abstractmethod

from gainy.billing.models import Invoice, PaymentTransaction, PaymentMethod, TransactionStatus


class AbstractPaymentProvider(ABC):

    @abstractmethod
    def supports(self, payment_method: PaymentMethod) -> bool:
        pass

    @abstractmethod
    def charge(self, invoice: Invoice,
               payment_method: PaymentMethod) -> PaymentTransaction:
        pass

    def _create_transaction(
            self, invoice: Invoice,
            payment_method: PaymentMethod) -> PaymentTransaction:
        transaction = PaymentTransaction()
        transaction.payment_method_id = payment_method.id
        transaction.invoice_id = invoice.id
        transaction.profile_id = invoice.profile_id
        transaction.status = TransactionStatus.PENDING

        return transaction
