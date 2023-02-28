from gainy.analytics.service import AnalyticsService
from gainy.billing.exceptions import PaymentProviderNotSupportedException, InvoiceSealedException
from gainy.billing.interfaces import BillingServiceInterface
from gainy.billing.locking_functions import ChargeInvoice
from gainy.billing.models import Invoice, PaymentMethod
from gainy.billing.provider import AbstractPaymentProvider
from gainy.billing.repository import BillingRepository
from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.utils import get_logger

logger = get_logger(__name__)


class BillingService(BillingServiceInterface):

    def __init__(self, repo: BillingRepository,
                 analytics_service: AnalyticsService,
                 providers: list[AbstractPaymentProvider]):
        self.repo = repo
        self.analytics_service = analytics_service
        self._providers = providers

    def create_invoices(self):
        self.repo.create_invoices()

    def charge_invoices(self):
        for invoice in self.repo.iterate_unpaid_invoices_due():
            func = ChargeInvoice(self.repo, self, invoice)
            try:
                func.execute()
            except LockAcquisitionTimeout as e:
                logger.exception(e)

    def charge(self, invoice: Invoice):
        try:
            if not invoice.can_charge():
                raise InvoiceSealedException()

            payment_method = self.repo.get_active_payment_method(
                invoice.profile_id)
            provider = self._get_payment_method_provider(payment_method)
            transaction = provider.charge(invoice, payment_method)
            invoice.on_new_transaction(transaction)
            self.repo.persist(invoice)

            self.repo.commit()
            self.analytics_service.on_commission_withdrawn(
                invoice.profile_id, float(invoice.amount))
            return transaction
        except Exception as e:
            logger.exception(e)
            self.repo.rollback()

    def _get_payment_method_provider(
            self, payment_method: PaymentMethod) -> AbstractPaymentProvider:
        for provider in self._providers:
            if provider.supports(payment_method):
                return provider

        raise PaymentProviderNotSupportedException()
