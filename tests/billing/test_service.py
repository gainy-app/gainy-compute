from gainy.analytics.service import AnalyticsService
from gainy.billing.models import InvoiceStatus, Invoice, PaymentMethod, PaymentTransaction, TransactionStatus
from gainy.billing.repository import BillingRepository
from gainy.billing.service import BillingService
from gainy.billing.stripe.provider import StripePaymentProvider
from gainy.tests.common import TestContextContainer

from gainy.tests.mocks.repository_mocks import mock_persist, mock_noop, mock_record_calls


def test_create_invoices(monkeypatch):
    repo = BillingRepository(None)
    service = BillingService(repo, None, None)

    create_invoices_called = False

    def mock_create_invoices():
        nonlocal create_invoices_called
        create_invoices_called = True

    monkeypatch.setattr(repo, "create_invoices", mock_create_invoices)

    service.create_invoices()

    assert create_invoices_called


def test_charge_invoices(monkeypatch):
    with TestContextContainer() as context_container:
        repo = BillingRepository(context_container.db_conn)
        service = BillingService(repo, None, None)

        invoice = Invoice()

        def mock_iterate_unpaid_invoices_due():
            return [invoice]

        monkeypatch.setattr(repo, "iterate_unpaid_invoices_due",
                            mock_iterate_unpaid_invoices_due)

        charge_called = {}

        def mock_charge(invoice):
            charge_called[invoice] = True

        monkeypatch.setattr(service, "charge", mock_charge)

        service.charge_invoices()

        assert invoice in charge_called


def test_charge(monkeypatch):
    profile_id = 1
    amount = 2

    transaction = PaymentTransaction()
    monkeypatch.setattr(transaction, "status", TransactionStatus.SUCCESS)

    invoice = Invoice()
    monkeypatch.setattr(invoice, "can_charge", lambda: True)
    monkeypatch.setattr(invoice, "profile_id", profile_id)
    monkeypatch.setattr(invoice, "amount", amount)

    stripe_payment_provider = StripePaymentProvider(None, None)

    payment_method = PaymentMethod()
    monkeypatch.setattr(payment_method, "provider",
                        stripe_payment_provider.provider_id)

    def mock_charge(_invoice, _payment_method):
        assert _invoice == invoice
        assert _payment_method == payment_method

        return transaction

    monkeypatch.setattr(stripe_payment_provider, "charge", mock_charge)

    repo = BillingRepository(None)

    def mock_get_active_payment_method(_profile_id):
        assert _profile_id == profile_id
        return payment_method

    monkeypatch.setattr(repo, "get_active_payment_method",
                        mock_get_active_payment_method)
    persisted_objects = {}
    monkeypatch.setattr(repo, "persist", mock_persist(persisted_objects))
    monkeypatch.setattr(repo, "commit", mock_noop)

    analytics_service = AnalyticsService(None, None, None)
    on_commission_withdrawn_calls = []
    monkeypatch.setattr(analytics_service, "on_commission_withdrawn",
                        mock_record_calls(on_commission_withdrawn_calls))
    service = BillingService(repo, analytics_service, stripe_payment_provider)
    service.charge(invoice)

    assert invoice.status == InvoiceStatus.PAID
    assert invoice in persisted_objects[Invoice]
    assert ((profile_id, amount), {}) in on_commission_withdrawn_calls