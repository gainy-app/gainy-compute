import datetime
import multiprocessing.dummy
import time
from decimal import Decimal

from gainy.billing.models import Invoice, InvoiceStatus
from gainy.tests.common import TestContextContainer


def _charge_invoices(monkeypatch, invoice: Invoice):
    charges_count = 0

    with TestContextContainer() as context_container:
        billing_repository = context_container.billing_repository
        monkeypatch.setattr(billing_repository, "iterate_unpaid_invoices_due",
                            lambda: [invoice])

        billing_service = context_container.billing_service

        def mock_charge(invoice: Invoice):
            if not invoice.can_charge():
                return

            nonlocal charges_count
            charges_count += 1

            invoice.status = InvoiceStatus.PAID
            billing_repository.persist(invoice)

        monkeypatch.setattr(billing_service, "charge", mock_charge)

        billing_service.charge_invoices()

    return invoice, charges_count


def test_charge_invoices(monkeypatch):
    threads_count = 5

    invoice = Invoice()
    invoice.profile_id = 1
    invoice.period_id = 'test_' + str(time.time())
    invoice.amount = Decimal(100)
    invoice.due_date = datetime.datetime.now()
    invoice.description = ""
    invoice.period_start = datetime.datetime.now()
    invoice.period_end = datetime.datetime.now()
    with TestContextContainer() as context_container:
        context_container.get_repository().persist(invoice)
    invoice_id = invoice.id

    with multiprocessing.dummy.Pool(threads_count) as pool:
        result = pool.starmap(_charge_invoices,
                              [(monkeypatch, invoice)] * threads_count)

    invoices = [i[0] for i in result]
    charges_count = [i[1] for i in result]

    assert len(charges_count) == threads_count
    assert sum(charges_count) == 1

    assert len(invoices) == threads_count
    for i in invoices:
        assert i == invoices[0]
        assert i.id == invoice_id
