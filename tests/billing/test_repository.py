from _decimal import Decimal

import datetime
from typing import Any, Dict

import dateutil.relativedelta

from gainy.billing.models import InvoiceStatus, PaymentMethodProvider
from gainy.billing.repository import BILLING_MIN_YEARLY_FEE, BILLING_EQUITY_VALUE_FEE_MULTIPLIER
from gainy.tests.common import TestContextContainer
from psycopg2.extras import RealDictCursor


def test_create_invoices():
    profile_id = 1
    equity_value = 100
    start_of_month = datetime.datetime.today().replace(day=1,
                                                       hour=0,
                                                       minute=0,
                                                       second=0,
                                                       microsecond=0)
    period_start = start_of_month - dateutil.relativedelta.relativedelta(
        months=1)
    period_end = start_of_month
    days_in_month = (period_end - period_start).days

    year_start = datetime.date(period_start.year, 1, 1)
    year_end = year_start + dateutil.relativedelta.relativedelta(years=1)
    days_in_year = (year_end - year_start).days
    print(days_in_month, days_in_year)

    with TestContextContainer() as context_container:
        with context_container.db_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO drivewealth_monthly_usage (profile_id, period_start, period_end, equity_value)
                VALUES (%(profile_id)s, %(period_start)s, %(period_end)s, %(equity_value)s)
                on conflict do nothing;
                delete from app.invoices where profile_id = %(profile_id)s;
                """, {
                    "profile_id": profile_id,
                    "period_start": period_start,
                    "period_end": period_end,
                    "equity_value": equity_value
                })
        context_container.billing_repository.create_invoices()

        with context_container.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "SELECT * from app.invoices where profile_id = %(profile_id)s order by created_at desc",
                {"profile_id": profile_id})

            invoice: Dict[str, Any] = cursor.fetchone()

        assert invoice
        assert invoice["profile_id"] == profile_id
        assert invoice["period_id"] == "mo_" + period_start.strftime(
            "%Y-%m-%d")
        assert invoice["status"] == InvoiceStatus.PENDING
        assert abs(invoice["amount"] -
                   max(equity_value * BILLING_EQUITY_VALUE_FEE_MULTIPLIER,
                       BILLING_MIN_YEARLY_FEE) * days_in_month /
                   days_in_year) < Decimal(1e-3)
        assert invoice["due_date"] > period_end.date()
        assert invoice["period_start"] == period_start
        assert invoice["period_end"] == period_end


def test_iterate_unpaid_invoices_due():
    profile_id = 1
    amount = 100
    period_start = datetime.datetime.today().replace(day=1,
                                                     hour=0,
                                                     minute=0,
                                                     second=0,
                                                     microsecond=0)
    period_end = period_start + dateutil.relativedelta.relativedelta(
        months=1) - datetime.timedelta(seconds=1)

    with TestContextContainer() as context_container:
        with context_container.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                delete from app.invoices where profile_id = %(profile_id)s;
                insert into app.invoices(profile_id, period_id, amount, status, due_date, period_start, period_end)
                select %(profile_id)s, status || '_' || due_date::date, %(amount)s, status, due_date, %(period_start)s, %(period_end)s
                from (values(%(status_pending)s), (%(status_paid)s), (%(status_failed)s)) t1(status)
                join (values(now() - interval '1 week'), (now() + interval '1 week')) t2(due_date) on true;
                """, {
                    "profile_id": profile_id,
                    "amount": amount,
                    "period_start": period_start,
                    "period_end": period_end,
                    "status_pending": InvoiceStatus.PENDING,
                    "status_paid": InvoiceStatus.PAID,
                    "status_failed": InvoiceStatus.FAILED
                })

        invoices = list(
            context_container.billing_repository.iterate_unpaid_invoices_due())

        assert len(invoices) == 1
        for invoice in invoices:
            assert invoice.profile_id == profile_id
            assert invoice.amount == amount
            assert invoice.period_start == period_start
            assert invoice.period_end == period_end
            assert invoice.status == InvoiceStatus.PENDING


def test_get_active_payment_method():
    profile_id = 1
    name1 = 'name1'
    name2 = 'name2'

    with TestContextContainer() as context_container:
        with context_container.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                insert into app.payment_methods(profile_id, name, set_active_at, provider)
                select %(profile_id)s, name, set_active_at, %(provider)s
                from (values(%(name1)s, now() - interval '1 week'), (%(name2)s, now())) t(name, set_active_at)
                """, {
                    "profile_id": profile_id,
                    "provider": PaymentMethodProvider.STRIPE,
                    "name1": name1,
                    "name2": name2
                })

        payment_method = context_container.billing_repository.get_active_payment_method(
            profile_id)

        assert payment_method.profile_id == profile_id
        assert payment_method.name == name2
