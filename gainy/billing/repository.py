import os

import datetime
import dateutil.parser
from decimal import Decimal
from typing import Iterable, Optional

from gainy.billing.exceptions import NoActivePaymentMethodException
from gainy.billing.models import Invoice, InvoiceStatus, PaymentMethod, PaymentTransaction, TransactionStatus
from gainy.data_access.operators import OperatorLt
from gainy.data_access.repository import Repository

# all yearly
BILLING_VALUE_FEE_MULTIPLIER = os.getenv("BILLING_VALUE_FEE_MULTIPLIER", 0.01)
BILLING_VALUE_FEE_MULTIPLIER = Decimal(BILLING_VALUE_FEE_MULTIPLIER)

BILLING_MIN_YEARLY_FEE = os.getenv("BILLING_MIN_YEARLY_FEE", 100)
BILLING_MIN_YEARLY_FEE = Decimal(BILLING_MIN_YEARLY_FEE)

BILLING_ENABLED_PROFILES = os.getenv("BILLING_ENABLED_PROFILES")
BILLING_ENABLED_PROFILES = BILLING_ENABLED_PROFILES.split(
    ",") if BILLING_ENABLED_PROFILES else None

BILLING_MIN_DATE = os.getenv("BILLING_MIN_DATE")
BILLING_MIN_DATE = dateutil.parser.parse(
    BILLING_MIN_DATE) if BILLING_MIN_DATE else None


class BillingRepository(Repository):

    def create_invoices(self):
        conditions = ""
        params = {
            "min_fee": BILLING_MIN_YEARLY_FEE,
            "value_fee_multiplier": BILLING_VALUE_FEE_MULTIPLIER,
        }

        if BILLING_MIN_DATE:
            params["min_period_start"] = BILLING_MIN_DATE
            conditions += " and period_start >= %(min_period_start)s"

        if BILLING_ENABLED_PROFILES:
            params["profile_ids"] = tuple(BILLING_ENABLED_PROFILES)
            conditions += " and profile_id in %(profile_ids)s"

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                f"""insert into app.invoices(profile_id, period_id, amount, due_date, description, period_start, period_end, metadata)
                    with drivewealth_monthly_usage_extended as
                             (
                                 select *,
                                        'mo_' || period_start::date::varchar as period_id
                                 from drivewealth_monthly_usage
                                 where profile_id is not null
                                   and value > 0
                                   {conditions}
                         )
                    select drivewealth_monthly_usage_extended.profile_id,
                           drivewealth_monthly_usage_extended.period_id,
                           greatest(%(min_fee)s, value * %(value_fee_multiplier)s)::numeric *
                           -- days in month
                           extract(days from
                                   drivewealth_monthly_usage_extended.period_end - drivewealth_monthly_usage_extended.period_start) /
                           -- days in year
                           extract(days from
                                   date_trunc('year', drivewealth_monthly_usage_extended.period_start) + interval '1 year' -
                                   date_trunc('year', drivewealth_monthly_usage_extended.period_start)) as amount,
                           drivewealth_monthly_usage_extended.period_end::date + interval '1 day'       as due_date,
                           'Invoice for Gainy services in ' ||
                           TO_CHAR(drivewealth_monthly_usage_extended.period_start, 'Month')            as description,
                           drivewealth_monthly_usage_extended.period_start,
                           drivewealth_monthly_usage_extended.period_end,
                           null                                                                         as metadata
                    from drivewealth_monthly_usage_extended
                             left join app.invoices using (profile_id, period_id)
                    where drivewealth_monthly_usage_extended.period_end < now()
                      and invoices.profile_id is null
                    on conflict do nothing""", params)

    def iterate_unpaid_invoices_due(self) -> Iterable[Invoice]:
        now = datetime.datetime.now()
        yield from self.iterate_all(Invoice, {
            "status": InvoiceStatus.PENDING,
            "due_date": OperatorLt(now)
        })

    def get_active_payment_method(self, profile_id) -> PaymentMethod:
        payment_method = self.find_one(PaymentMethod,
                                       {"profile_id": profile_id},
                                       [('set_active_at', 'desc')])

        if not payment_method:
            raise NoActivePaymentMethodException()

        return payment_method

    def get_pending_invoice_transaction(
            self, invoice: Invoice) -> Optional[PaymentTransaction]:
        return self.find_one(PaymentTransaction, {
            "status": TransactionStatus.PENDING,
            "invoice_id": invoice.id
        })
