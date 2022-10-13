import datetime
from decimal import Decimal
from typing import Iterable

from gainy.billing.exceptions import NoActivePaymentMethodException
from gainy.billing.models import Invoice, InvoiceStatus, PaymentMethod
from gainy.data_access.operators import OperatorLt
from gainy.data_access.repository import Repository

MONTHLY_EQUITY_VALUE_FEE_MULTIPLIER = Decimal(1) / 100 / 12
MIN_FEE = 10


class BillingRepository(Repository):

    def create_invoices(self):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """ insert into app.invoices(profile_id, period_id, amount, due_date, description, period_start, period_end, metadata)
                    with drivewealth_monthly_usage_extended as
                             (
                                 select *,
                                        'mo_' || period_start::date::varchar as period_id
                                 from drivewealth_monthly_usage
                         )
                    select drivewealth_monthly_usage_extended.profile_id,
                           drivewealth_monthly_usage_extended.period_id,
                           greatest(%(min_fee)s, equity_value * %(monthly_equity_value_fee_multiplier)s) as amount,
                           drivewealth_monthly_usage_extended.period_end::date + interval '1 day'        as due_date,
                           'Invoice for Gainy services in ' ||
                           TO_CHAR(drivewealth_monthly_usage_extended.period_start, 'Month')             as description,
                           drivewealth_monthly_usage_extended.period_start,
                           drivewealth_monthly_usage_extended.period_end,
                           null                                                                          as metadata
                    from drivewealth_monthly_usage_extended
                             left join app.invoices using (profile_id, period_id)
                    where drivewealth_monthly_usage_extended.period_end < now()
                      and invoices is null
                    on conflict do nothing""", {
                    "min_fee":
                    MIN_FEE,
                    "monthly_equity_value_fee_multiplier":
                    MONTHLY_EQUITY_VALUE_FEE_MULTIPLIER
                })

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
