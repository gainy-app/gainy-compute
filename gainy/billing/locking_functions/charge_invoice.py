from gainy.billing.interfaces import BillingServiceInterface
from gainy.billing.models import Invoice
from gainy.billing.repository import BillingRepository
from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.utils import get_logger

logger = get_logger(__name__)


class ChargeInvoice(AbstractPessimisticLockingFunction):
    repo: BillingRepository
    api = None

    def __init__(self, repo: BillingRepository,
                 service: BillingServiceInterface, invoice: Invoice):
        super().__init__(repo)
        self.service = service
        self.invoice = invoice

    def execute(self, max_tries: int = 3) -> Invoice:
        return super().execute(max_tries)

    def load_version(self) -> Invoice:
        return self.repo.refresh(self.invoice)

    def _do(self, invoice: Invoice):
        if not invoice.can_charge():
            return

        self.service.charge(invoice)
