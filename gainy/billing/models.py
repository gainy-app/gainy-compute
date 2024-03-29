import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from gainy.billing.exceptions import InvoiceSealedException
from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion


class InvoiceStatus(str, Enum):
    PENDING = 'PENDING'
    PAID = 'PAID'


class PaymentMethodProvider(str, Enum):
    STRIPE = 'STRIPE'
    DRIVEWEALTH = 'DRIVEWEALTH'


class PaymentTransactionStatus(str, Enum):
    PENDING = 'PENDING'
    REQUIRES_AUTHENTICATION = 'REQUIRES_AUTHENTICATION'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


class PaymentTransaction(BaseModel):
    id: int = None
    profile_id: int = None
    invoice_id: int = None
    payment_method_id: int = None
    status: PaymentTransactionStatus = None
    metadata: Any = None
    created_at: datetime.datetime = None

    key_fields = ["id"]
    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["status"]:
            self.status = PaymentTransactionStatus(row["status"])
        else:
            self.status = PaymentTransactionStatus.PENDING

        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "payment_transactions"


class Invoice(BaseModel, ResourceVersion):
    id: int = None
    profile_id: int = None
    period_id: str = None
    status: InvoiceStatus = InvoiceStatus.PENDING
    amount: Decimal = None
    due_date: datetime.datetime = None
    description: str = None
    period_start: datetime.datetime = None
    period_end: datetime.datetime = None
    metadata: Any
    version: int = 0
    created_at: datetime.datetime = None

    key_fields = ["id"]
    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["status"]:
            self.status = InvoiceStatus(row["status"])

        if row and row["amount"]:
            self.amount = Decimal(row["amount"])
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "invoices"

    def can_charge(self):
        return self.status == InvoiceStatus.PENDING

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.INVOICE

    @property
    def resource_id(self) -> int:
        return self.id

    @property
    def resource_version(self):
        return self.version

    def update_version(self):
        self.version = self.version + 1 if self.version else 1

    def on_new_transaction(self, transaction: PaymentTransaction):
        if transaction.status == PaymentTransactionStatus.PENDING:
            return

        if transaction.status == PaymentTransactionStatus.SUCCESS and self.status == InvoiceStatus.PAID:
            return

        if self.status == InvoiceStatus.PENDING:
            if transaction.status == PaymentTransactionStatus.SUCCESS:
                self.status = InvoiceStatus.PAID
        else:
            raise InvoiceSealedException()


class PaymentMethod(BaseModel):
    id: int = None
    profile_id: int = None
    name: datetime.datetime = None
    set_active_at: datetime.datetime = None
    provider: str = None
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None

    key_fields = ["id"]
    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["provider"]:
            self.provider = PaymentMethodProvider(row["provider"])
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "payment_methods"
