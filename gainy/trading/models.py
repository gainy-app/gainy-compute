import abc
from abc import ABC

import datetime
import enum
import json
from decimal import Decimal
from typing import Dict

from gainy.data_access.models import BaseModel, classproperty, DecimalEncoder


class KycStatus(str, enum.Enum):
    NOT_READY = "NOT_READY"
    READY = "READY"
    PROCESSING = "PROCESSING"
    APPROVED = "APPROVED"
    INFO_REQUIRED = "INFO_REQUIRED"
    DOC_REQUIRED = "DOC_REQUIRED"
    MANUAL_REVIEW = "MANUAL_REVIEW"
    DENIED = "DENIED"


class TradingMoneyFlowStatus(enum.Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TradingOrderStatus(enum.Enum):
    PENDING = "PENDING"
    PENDING_EXECUTION = "PENDING_EXECUTION"
    EXECUTED_FULLY = "EXECUTED_FULLY"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class TradingOrderSource(enum.Enum):
    MANUAL = "MANUAL"
    AUTOMATIC = "AUTOMATIC"


class TradingStatementType(str, enum.Enum):
    MONTHLY_STATEMENT = "MONTHLY_STATEMENT"
    TAX = "TAX"
    TRADE_CONFIRMATION = "TRADE_CONFIRMATION"


class FundingAccount(BaseModel):
    id = None
    profile_id = None
    plaid_access_token_id = None
    plaid_account_id = None
    name = None
    balance = None
    mask: str = None
    needs_reauth: bool = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_funding_accounts"


class AbstractProviderBankAccount(BaseModel):

    @abc.abstractmethod
    def fill_funding_account_details(self, funding_account: FundingAccount):
        pass


class TradingAccount(BaseModel):
    id = None
    profile_id = None
    name = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance: float = None
    equity_value = None
    account_no = None
    is_artificial: bool = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_accounts"


class AbstractTradingOrder(BaseModel, ABC):
    id = None
    profile_id = None
    target_amount_delta: Decimal = None
    target_amount_delta_relative: Decimal = None
    executed_amount: Decimal = None
    status: TradingOrderStatus = None
    pending_execution_since: datetime.datetime
    source: TradingOrderSource = None
    created_at: datetime.datetime
    executed_at: datetime.datetime

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if not row:
            return self

        self.source = TradingOrderSource[
            row["source"]] if row["source"] else None
        self.status = TradingOrderStatus[
            row["status"]] if row["status"] else None
        return self

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "source": self.source.name if self.source else None,
            "status": self.status.name if self.status else None,
        }

    def is_pending(self) -> bool:
        return self.status in [
            TradingOrderStatus.PENDING, TradingOrderStatus.PENDING_EXECUTION
        ]

    def is_executed(self) -> bool:
        return self.status == TradingOrderStatus.EXECUTED_FULLY


class TradingCollectionVersion(AbstractTradingOrder):
    id = None
    profile_id = None
    collection_id = None
    source: TradingOrderSource = None
    status: TradingOrderStatus = None
    fail_reason: str = None
    target_amount_delta: Decimal = None
    target_amount_delta_relative: Decimal = None
    weights: Dict[str, Decimal] = None
    trading_account_id: int = None
    pending_execution_since = None
    last_optimization_at: datetime.date = None
    executed_amount: Decimal = None
    executed_at = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_collection_versions"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "weights":
            json.dumps(self.weights, cls=DecimalEncoder),
        }


class TradingOrder(AbstractTradingOrder):
    id = None
    profile_id = None
    symbol = None
    status: TradingOrderStatus = None
    target_amount_delta: Decimal = None
    target_amount_delta_relative: Decimal = None
    trading_account_id: int = None
    source: TradingOrderSource = None
    fail_reason: str = None
    pending_execution_since = None
    executed_amount: Decimal = None
    executed_at = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_orders"


class TradingMoneyFlow(BaseModel):
    id = None
    profile_id = None
    status: TradingMoneyFlowStatus = None
    amount: Decimal = None
    trading_account_id = None
    funding_account_id = None
    fees_total_amount: Decimal = None
    created_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if not row:
            return self

        self.status = TradingMoneyFlowStatus[
            row["status"]] if row["status"] else None
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_money_flow"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "status":
            self.status.name if self.status else None,
        }


class KycForm(BaseModel):
    profile_id = None
    first_name = None
    last_name = None
    country = None
    phone_number = None
    email_address = None
    language = None
    employment_status = None
    employment_company_name = None
    employment_type = None
    employment_position = None
    employment_affiliated_with_a_broker = None
    employment_is_director_of_a_public_company = None
    investor_profile_experience = None
    investor_profile_annual_income = None
    investor_profile_net_worth_total = None
    investor_profile_risk_tolerance = None
    investor_profile_objectives = None
    investor_profile_net_worth_liquid = None
    disclosures_drivewealth_terms_of_use = None
    disclosures_drivewealth_customer_agreement = None
    disclosures_drivewealth_ira_agreement = None
    disclosures_drivewealth_market_data_agreement = None
    disclosures_rule14b = None
    disclosures_drivewealth_privacy_policy = None
    disclosures_drivewealth_data_sharing = None
    disclosures_signed_by = None
    disclosures_extended_hours_agreement = None
    tax_id_value = None
    tax_id_type = None
    citizenship = None
    is_us_tax_payer = None
    tax_treaty_with_us = None
    birthdate = None
    politically_exposed_names = None
    irs_backup_withholdings_notified = None
    gender = None
    marital_status = None
    address_street1 = None
    address_street2 = None
    address_city = None
    address_province = None
    address_postal_code = None
    address_country = None
    status = None
    created_at = None
    updated_at = None

    key_fields = ["profile_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_form"


class TradingStatement(BaseModel):
    id: int = None
    profile_id: int = None
    type: TradingStatementType = None
    display_name: str = None
    date: datetime.date = None
    created_at: datetime.datetime = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["type"]:
            self.type = TradingStatementType(row["type"])
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_statements"


class ProfileKycStatus(BaseModel):
    id: int = None
    profile_id: int = None
    status: KycStatus = None
    message: str = None
    error_messages: list[str] = None
    created_at: datetime.datetime = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["status"]:
            self.status = KycStatus(row["status"])
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "kyc_statuses"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(), "error_messages":
            json.dumps(self.error_messages)
        }
