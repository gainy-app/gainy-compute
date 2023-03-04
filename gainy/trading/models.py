from abc import ABC

import datetime
import enum
import json
from decimal import Decimal
from typing import Dict

from gainy.data_access.models import BaseModel, classproperty, DecimalEncoder


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


class FundingAccount(BaseModel):
    id = None
    profile_id = None
    plaid_access_token_id = None
    plaid_account_id = None
    name = None
    balance = None
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


class TradingAccount(BaseModel):
    id = None
    profile_id = None
    name = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
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


class AbstractTradingOrder(ABC, BaseModel):
    id = None
    profile_id = None
    target_amount_delta: Decimal = None
    target_amount_delta_relative: Decimal = None
    executed_amount: Decimal = None
    status: TradingOrderStatus = None
    pending_execution_since: datetime.datetime
    created_at: datetime.datetime
    executed_at: datetime.datetime


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

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if not row:
            return self

        self.source = TradingOrderSource[
            row["source"]] if row["source"] else None
        self.status = TradingOrderStatus[
            row["status"]] if row["status"] else None
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_collection_versions"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "source": self.source.name if self.source else None,
            "status": self.status.name if self.status else None,
            "weights": json.dumps(self.weights, cls=DecimalEncoder),
        }

    def set_status(self, status: TradingOrderStatus):
        self.status = status

    def is_pending(self) -> bool:
        return self.status in [
            TradingOrderStatus.PENDING, TradingOrderStatus.PENDING_EXECUTION
        ]

    def is_executed(self) -> bool:
        return self.status == TradingOrderStatus.EXECUTED_FULLY


class TradingOrder(BaseModel, AbstractTradingOrder):
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

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if not row:
            return self

        self.source = TradingOrderSource[
            row["source"]] if row["source"] else None
        self.status = TradingOrderStatus[
            row["status"]] if row["status"] else None
        return self

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_orders"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "source": self.source.name if self.source else None,
            "status": self.status.name if self.status else None,
        }

    def set_status(self, status: TradingOrderStatus):
        self.status = status

    def is_pending(self) -> bool:
        return self.status in [
            TradingOrderStatus.PENDING, TradingOrderStatus.PENDING_EXECUTION
        ]

    def is_executed(self) -> bool:
        return self.status == TradingOrderStatus.EXECUTED_FULLY


class TradingMoneyFlow(BaseModel):
    id = None
    profile_id = None
    status: TradingMoneyFlowStatus = None
    amount = None
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
