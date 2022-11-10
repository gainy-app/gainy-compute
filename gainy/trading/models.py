import datetime
import enum
import json
from decimal import Decimal
from typing import Dict

from gainy.data_access.models import BaseModel, classproperty, DecimalEncoder


class TradingMoneyFlowStatus(enum.Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TradingCollectionVersionStatus(enum.Enum):
    PENDING = "PENDING"
    PENDING_EXECUTION = "PENDING_EXECUTION"
    EXECUTED_FULLY = "EXECUTED_FULLY"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class TradingAccount(BaseModel):
    id = None
    profile_id = None
    name = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    equity_value = None
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


class TradingCollectionVersion(BaseModel):
    id = None
    profile_id = None
    collection_id = None
    status: TradingCollectionVersionStatus = None
    fail_reason: str = None
    target_amount_delta = None
    weights: Dict[str, Decimal] = None
    created_at = None
    executed_at = None
    updated_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, row=None):
        super().__init__(row)

        if not row:
            return

        self.status = TradingCollectionVersionStatus[
            row["status"]] if row["status"] else None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "trading_collection_versions"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "status": self.status.name if self.status else None,
            "weights": json.dumps(self.weights, cls=DecimalEncoder),
        }

    def set_status(self, status: TradingCollectionVersionStatus):
        self.status = status
        if status == TradingCollectionVersionStatus.EXECUTED_FULLY:
            # TODO set from actual autopilot execution data
            self.executed_at = datetime.datetime.now()

    def is_pending(self) -> bool:
        return self.status in [
            TradingCollectionVersionStatus.PENDING,
            TradingCollectionVersionStatus.PENDING_EXECUTION
        ]

    def is_executed(self) -> bool:
        return self.status == TradingCollectionVersionStatus.EXECUTED_FULLY
