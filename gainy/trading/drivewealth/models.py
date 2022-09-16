import datetime
from abc import ABC
import json
from typing import Any

from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion, DecimalEncoder
from gainy.trading.models import TradingAccount

PRECISION = 1e-3


class BaseDriveWealthModel(BaseModel, ABC):
    data = None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data, cls=DecimalEncoder),
        }


class DriveWealthAuthToken(BaseDriveWealthModel, ResourceVersion):
    id: int = None
    auth_token: str = None
    expires_at: datetime.datetime = None
    version: int = 0
    data: Any = None
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None

    key_fields = ["id"]
    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.DRIVEWEALTH_AUTH_TOKEN

    @property
    def resource_id(self) -> int:
        return self.id

    @property
    def resource_version(self):
        return self.version

    def update_version(self):
        self.version = self.version + 1 if self.version else 1

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_auth_tokens"

    def is_expired(self):
        if not self.expires_at:
            return True
        return self.expires_at <= datetime.datetime.now(
            tz=datetime.timezone.utc)


class DriveWealthUser(BaseDriveWealthModel):
    ref_id = None
    profile_id = None
    status = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_users"


class DriveWealthAccount(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    trading_account_id = None
    portfolio_id = None
    status = None
    ref_no = None
    nickname = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_accounts"

    def set_from_response(self, data=None):
        if not data:
            return
        self.ref_id = data["id"]
        self.status = data["status"]['name']
        self.ref_no = data["accountNo"]
        self.nickname = data["nickname"]
        self.cash_available_for_trade = data["bod"].get(
            "cashAvailableForTrading", 0)
        self.cash_available_for_withdrawal = data["bod"].get(
            "cashAvailableForWithdrawal", 0)
        self.cash_balance = data["bod"].get("cashBalance", 0)
        self.data = json.dumps(data)

    def update_trading_account(self, trading_account: TradingAccount):
        trading_account.cash_available_for_trade = self.cash_available_for_trade
        trading_account.cash_available_for_withdrawal = self.cash_available_for_withdrawal
        trading_account.cash_balance = self.cash_balance
        pass


class DriveWealthAccountMoney(BaseDriveWealthModel):
    id = None
    drivewealth_account_id = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance = None
    data = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_accounts_money"

    def set_from_response(self, data=None):
        if not data:
            return
        self.drivewealth_account_id = data["accountID"]
        self.data = json.dumps(data)

        cash = data["cash"]
        self.cash_available_for_trade = cash["cashAvailableForTrade"]
        self.cash_available_for_withdrawal = cash["cashAvailableForWithdrawal"]
        self.cash_balance = cash["cashBalance"]

    def update_trading_account(self, trading_account: TradingAccount):
        trading_account.cash_available_for_trade = self.cash_available_for_trade
        trading_account.cash_available_for_withdrawal = self.cash_available_for_withdrawal
        trading_account.cash_balance = self.cash_balance
        pass


class DriveWealthAccountPositions(BaseDriveWealthModel):
    id = None
    drivewealth_account_id = None
    equity_value = None
    data = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_accounts_positions"

    def set_from_response(self, data=None):
        if not data:
            return
        self.drivewealth_account_id = data["accountID"]
        self.equity_value = data["equityValue"]
        self.data = json.dumps(data)

    def update_trading_account(self, trading_account: TradingAccount):
        trading_account.equity_value = self.equity_value
        pass
