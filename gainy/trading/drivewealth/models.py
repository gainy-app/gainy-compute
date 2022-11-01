import dateutil.parser
import datetime
from abc import ABC
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional

from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion, DecimalEncoder
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)

PRECISION = 1e-3
ONE = Decimal(1)
ZERO = Decimal(0)
DW_WEIGHT_PRECISION = 4


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

    def set_from_response(self, data):
        self.ref_id = data["id"]
        self.status = data["status"]["name"]
        self.data = data


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
    is_artificial = False
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
        self.drivewealth_user_id = data["userID"]
        self.status = data["status"]['name']
        self.ref_no = data["accountNo"]
        self.nickname = data["nickname"]
        self.cash_available_for_trade = data["bod"].get(
            "cashAvailableForTrading", 0)
        self.cash_available_for_withdrawal = data["bod"].get(
            "cashAvailableForWithdrawal", 0)
        self.cash_balance = data["bod"].get("cashBalance", 0)
        self.data = data

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
        self.data = data

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
        self.data = data

    def update_trading_account(self, trading_account: TradingAccount):
        trading_account.equity_value = self.equity_value
        pass


class CollectionHoldingStatus:
    symbol = None
    target_weight = None
    actual_weight = None
    value = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "target_weight": self.target_weight,
            "actual_weight": self.actual_weight,
            "value": self.value,
        }


class CollectionStatus:
    holdings: List[CollectionHoldingStatus] = None
    value = None

    def to_dict(self) -> dict:
        return {
            "value": self.value,
            "holdings": self.holdings,
        }


class DriveWealthPortfolioStatusFundHolding:

    def __init__(self, data):
        self.data = data

    def get_collection_holding_status(self) -> CollectionHoldingStatus:
        entity = CollectionHoldingStatus()
        entity.symbol = self.data["symbol"]
        entity.target_weight = self.data["target"]
        entity.actual_weight = self.data["actual"]
        entity.value = self.data["value"]
        return entity


class DriveWealthPortfolioStatusHolding:

    def __init__(self, data):
        self.data = data

    @property
    def value(self) -> str:
        return self.data["value"]

    @property
    def actual_weight(self) -> str:
        return self.data["actual"]

    @property
    def target_weight(self) -> str:
        return self.data["target"]

    @property
    def holdings(self) -> List[DriveWealthPortfolioStatusFundHolding]:
        return [
            DriveWealthPortfolioStatusFundHolding(i)
            for i in self.data["holdings"]
        ]

    def get_collection_status(self) -> CollectionStatus:
        entity = CollectionStatus()
        entity.value = self.data["value"]
        entity.holdings = [
            i.get_collection_holding_status() for i in self.holdings
        ]
        return entity


class DriveWealthPortfolioStatus(BaseDriveWealthModel):
    id = None
    drivewealth_portfolio_id = None
    equity_value: Decimal = None
    cash_value: Decimal = None
    cash_actual_weight: Decimal = None
    cash_target_weight: Decimal = None
    last_portfolio_rebalance_at: datetime.datetime = None
    next_portfolio_rebalance_at: datetime.datetime = None
    holdings: Dict[str, DriveWealthPortfolioStatusHolding] = None
    data = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "holdings"]
    non_persistent_fields = ["id", "created_at"]

    def __init__(self, row=None):
        super().__init__(row)
        self._reset_holdings()

    def set_from_response(self, data=None):
        self.data = data
        self._reset_holdings()

        if not data:
            return

        self.drivewealth_portfolio_id = data["id"]

    def _reset_holdings(self):
        self.holdings = {}
        if not self.data:
            return

        if self.data["lastPortfolioRebalance"]:
            self.last_portfolio_rebalance_at = dateutil.parser.parse(
                self.data["lastPortfolioRebalance"])
        if self.data["nextPortfolioRebalance"]:
            self.next_portfolio_rebalance_at = dateutil.parser.parse(
                self.data["nextPortfolioRebalance"])
        self.equity_value = Decimal(self.data["equity"])

        for i in self.data["holdings"]:
            if i["type"] == "CASH_RESERVE":
                self.cash_value = Decimal(i["value"])
                self.cash_actual_weight = Decimal(i["actual"])
                self.cash_target_weight = Decimal(i["target"])
            else:
                self.holdings[i["id"]] = DriveWealthPortfolioStatusHolding(i)

    def get_fund_value(self, fund_ref_id) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return Decimal(0)

        return Decimal(self.holdings[fund_ref_id].value)

    def get_fund_actual_weight(self, fund_ref_id) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return Decimal(0)

        return Decimal(self.holdings[fund_ref_id].actual_weight)

    def get_fund(self,
                 fund_ref_id) -> Optional[DriveWealthPortfolioStatusHolding]:
        if not self.holdings or fund_ref_id not in self.holdings:
            return None

        return self.holdings[fund_ref_id]

    def get_fund_ref_ids(self) -> list:
        if not self.holdings:
            return []

        return list(self.holdings.keys())

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_portfolio_statuses"

    def to_dict(self) -> dict:
        holdings = {k: i.data for k, i in self.holdings.items()}
        return {
            **super().to_dict(),
            "holdings":
            json.dumps(holdings, cls=DecimalEncoder),
        }


class DriveWealthFund(BaseDriveWealthModel):
    ref_id = None
    profile_id = None
    collection_id = None
    trading_collection_version_id = None
    weights: Dict[str, Decimal] = None
    holdings = []
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data["id"]
        self.data = data
        self.holdings = self.data["holdings"]

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "weights":
            json.dumps(self.weights, cls=DecimalEncoder),
            "holdings":
            json.dumps(self.holdings, cls=DecimalEncoder),
        }

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_funds"


class DriveWealthPortfolio(BaseDriveWealthModel):
    ref_id = None
    profile_id = None
    drivewealth_account_id = None
    cash_target_weight: Decimal = None
    holdings: Dict[str, Decimal] = None
    data = None
    is_artificial = False
    waiting_rebalance_since: Optional[datetime.datetime] = None
    last_rebalance_at: Optional[datetime.datetime] = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data["id"]
        self.data = data

        self.cash_target_weight = Decimal(1)
        self.holdings = {}
        for i in data["holdings"]:
            if i["type"] == "CASH_RESERVE":
                self.cash_target_weight = Decimal(i["target"])
            else:
                fund_id = i.get("id") or i.get("instrumentID")
                self.holdings[fund_id] = Decimal(i["target"])

    def set_target_weights_from_status_actual_weights(
            self, portfolio_status: DriveWealthPortfolioStatus):
        self.cash_target_weight = portfolio_status.cash_actual_weight

        for fund_ref_id, i in self.holdings.items():
            self.holdings[fund_ref_id] = ZERO

        for fund_ref_id in portfolio_status.get_fund_ref_ids():
            self.holdings[
                fund_ref_id] = portfolio_status.get_fund_actual_weight(
                    fund_ref_id)

    def move_cash_to_fund(self, fund: DriveWealthFund, weight_delta: Decimal):
        cash_weight = self.cash_target_weight
        if cash_weight - weight_delta < -PRECISION:
            raise Exception('cash weight can not be negative')
        if cash_weight - weight_delta > 1 + PRECISION:
            raise Exception('cash weight can not be greater than 1')

        fund_weight = self.get_fund_weight(fund.ref_id)
        if fund_weight + weight_delta < -PRECISION:
            raise Exception('fund weight can not be negative')
        if fund_weight + weight_delta > 1 + PRECISION:
            raise Exception('fund weight can not be greater than 1')

        cash_weight -= weight_delta
        self.cash_target_weight = min(ONE, max(ZERO, cash_weight))

        fund_weight += weight_delta
        self.set_fund_weight(fund.ref_id, min(ONE, max(ZERO, fund_weight)))
        self.normalize_weights()

    def normalize_weights(self):
        self.cash_target_weight = round(self.cash_target_weight,
                                        DW_WEIGHT_PRECISION)
        weight_sum = Decimal(self.cash_target_weight)
        for k, i in self.holdings.items():
            self.holdings[k] = round(i, DW_WEIGHT_PRECISION)
            weight_sum += i

        logger.info('normalize_weights pre',
                    extra={
                        "weight_sum": weight_sum,
                        "cash_target_weight": self.cash_target_weight,
                        "holdings": self.holdings.values(),
                    })

        weight_threshold = Decimal(10)**(-DW_WEIGHT_PRECISION)
        self.cash_target_weight = round(self.cash_target_weight / weight_sum,
                                        DW_WEIGHT_PRECISION)
        holdings_to_delete = []
        for k, i in self.holdings.items():
            weight = round(i / weight_sum, DW_WEIGHT_PRECISION)
            if weight < weight_threshold:
                holdings_to_delete.append(k)
            else:
                self.holdings[k] = round(i / weight_sum, DW_WEIGHT_PRECISION)

        for k in holdings_to_delete:
            del self.holdings[k]

        weight_sum = Decimal(self.cash_target_weight)
        for k, i in self.holdings.items():
            weight_sum += i

        self.cash_target_weight += 1 - weight_sum
        weight_sum += 1 - weight_sum

        logger.info('normalize_weights post',
                    extra={
                        "weight_threshold": weight_threshold,
                        "weight_sum": weight_sum,
                        "cash_target_weight": self.cash_target_weight,
                        "holdings": self.holdings.values(),
                    })

    def get_fund_weight(self, fund_ref_id: str) -> Decimal:
        if not self.holdings or fund_ref_id not in self.holdings:
            return ZERO

        return self.holdings[fund_ref_id]

    def set_fund_weight(self, fund_ref_id: str, weight: Decimal):
        if not self.holdings:
            self.holdings = {}

        self.holdings[fund_ref_id] = weight

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "holdings":
            json.dumps(self.holdings, cls=DecimalEncoder),
        }

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_portfolios"

    def set_pending_rebalance(self):
        self.waiting_rebalance_since = datetime.datetime.now(
            tz=datetime.timezone.utc)

    def is_pending_rebalance(self) -> bool:
        if self.waiting_rebalance_since is None:
            return False
        if self.last_rebalance_at is None:
            return True

        return self.waiting_rebalance_since > self.last_rebalance_at

    def update_from_status(self, portfolio_status: DriveWealthPortfolioStatus):
        if self.last_rebalance_at:
            self.last_rebalance_at = max(
                self.last_rebalance_at,
                portfolio_status.last_portfolio_rebalance_at)
        else:
            self.last_rebalance_at = portfolio_status.last_portfolio_rebalance_at
