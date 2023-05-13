import abc
import enum
import re

import dateutil.parser
import datetime
from abc import ABC
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional, Iterable

import pytz

from gainy.billing.models import PaymentTransaction, PaymentTransactionStatus
from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion, DecimalEncoder
from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.provider.misc import normalize_symbol
from gainy.trading.models import TradingAccount, TradingMoneyFlowStatus, AbstractProviderBankAccount, FundingAccount, \
    ProfileKycStatus, KycStatus, TradingStatementType, KycErrorCode, CorporateActionAdjustment
from gainy.utils import get_logger

logger = get_logger(__name__)

EXECUTED_AMOUNT_PRECISION = Decimal(1)
PRECISION = Decimal(10)**-3
ONE = Decimal(1)
ZERO = Decimal(0)
DW_WEIGHT_PRECISION = 4
DW_WEIGHT_THRESHOLD = Decimal(10)**(-DW_WEIGHT_PRECISION)

DW_ERRORS_MAPPING = [
    {
        "name": "AGE_VALIDATION",
        "gainy_code": KycErrorCode.AGE_VALIDATION,
        "code": "K001",
    },
    {
        "name": "POOR_PHOTO_QUALITY",
        "gainy_code": KycErrorCode.POOR_PHOTO_QUALITY,
        "code": "K002",
    },
    {
        "name": "POOR_DOC_QUALITY",
        "gainy_code": KycErrorCode.POOR_DOC_QUALITY,
        "code": "K003",
    },
    {
        "name": "SUSPECTED_DOCUMENT_FRAUD",
        "gainy_code": KycErrorCode.SUSPECTED_DOCUMENT_FRAUD,
        "code": "K004",
    },
    {
        "name": "INCORRECT_SIDE",
        "gainy_code": KycErrorCode.INCORRECT_SIDE,
        "code": "K005",
    },
    {
        "name": "NO_DOC_IN_IMAGE",
        "gainy_code": KycErrorCode.NO_DOC_IN_IMAGE,
        "code": "K006",
    },
    {
        "name": "TWO_DOCS_UPLOADED",
        "gainy_code": KycErrorCode.TWO_DOCS_UPLOADED,
        "code": "K007",
    },
    {
        "name": "EXPIRED_DOCUMENT",
        "gainy_code": KycErrorCode.EXPIRED_DOCUMENT,
        "code": "K008",
    },
    {
        "name": "MISSING_BACK",
        "gainy_code": KycErrorCode.MISSING_BACK,
        "code": "K009",
    },
    {
        "name": "UNSUPPORTED_DOCUMENT",
        "gainy_code": KycErrorCode.UNSUPPORTED_DOCUMENT,
        "code": "K010",
    },
    {
        "name": "DOB_NOT_MATCH_ON_DOC",
        "gainy_code": KycErrorCode.DOB_NOT_MATCH_ON_DOC,
        "code": "K011",
    },
    {
        "name": "NAME_NOT_MATCH_ON_DOC",
        "gainy_code": KycErrorCode.NAME_NOT_MATCH_ON_DOC,
        "code": "K012",
    },
    {
        "name": "INVALID_DOCUMENT",
        "gainy_code": KycErrorCode.INVALID_DOCUMENT,
        "code": "K050",
    },
    {
        "name": "ADDRESS_NOT_MATCH",
        "gainy_code": KycErrorCode.ADDRESS_NOT_MATCH,
        "code": "K101",
    },
    {
        "name": "SSN_NOT_MATCH",
        "gainy_code": KycErrorCode.SSN_NOT_MATCH,
        "code": "K102",
    },
    {
        "name": "DOB_NOT_MATCH",
        "gainy_code": KycErrorCode.DOB_NOT_MATCH,
        "code": "K103",
    },
    {
        "name": "NAME_NOT_MATCH",
        "gainy_code": KycErrorCode.NAME_NOT_MATCH,
        "code": "K104",
    },
    {
        "name": "SANCTION_WATCHLIST",
        "gainy_code": KycErrorCode.SANCTION_WATCHLIST,
        "code": "K106",
    },
    {
        "name": "SANCTION_OFAC",
        "gainy_code": KycErrorCode.SANCTION_OFAC,
        "code": "K107",
    },
    {
        "name": "INVALID_PHONE_NUMBER",
        "gainy_code": KycErrorCode.INVALID_PHONE_NUMBER,
        "code": "K108",
    },
    {
        "name": "INVALID_EMAIL_ADDRESS",
        "gainy_code": KycErrorCode.INVALID_EMAIL_ADDRESS,
        "code": "K109",
    },
    {
        "name": "INVALID_NAME_TOO_LONG",
        "gainy_code": KycErrorCode.INVALID_NAME_TOO_LONG,
        "code": "K110",
    },
    {
        "name": "UNSUPPORTED_COUNTRY",
        "gainy_code": KycErrorCode.UNSUPPORTED_COUNTRY,
        "code": "K111",
    },
    {
        "name": "AGED_ACCOUNT",
        "gainy_code": KycErrorCode.AGED_ACCOUNT,
        "code": "K801",
    },
    {
        "name": "ACCOUNT_INTEGRITY",
        "gainy_code": KycErrorCode.ACCOUNT_INTEGRITY,
        "code": "K802",
    },
    {
        "name": "UNKNOWN",
        "code": "U999",
    },
]


class DriveWealthRedemptionStatus(str, enum.Enum):
    RIA_Pending = 'RIA_Pending'
    RIA_Approved = 'RIA_Approved'
    Approved = 'Approved'
    Successful = 'Successful'


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


class DriveWealthAccountStatus(str, enum.Enum):
    OPEN = "OPEN"
    OPEN_NO_NEW_TRADES = "OPEN_NO_NEW_TRADES"


class DriveWealthAccount(BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    trading_account_id = None
    payment_method_id = None
    status = None
    ref_no = None
    nickname = None
    cash_available_for_trade = None
    cash_available_for_withdrawal = None
    cash_balance: float = None
    data = None
    is_artificial = False
    created_at: datetime.datetime = None
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
        trading_account.account_no = self.ref_no
        pass

    def is_open(self):
        return self.status == DriveWealthAccountStatus.OPEN


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

    def get_symbol_market_price(self, symbol: str) -> Decimal:
        """
        :raises EntityNotFoundException:
        """
        try:
            position = next(
                filter(lambda x: x["symbol"] == symbol,
                       self.data["equityPositions"]))
            return Decimal(position["mktPrice"])
        except StopIteration as e:
            raise EntityNotFoundException(
                "drivewealth_accounts_positions position") from e


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

    @property
    def symbol(self) -> str:
        return self.data["symbol"]

    @property
    def instrument_id(self) -> str:
        return self.data["instrumentID"]

    @property
    def value(self) -> Decimal:
        return Decimal(self.data["value"])

    @property
    def actual_weight(self) -> Decimal:
        return Decimal(self.data["actual"])

    @property
    def target_weight(self) -> Decimal:
        return Decimal(self.data["target"])

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
    def value(self) -> Decimal:
        return Decimal(self.data["value"])

    @property
    def actual_weight(self) -> Decimal:
        return Decimal(self.data["actual"])

    @property
    def target_weight(self) -> Decimal:
        return Decimal(self.data["target"])

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

    def is_valid(self) -> bool:
        logger_extra = {"holding": self.data}
        value_sum = Decimal(0)
        for holding in self.holdings:
            value_sum += holding.value

        diff = abs(value_sum - self.value)
        if diff > 1:
            logger.info(f'is_valid: value_sum is invalid, diff: %.6f',
                        diff,
                        extra=logger_extra)
            return False

        return self.is_valid_weights()

    def is_valid_weights(self) -> bool:
        logger_extra = {"holding": self.data}
        try:
            weight_sum = sum(holding.actual_weight
                             for holding in self.holdings)
        except StopIteration:
            # empty holdings: let's say it's correct
            return True

        diff = abs(weight_sum - 1)
        if diff > 2e-3 and self.value > 0:
            logger.info(f'is_valid: weight_sum is invalid, diff: %.6f',
                        diff,
                        extra=logger_extra)
            return False

        return True


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
    date: datetime.date = None
    data = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "holdings"]
    non_persistent_fields = ["id", "created_at"]

    def __init__(self):
        self._reset_holdings()

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)
        self._reset_holdings()
        return self

    def set_from_response(self, data=None):
        self.data = data
        self._reset_holdings()
        self.date = datetime.datetime.now(
            pytz.timezone('America/New_York')).date()

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
        self.cash_value = Decimal(0)
        self.cash_actual_weight = Decimal(0)
        self.cash_target_weight = Decimal(0)

        for i in self.data["holdings"]:
            if i["type"] == "CASH_RESERVE":
                self.cash_value = Decimal(i["value"])
                self.cash_actual_weight = Decimal(i["actual"])
                self.cash_target_weight = Decimal(i["target"])
            else:
                self.holdings[i["id"]] = DriveWealthPortfolioStatusHolding(i)

    def is_valid(self) -> bool:
        logger_extra = {"portfolio_status": self.data}
        for holding_id, holding in self.holdings.items():
            if holding.is_valid():
                continue

            logger.info(f'is_valid: holding {holding_id} is invalid',
                        extra=logger_extra)
            return False

        return self.is_valid_weights()

    def is_valid_weights(self) -> bool:
        logger_extra = {"portfolio_status": self.data}
        weight_sum = Decimal(self.cash_actual_weight)
        for holding_id, holding in self.holdings.items():
            if not holding.is_valid_weights():
                logger.info(f'is_valid: holding {holding_id} is invalid',
                            extra=logger_extra)
                return False
            weight_sum += holding.actual_weight

        equity_value = self.equity_value

        diff = abs(weight_sum - 1)
        if diff > 2e-3 and equity_value > 0:
            logger.info(f'is_valid: weight_sum is invalid, diff: %6.f',
                        diff,
                        extra=logger_extra)
            return False

        return True

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

    def is_pending_rebalance(self):
        if not self.data:
            return False
        if self.data.get("rebalanceRequired"):
            return True

        next_portfolio_rebalance = self.data.get("nextPortfolioRebalance")
        last_portfolio_rebalance = self.data.get("lastPortfolioRebalance")
        return next_portfolio_rebalance and last_portfolio_rebalance and next_portfolio_rebalance > last_portfolio_rebalance


class DriveWealthPortfolioHolding(BaseModel):
    portfolio_status_id = None
    profile_id: int = None
    holding_id_v2: str = None
    actual_value: Decimal = None
    quantity: Decimal = None
    symbol: str = None
    collection_uniq_id: str = None
    collection_id: int = None
    updated_at: datetime.datetime = None

    key_fields = ["holding_id_v2"]

    db_excluded_fields = ["updated_at"]
    non_persistent_fields = ["updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_portfolio_holdings"


class DriveWealthFund(BaseDriveWealthModel):
    ref_id = None
    profile_id = None
    collection_id = None
    symbol = None
    trading_collection_version_id = None
    trading_order_id = None
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

    def normalize_weights(self):
        weight_sum = Decimal(0)
        for k, i in enumerate(self.holdings):
            new_target = round(Decimal(i['target']), DW_WEIGHT_PRECISION)
            self.holdings[k]['target'] = new_target
            weight_sum += new_target

        logger.info('DriveWealthFund normalize_weights pre',
                    extra={
                        "weight_sum": weight_sum,
                        "holdings": self.holdings,
                    })

        if weight_sum < DW_WEIGHT_THRESHOLD:
            return

        for k, i in enumerate(self.holdings):
            new_target = round(i['target'] / weight_sum, DW_WEIGHT_PRECISION)
            self.holdings[k]['target'] = new_target
        self.holdings = list(
            filter(lambda x: x['target'] >= DW_WEIGHT_THRESHOLD,
                   self.holdings))

        weight_sum = Decimal(0)
        for i in self.holdings:
            weight_sum += i['target']

        if self.holdings:
            self.holdings[0]['target'] += 1 - weight_sum
            weight_sum += 1 - weight_sum

        logger.info('DriveWealthFund normalize_weights post',
                    extra={
                        "weight_threshold": DW_WEIGHT_THRESHOLD,
                        "weight_sum": weight_sum,
                        "holdings": self.holdings,
                    })

    def set_target_weights_from_status_actual_weights(
            self, portfolio_status: DriveWealthPortfolioStatus):
        holdings = portfolio_status.get_fund(self.ref_id)
        if not holdings:
            raise Exception("Fund not found in portfolio status.")
        fund_holdings = holdings.holdings
        if not fund_holdings:
            raise Exception("Fund not found in portfolio status.")

        self.holdings = []
        self.weights = {}

        for holding in fund_holdings:
            self.weights[holding.symbol] = holding.actual_weight
            self.holdings.append({
                "instrumentID": holding.instrument_id,
                "target": holding.actual_weight
            })

    def has_valid_weights(self) -> bool:
        return any(filter(lambda x: Decimal(x) > ZERO, self.weights.values()))

    def get_instrument_ids(self) -> list[str]:
        return [i["instrumentID"] for i in self.holdings]

    def remove_instrument_ids(self, ref_ids: Iterable[str]):
        self.holdings = [
            i for i in self.holdings if i["instrumentID"] not in ref_ids
        ]


class DriveWealthPortfolio(BaseDriveWealthModel):
    ref_id = None
    profile_id = None
    drivewealth_account_id = None
    cash_target_weight: Decimal = None
    cash_target_value: Decimal = None  # deprecated
    holdings: Dict[str, Decimal] = None
    data = None
    is_artificial = False
    waiting_rebalance_since: Optional[datetime.datetime] = None
    last_rebalance_at: Optional[datetime.datetime] = None
    last_order_executed_at: Optional[datetime.datetime] = None
    last_sync_at: Optional[datetime.datetime] = None
    last_transaction_id: int = None
    pending_redemptions_amount_sum: Decimal = None
    last_equity_value: Decimal = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if not self.holdings:
            self.holdings = {}

        for k, i in self.holdings.items():
            self.holdings[k] = Decimal(i)
        return self

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data["id"]
        self.data = data

        self.cash_target_weight = Decimal(0)
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

        if not self.holdings:
            self.holdings = {}

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

    def rebalance_cash(self, weight_delta: Decimal):
        old_cash_weight = cash_weight = self.cash_target_weight
        funds_weight_sum = 1 - self.cash_target_weight

        if funds_weight_sum < PRECISION:
            return

        if cash_weight + weight_delta < -PRECISION:
            raise Exception('cash weight can not be negative')
        if cash_weight + weight_delta > 1 + PRECISION:
            raise Exception('cash weight can not be greater than 1')

        logger.debug('rebalance_cash weight_delta=%f', weight_delta)
        cash_weight += weight_delta
        self.cash_target_weight = min(ONE, max(ZERO, cash_weight))
        logger.debug('Change cash weight from %f to %f', old_cash_weight,
                     self.cash_target_weight)

        for fund_ref_id in self.holdings.keys():
            old_fund_weight = fund_weight = self.get_fund_weight(fund_ref_id)
            fund_weight_delta = weight_delta * fund_weight / funds_weight_sum
            logger.debug('fund %s ',
                         fund_ref_id,
                         extra={
                             "fund_weight_delta": fund_weight_delta,
                             "weight_delta": weight_delta,
                             "fund_weight": fund_weight,
                             "funds_weight_sum": funds_weight_sum,
                         })

            if fund_weight - fund_weight_delta < -PRECISION:
                raise Exception('fund weight can not be negative')
            if fund_weight - fund_weight_delta > 1 + PRECISION:
                raise Exception('fund weight can not be greater than 1')

            fund_weight -= fund_weight_delta
            fund_weight = min(ONE, max(ZERO, fund_weight))
            logger.debug('Set fund %s weight from %f to %f', fund_ref_id,
                         old_fund_weight, fund_weight)

            self.set_fund_weight(fund_ref_id, fund_weight)

    def normalize_weights(self):
        self.cash_target_weight = round(self.cash_target_weight,
                                        DW_WEIGHT_PRECISION)
        weight_sum = Decimal(self.cash_target_weight)
        for k, i in self.holdings.items():
            self.holdings[k] = round(i, DW_WEIGHT_PRECISION)
            weight_sum += i

        if weight_sum < DW_WEIGHT_THRESHOLD:
            return

        logger.info('DriveWealthPortfolio normalize_weights pre',
                    extra={
                        "weight_sum": weight_sum,
                        "cash_target_weight": self.cash_target_weight,
                        "holdings": self.holdings.values(),
                    })

        self.cash_target_weight = round(self.cash_target_weight / weight_sum,
                                        DW_WEIGHT_PRECISION)
        holdings_to_delete = []
        for k, i in self.holdings.items():
            weight = round(i / weight_sum, DW_WEIGHT_PRECISION)
            if weight < DW_WEIGHT_THRESHOLD:
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

        logger.info('DriveWealthPortfolio normalize_weights post',
                    extra={
                        "weight_threshold": DW_WEIGHT_THRESHOLD,
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

    def update_from_status(self, portfolio_status: DriveWealthPortfolioStatus):
        if self.last_rebalance_at:
            self.last_rebalance_at = max(
                self.last_rebalance_at,
                portfolio_status.last_portfolio_rebalance_at)
        else:
            self.last_rebalance_at = portfolio_status.last_portfolio_rebalance_at

    def get_fund_ref_ids(self) -> list:
        if not self.holdings:
            return []

        return list(self.holdings.keys())


class DriveWealthInstrumentStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"


class DriveWealthInstrument(BaseDriveWealthModel):
    ref_id = None
    symbol = None
    status = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data.get("id") or data.get("instrumentID")
        self.symbol = data["symbol"]
        self.status = data["status"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_instruments"


class DriveWealthCountry(BaseDriveWealthModel):
    ref_id = None
    code2 = None
    code3 = None
    name = None
    active = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return

        self.ref_id = data.get("id")
        self.code2 = data["code2"]
        self.code3 = data["code3"]
        self.name = data["name"]
        self.active = data["active"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_countries"


class DriveWealthTransactionInterface(abc.ABC):
    id: int = None
    account_id: str = None
    type: str = None
    data: dict = None

    @classmethod
    @abc.abstractmethod
    def supports(cls, tx_type: str) -> bool:
        pass


class DriveWealthTransaction(BaseDriveWealthModel,
                             DriveWealthTransactionInterface):
    id = None
    ref_id = None
    account_id = None
    type = None  # CSR, DIV, DIVTAX, MERGER_ACQUISITION, SLIP
    symbol = None
    account_amount_delta: Decimal = None
    datetime = None
    date = None
    data = None
    created_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["created_at"]

    def set_from_response(self, data: dict = None):
        if not data:
            return

        self.ref_id = data["finTranID"]
        self.type = data["finTranTypeID"]
        if "instrument" in data:
            self.symbol = data["instrument"]["symbol"]

        if "tranWhen" in data:
            self.datetime = dateutil.parser.parse(data["tranWhen"])
        else:
            self.datetime = datetime.datetime.now(tz=datetime.timezone.utc)
        self.date = self.datetime.date()

        self.account_amount_delta = Decimal(data["accountAmount"])

        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_transactions"

    @classmethod
    def supports(cls, tx_type) -> bool:
        return True

    @classmethod
    def create_typed_transaction(
        cls, transaction: DriveWealthTransactionInterface
    ) -> DriveWealthTransactionInterface:
        classes = [
            DriveWealthDividendTransaction, DriveWealthSpinOffTransaction,
            DriveWealthMergerAcquisitionTransaction
        ]
        typed_transaction = DriveWealthTransaction()
        for _cls in classes:
            if not _cls.supports(transaction.type):
                continue
            typed_transaction = _cls()
            break

        typed_transaction.account_id = transaction.account_id
        typed_transaction.set_from_response(transaction.data)
        return typed_transaction


class DriveWealthDividendTransaction(DriveWealthTransaction,
                                     DriveWealthTransactionInterface):

    @classmethod
    def supports(cls, tx_type) -> bool:
        return tx_type in ["DIVTAX", "DIV"]


class DriveWealthSpinOffTransaction(DriveWealthTransaction,
                                    DriveWealthTransactionInterface):

    @classmethod
    def supports(cls, tx_type) -> bool:
        return tx_type == "SPINOFF"

    @property
    def position_delta(self) -> Decimal:
        return Decimal(self.data["positionDelta"])

    @property
    def from_symbol(self) -> str:
        m = re.search(DriveWealthSpinOffTransaction.__comment_regex,
                      self.data["comment"])
        if not m:
            raise Exception("Failed to parse transaction comment %s" %
                            self.data["comment"])
        return m[1]

    @property
    def to_symbol(self) -> str:
        m = re.search(self.__comment_regex, self.data["comment"])
        if not m:
            raise Exception("Failed to parse transaction comment %s" %
                            self.data["comment"])
        return m[2]

    @classproperty
    def __comment_regex(cls):
        return r"from (\w*) to (\w*)"


class DRIVEWEALTH_MERGER_ACQUISITION_TX_TYPE(str, enum.Enum):
    EXCHANGE_STOCK_CASH = "EXCHANGE_STOCK_CASH"
    REMOVE_SHARES = "REMOVE_SHARES"
    ADD_SHARES_CASH = "ADD_SHARES_CASH"


class DriveWealthMergerAcquisitionTransaction(DriveWealthTransaction,
                                              DriveWealthTransactionInterface):

    @classmethod
    def supports(cls, tx_type) -> bool:
        return tx_type == "MERGER_ACQUISITION"

    @property
    def position_delta(self) -> Decimal:
        return Decimal(self.data["positionDelta"])

    @property
    def merger_transaction_type(self) -> str:
        return self.data["mergerAcquisition"]["type"]

    @property
    def acquirer_symbol(self) -> str:
        return self.data["mergerAcquisition"]["acquirer"]["symbol"]

    @property
    def acquiree_symbol(self) -> str:
        return self.data["mergerAcquisition"]["acquiree"]["symbol"]


class DriveWealthBankAccount(AbstractProviderBankAccount,
                             BaseDriveWealthModel):
    ref_id = None
    drivewealth_user_id = None
    funding_account_id = None
    plaid_access_token_id = None
    plaid_account_id = None
    status = None
    bank_account_nickname = None
    bank_account_number = None
    bank_routing_number = None
    holder_name = None
    bank_account_type = None
    data = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        if not data:
            return
        self.ref_id = data['id']
        self.status = data["status"]

        details = data["bankAccountDetails"]
        self.bank_account_nickname = details['bankAccountNickname']
        self.bank_account_number = details['bankAccountNumber']
        self.bank_routing_number = details['bankRoutingNumber']
        self.bank_account_type = details.get('bankAccountType')
        self.data = data

        if "userDetails" in data:
            self.drivewealth_user_id = data["userDetails"]['userID']
            self.holder_name = " ".join([
                data["userDetails"]['firstName'],
                data["userDetails"]['lastName']
            ])

    def fill_funding_account_details(self, funding_account: FundingAccount):
        funding_account.mask = self.bank_account_number

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_bank_accounts"


class BaseDriveWealthMoneyFlowModel(BaseDriveWealthModel, ABC):
    ref_id = None
    trading_account_ref_id = None
    bank_account_ref_id = None
    money_flow_id = None
    data = None
    status = None
    fees_total_amount: Decimal = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data=None):
        self.ref_id = data.get("id") or data.get("paymentID")
        if "accountDetails" in data:
            self.trading_account_ref_id = data["accountDetails"]["accountID"]
        elif "accountID" in data:
            self.trading_account_ref_id = data["accountID"]

        if "statusMessage" in data:
            self.status = data["statusMessage"]
        else:
            self.status = data["status"]["message"]

        self.data = data

    def is_pending(self) -> bool:
        return self.status in [
            'Started', DriveWealthRedemptionStatus.RIA_Pending.name, 'Pending',
            'Other', 'On Hold'
        ]

    def is_approved(self) -> bool:
        return self.status in [
            DriveWealthRedemptionStatus.Approved.name,
            DriveWealthRedemptionStatus.RIA_Approved.name
        ]

    def is_successful(self) -> bool:
        return self.status == DriveWealthRedemptionStatus.Successful.name

    def get_error_message(self) -> Optional[str]:
        if "status" in self.data and isinstance(self.data["status"], dict):
            return self.data["status"].get("comment")
        return self.data.get("statusComment")

    def get_money_flow_status(self) -> TradingMoneyFlowStatus:
        """
        Started	0	"STARTED"
        Pending	1	"PENDING"	Every new deposit for a self-directed account is set to "Pending". From here, the deposit can be marked as "Rejected", "On Hold" or "Approved".
        Successful	2	"SUCCESSFUL"	After a deposit is marked "Approved", the next step is "Successful".
        Failed	3	"FAILED"	If a deposit is marked as "Rejected", the deposit will immediately be set to "Failed".
        Other	4	"OTHER"
        RIA Pending	11	"RIA_Pending"
        RIA Approved	12	"RIA_Approved"
        RIA Rejected	13	"RIA_Rejected"
        Approved	14	"APPROVED"	Once marked as "Approved", the deposit will be processed.
        Rejected	15	"REJECTED"	Updating a deposit to "Rejected" will immediately set it's status to "Failed"
        On Hold	16	"ON_HOLD"	The "On Hold" status is reserved for deposits that aren't ready to be processed.
        Returned	5	"RETURNED"	A deposit is marked as returned if DW receives notification from our bank that the deposit had failed.
        Unknown	-1	–	Reserved for errors.
        """

        if self.is_pending():
            return TradingMoneyFlowStatus.PENDING
        if self.is_approved():
            return TradingMoneyFlowStatus.APPROVED
        if self.is_successful():
            return TradingMoneyFlowStatus.SUCCESS
        return TradingMoneyFlowStatus.FAILED


class DriveWealthDeposit(BaseDriveWealthMoneyFlowModel):

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_deposits"


class DriveWealthRedemption(BaseDriveWealthMoneyFlowModel):
    payment_transaction_id = None
    transaction_ref_id = None

    def set_from_response(self, data=None):
        if not data:
            return

        if "fees" in data:
            fees_total_amount = Decimal(0)
            for fee in data["fees"]:
                fees_total_amount += Decimal(fee["amount"])
            self.fees_total_amount = fees_total_amount
        self.transaction_ref_id = data.get("finTranRef") or data.get(
            "finTranReference")

        super().set_from_response(data)

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_redemptions"

    def update_payment_transaction(self,
                                   payment_transaction: PaymentTransaction):
        if self.is_pending() or self.is_approved():
            status = PaymentTransactionStatus.PENDING
        elif self.is_successful():
            status = PaymentTransactionStatus.SUCCESS
        else:
            status = PaymentTransactionStatus.FAILED

        payment_transaction.status = status

    @property
    def amount(self) -> Optional[Decimal]:
        if self.data and self.data.get("amount"):
            return Decimal(self.data["amount"])

        return None


class DriveWealthKycStatus:
    data = None

    def __init__(self, data: dict):
        self.data = data

    def get_profile_kyc_status(self) -> ProfileKycStatus:
        kyc = self.data["kyc"]
        message = kyc["status"].get("name") or kyc.get("statusComment")
        errors = kyc.get("errors", [])
        error_codes = list(map(lambda e: e["code"], errors))
        kyc_status = self.map_dw_kyc_status(kyc["status"]["name"], error_codes)

        entity = ProfileKycStatus()
        entity.status = kyc_status
        entity.message = message
        entity.error_codes = DriveWealthKycStatus.map_dw_error_codes(
            error_codes)
        entity.reset_error_messages()
        entity.created_at = dateutil.parser.parse(kyc["updated"])
        return entity

    @staticmethod
    def map_dw_kyc_status(kyc_status, error_codes=None):
        if kyc_status == "KYC_NOT_READY":
            return KycStatus.NOT_READY
        if kyc_status == "KYC_READY":
            return KycStatus.READY
        if kyc_status == "KYC_PROCESSING":
            return KycStatus.PROCESSING
        if kyc_status == "KYC_APPROVED":
            return KycStatus.APPROVED
        if kyc_status == "KYC_INFO_REQUIRED":
            return KycStatus.INFO_REQUIRED
        if kyc_status == "KYC_DOC_REQUIRED":
            return KycStatus.DOC_REQUIRED
        if kyc_status == "KYC_MANUAL_REVIEW":
            if error_codes:
                return KycStatus.INFO_REQUIRED
            else:
                return KycStatus.MANUAL_REVIEW
        if kyc_status == "KYC_DENIED":
            return KycStatus.DENIED
        raise Exception('Unknown kyc status %s' % kyc_status)

    @staticmethod
    def map_dw_error_codes(error_codes: list[str]) -> list[KycErrorCode]:
        result = []
        for i in DW_ERRORS_MAPPING:
            if i["name"] in error_codes or i["code"] in error_codes:
                result.append(i["gainy_code"])
        return result


class DriveWealthOrder(BaseDriveWealthModel):
    ref_id = None
    status = None  # NEW, PARTIAL_FILL, CANCELLED, REJECTED, FILLED
    account_id = None
    symbol = None
    symbol_normalized = None
    data = None
    last_executed_at = None
    total_order_amount_normalized: Decimal = None
    date: datetime.date = None
    created_at = None
    updated_at = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["created_at", "updated_at"]

    def set_from_response(self, data: dict = None):
        if not data:
            return
        self.ref_id = data["id"]
        self.status = data["status"]
        self.account_id = data["accountID"]
        self.symbol = data["symbol"]
        self.symbol_normalized = normalize_symbol(data["symbol"])
        if "lastExecuted" in data:
            self.last_executed_at = dateutil.parser.parse(data["lastExecuted"])
            self.date = self.last_executed_at.astimezone(
                pytz.timezone('America/New_York')).date()
        self.total_order_amount_normalized = abs(
            Decimal(data['totalOrderAmount'])) * (-1 if data['side'] == 'SELL'
                                                  else 1)
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_orders"

    def is_filled(self) -> bool:
        return self.status == "FILLED"

    def is_rejected(self):
        return self.status == "REJECTED"


class DriveWealthStatement(BaseDriveWealthModel):
    file_key: str = None
    trading_statement_id: int = None
    type: TradingStatementType = None
    display_name: str = None
    account_id: str = None
    user_id: str = None
    created_at: datetime.datetime = None

    key_fields = ["account_id", "type", "file_key"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["created_at"]

    def set_from_dict(self, row: dict = None):
        super().set_from_dict(row)

        if row and row["type"]:
            self.type = TradingStatementType(row["type"])
        return self

    def set_from_response(self, data: dict = None):
        if not data:
            return
        self.display_name = data["displayName"]
        self.file_key = data["fileKey"]
        self.data = data

    @classproperty
    def table_name(self) -> str:
        return "drivewealth_statements"

    @property
    def date(self) -> Optional[datetime.date]:
        if not self.file_key:
            return None

        try:
            return datetime.datetime.strptime(self.file_key[:8],
                                              "%Y%m%d").date()
        except Exception as e:
            logger.exception(e, extra={"file_key": self.file_key})
            return None


class DriveWealthCorporateActionTransactionLink(BaseModel):
    corporate_action_adjustment_id: int = None
    drivewealth_transaction_id: int = None
    created_at: datetime.datetime = None

    key_fields = [
        "corporate_action_adjustment_id", "drivewealth_transaction_id"
    ]
    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["created_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "corporate_action_drivewealth_transaction_link"
