from decimal import Decimal

from gainy.analytics.constants import EVENT_DW_BROKERAGE_ACCOUNT_OPENED, EVENT_DW_KYC_STATUS_REJECTED, \
    EVENT_DW_DEPOSIT_SUCCSESS, EVENT_DW_WITHDRAW_SUCCESS, EVENT_PURCHASE_COMLETED, EVENT_SELL_COMPLETED, \
    EVENT_COMMISSION_WITHDRAWN
from gainy.analytics.interfaces import AnalyticsSinkInterface, AttributionSourceInterface
from gainy.data_access.operators import OperatorLt
from gainy.data_access.repository import Repository
from gainy.trading.models import TradingOrder, TradingCollectionVersion, TradingMoneyFlow, TradingMoneyFlowStatus
from gainy.utils import get_logger

logger = get_logger(__name__)
PRECISION = Decimal(10)**-3


def _get_order_properties(order) -> dict:
    if isinstance(order, TradingOrder):
        order_id = "to_%d" % order.id
        collection_id = None
        symbol = order.symbol
        _type = 'ticker'
    elif isinstance(order, TradingCollectionVersion):
        order_id = "tcv_%d" % order.id
        collection_id = order.collection_id
        symbol = None
        _type = 'ttf'
    else:
        raise Exception('unsupported class ' + order.__class__.__name__)

    amount = order.target_amount_delta
    return {
        "orderId": order_id,
        "amount": amount,
        "collectionId": collection_id,
        "tickerSymbol": symbol,
        "productType": _type,
    }


class AnalyticsService:

    def __init__(self, attribution_sources: list[AttributionSourceInterface],
                 sinks: list[AnalyticsSinkInterface], repository: Repository):
        self.attribution_sources = attribution_sources
        self.sinks = sinks
        self.repository = repository

    def sync_profile_attribution(self, profile_id):
        attributes = {}
        for source in self.attribution_sources:
            attributes.update(source.get_attributes(profile_id))

        for sink in self.sinks:
            sink.update_profile_attribution(profile_id, attributes)

    def on_dw_brokerage_account_opened(self, profile_id):
        event_name = EVENT_DW_BROKERAGE_ACCOUNT_OPENED
        properties = {}
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)

    def on_dw_kyc_status_rejected(self, profile_id: int):
        event_name = EVENT_DW_KYC_STATUS_REJECTED
        properties = {}
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)

    def on_dw_deposit_success(self, money_flow: TradingMoneyFlow):
        profile_id = money_flow.profile_id
        prev_money_flow = self.repository.find_one(
            TradingMoneyFlow, {
                "profile_id": profile_id,
                "status": TradingMoneyFlowStatus.SUCCESS.name,
                "id": OperatorLt(money_flow.id)
            })
        is_first_deposit = not prev_money_flow

        event_name = EVENT_DW_DEPOSIT_SUCCSESS
        properties = {
            "amount": money_flow.amount,
            "isFirstDeposit": is_first_deposit
        }
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)

    def on_dw_withdraw_success(self, profile_id: int, amount: float):
        event_name = EVENT_DW_WITHDRAW_SUCCESS
        properties = {
            "amount": amount,
        }
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)

    def on_order_executed(self, order):
        if not isinstance(order, TradingOrder) and not isinstance(
                order, TradingCollectionVersion):
            raise Exception('unsupported class ' + order.__class__.__name__)

        properties = {
            **_get_order_properties(order),
            # "orderType": order_type,
            # "isFirstPurshase": is_first_purchase,
        }

        if order.target_amount_delta is None:
            # order_type = "rebalance"
            return
        elif order.target_amount_delta < 0:
            event_name = EVENT_SELL_COMPLETED
            properties["isSellAll"] = abs(
                Decimal(-1) - order.target_amount_delta_relative) < PRECISION
            # order_type = "sell"
        else:
            event_name = EVENT_PURCHASE_COMLETED
            # order_type = "buy"

        for sink in self.sinks:
            sink.send_event(order.profile_id, event_name, properties)

    def on_commission_withdrawn(self, profile_id: int, revenue: float):
        event_name = EVENT_COMMISSION_WITHDRAWN
        properties = {"revenue": revenue}
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)
