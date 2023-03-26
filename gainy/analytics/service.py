from decimal import Decimal

from gainy.analytics.constants import EVENT_DW_BROKERAGE_ACCOUNT_OPENED, EVENT_DW_KYC_STATUS_REJECTED, \
    EVENT_DEPOSIT_SUCCESS, EVENT_WITHDRAW_SUCCESS, EVENT_PURCHASE_COMPLETED, EVENT_SELL_COMPLETED, \
    EVENT_COMMISSION_WITHDRAWN
from gainy.analytics.interfaces import AnalyticsSinkInterface, AttributionSourceInterface
from gainy.data_access.operators import OperatorLt, OperatorNot, OperatorEq
from gainy.data_access.repository import Repository
from gainy.trading.models import TradingOrder, TradingCollectionVersion, TradingMoneyFlow, TradingMoneyFlowStatus, \
    AbstractTradingOrder, TradingOrderStatus
from gainy.utils import get_logger

logger = get_logger(__name__)
PRECISION = Decimal(10)**-3


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
        self._emit(profile_id, event_name, properties)

    def on_kyc_status_rejected(self, profile_id: int):
        event_name = EVENT_DW_KYC_STATUS_REJECTED
        properties = {}
        self._emit(profile_id, event_name, properties)

    def on_deposit_success(self, money_flow: TradingMoneyFlow):
        profile_id = money_flow.profile_id
        prev_money_flow = self.repository.find_one(
            TradingMoneyFlow, {
                "profile_id": profile_id,
                "status": TradingMoneyFlowStatus.SUCCESS.name,
                "id": OperatorLt(money_flow.id)
            })
        is_first_deposit = not prev_money_flow

        event_name = EVENT_DEPOSIT_SUCCESS
        properties = {
            "amount": float(money_flow.amount),
            "isFirstDeposit": is_first_deposit
        }
        self._emit(profile_id, event_name, properties)

    def on_withdraw_success(self, profile_id: int, amount: float):
        event_name = EVENT_WITHDRAW_SUCCESS
        properties = {
            "amount": amount,
        }
        self._emit(profile_id, event_name, properties)

    def on_order_executed(self, order: AbstractTradingOrder):
        if not isinstance(order, AbstractTradingOrder):
            raise Exception('unsupported class ' + order.__class__.__name__)

        properties = {
            **self._get_order_properties(order),
        }

        if order.target_amount_delta is None:
            return
        elif order.target_amount_delta < 0:
            event_name = EVENT_SELL_COMPLETED
            properties[
                "isSellAll"] = order.target_amount_delta_relative is not None and abs(
                    Decimal(-1) -
                    order.target_amount_delta_relative) < PRECISION
        else:
            event_name = EVENT_PURCHASE_COMPLETED
            order_classes = [TradingOrder, TradingCollectionVersion]
            is_first_purchase = True
            for order_class in order_classes:
                first_order: AbstractTradingOrder = self.repository.find_one(
                    order_class, {
                        "profile_id":
                        order.profile_id,
                        "status":
                        OperatorNot(
                            OperatorEq(TradingOrderStatus.CANCELLED.name))
                    }, [("created_at", "asc")])
                if first_order and first_order.created_at < order.created_at:
                    is_first_purchase = False
                    break
            properties["isFirstPurchase"] = is_first_purchase

        self._emit(order.profile_id, event_name, properties)

    def on_commission_withdrawn(self, profile_id: int, revenue: float):
        event_name = EVENT_COMMISSION_WITHDRAWN
        properties = {"$revenue": revenue}
        self._emit(profile_id, event_name, properties)

    def _get_order_properties(self, order) -> dict:
        if isinstance(order, TradingOrder):
            order_id = "to_%d" % order.id
            collection_id = None
            symbol = order.symbol
            with self.repository.db_conn.cursor() as cursor:
                query = "select type from base_tickers where symbol = %(symbol)s"
                params = {"symbol": symbol}
                cursor.execute(query, params)
                row = cursor.fetchone()
                _type = row[0] if row else None
        elif isinstance(order, TradingCollectionVersion):
            order_id = "tcv_%d" % order.id
            collection_id = order.collection_id
            symbol = None
            _type = 'ttf'
        else:
            raise Exception('unsupported class ' + order.__class__.__name__)

        amount = order.target_amount_delta
        properties = {
            "orderId": order_id,
            "productType": _type,
        }
        if amount is not None:
            properties["amount"] = float(amount)
        if collection_id:
            properties["collectionId"] = collection_id
        if symbol:
            properties["tickerSymbol"] = symbol
        return properties

    def _emit(self, profile_id, event_name, properties):
        #TODO make async through sqs
        for k, i in properties.items():
            if i is None:
                del properties[k]

        logger.info('Emitting event %s',
                    event_name,
                    extra={
                        "profile_id": profile_id,
                        "event_name": event_name,
                        "properties": properties,
                    })
        for sink in self.sinks:
            sink.send_event(profile_id, event_name, properties)
