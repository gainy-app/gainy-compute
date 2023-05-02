from decimal import Decimal
from typing import Dict, Any, Optional, List

from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, DW_WEIGHT_THRESHOLD, PRECISION
from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.models import TradingCollectionVersion, TradingOrder, AbstractTradingOrder
from gainy.trading.repository import TradingRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderRebalanceHelper:

    def __init__(self, provider: DriveWealthProviderBase,
                 trading_repository: TradingRepository):
        self.provider = provider
        self.repository = provider.repository
        self.trading_repository = trading_repository
        self.api = provider.api

    def ensure_fund(self, profile_id,
                    trading_order: AbstractTradingOrder) -> DriveWealthFund:
        repository = self.repository
        collection_id = None
        symbol = None

        if isinstance(trading_order, TradingCollectionVersion):
            collection_id = trading_order.collection_id
            weights = trading_order.weights

            fund = self.repository.get_profile_fund(
                profile_id, collection_id=collection_id)

            # todo only update weights if they were marked to be updated in the order
            # if fund and trading_order.use_static_weights:
            #     fund.weights = weights
            #     repository.persist(fund)

        elif isinstance(trading_order, TradingOrder):
            symbol = trading_order.symbol
            weights = {symbol: Decimal(1)}
            fund = self.repository.get_profile_fund(profile_id, symbol=symbol)
        else:
            raise Exception("Unsupported order class.")

        if fund and fund.has_valid_weights():
            return fund

        if not fund:
            fund = DriveWealthFund()
        fund.profile_id = profile_id
        fund.holdings = self._generate_new_fund_holdings(weights, fund)
        fund.weights = weights
        fund.normalize_weights()

        user = repository.get_user(profile_id)
        user_id = user.ref_id

        if isinstance(trading_order, TradingCollectionVersion):
            fund.collection_id = collection_id
            fund.trading_collection_version_id = trading_order.id
            name = f"Gainy {user_id}'s fund for collection {collection_id}"
            client_fund_id = f"{profile_id}_{collection_id}"
        elif isinstance(trading_order, TradingOrder):
            fund.symbol = symbol
            fund.trading_order_id = trading_order.id
            name = f"Gainy {user_id}'s fund for symbol {symbol}"
            client_fund_id = f"{profile_id}_{symbol}"
        else:
            raise Exception("Unsupported order class.")

        if not fund.ref_id:
            description = name
            self.api.create_fund(fund, name, client_fund_id, description)

        repository.persist(fund)

        return fund

    def handle_cash_amount_change(self, order: AbstractTradingOrder,
                                  portfolio: DriveWealthPortfolio,
                                  chosen_fund: DriveWealthFund):

        target_amount_delta = Decimal(
            order.target_amount_delta) if order.target_amount_delta else None
        if order.executed_amount:
            target_amount_delta -= order.executed_amount
        target_amount_delta_relative = order.target_amount_delta_relative

        if not target_amount_delta and not target_amount_delta_relative:
            return

        portfolio_status = self.provider.sync_portfolio_status(portfolio)
        cash_weight = portfolio.cash_target_weight
        cash_value = cash_weight * portfolio_status.equity_value
        fund_weight = portfolio.get_fund_weight(chosen_fund.ref_id)
        fund_value = fund_weight * portfolio_status.equity_value

        logging_extra = {
            "profile_id": portfolio.profile_id,
            "target_amount_delta": target_amount_delta,
            "target_amount_delta_relative": target_amount_delta_relative,
            "portfolio_status": portfolio_status.to_dict(),
            "chosen_fund": chosen_fund.to_dict(),
            "cash_weight": cash_weight,
            "cash_value": cash_value,
            "fund_weight": fund_weight,
            "fund_value": fund_value,
            "portfolio_pre": portfolio.to_dict(),
        }
        try:
            if target_amount_delta_relative is not None:
                if target_amount_delta_relative < -1 or target_amount_delta_relative >= 0:
                    raise Exception(
                        'target_amount_delta_relative must be within [-1, 0).')

                # negative target_amount_delta_relative - check fund_value is > 0
                if fund_weight < DW_WEIGHT_THRESHOLD:
                    raise InsufficientFundsException()
                weight_delta = target_amount_delta_relative * fund_weight
                order.target_amount_delta = target_amount_delta_relative * fund_value
            elif target_amount_delta > 0:
                if cash_value - target_amount_delta < -PRECISION:
                    raise InsufficientFundsException()
                weight_delta = target_amount_delta / cash_value * cash_weight
            else:
                weight_delta = target_amount_delta / fund_value * fund_weight
                if weight_delta < -fund_weight:
                    # if target_amount_delta < -fund_value, then sell all
                    weight_delta = -fund_weight
                    order.target_amount_delta = -fund_value
                    order.target_amount_delta_relative = -1

            logging_extra["weight_delta"] = weight_delta

            portfolio.move_cash_to_fund(chosen_fund, weight_delta)
            self.repository.persist(portfolio)
            logging_extra["portfolio_post"] = portfolio.to_dict()
            logger.info('_handle_cash_amount_change', extra=logging_extra)
        except InsufficientFundsException as e:
            logger.warning('_handle_cash_amount_change: %s',
                           e,
                           extra=logging_extra)
            raise e
        except Exception as e:
            logging_extra["exc"] = e
            logger.exception('_handle_cash_amount_change: %s',
                             e,
                             extra=logging_extra)
            raise e

    def _generate_new_fund_holdings(
            self, weights: Dict[str, Any],
            fund: Optional[DriveWealthFund]) -> List[Dict[str, Any]]:
        new_holdings = {}

        # add old holdings with zero weight for the api to remove it if they are missing from the weights
        if fund:
            for holding in fund.holdings:
                new_holdings[holding["instrumentID"]] = 0

        weight_sum = Decimal(0)
        for symbol, weight in weights.items():
            weight = Decimal(weight)
            try:
                instrument = self.repository.get_instrument_by_symbol(symbol)
                new_holdings[instrument.ref_id] = weight
                weight_sum += weight
            except EntityNotFoundException:
                pass

        return [{
            "instrumentID": k,
            "target": i / weight_sum,
        } for k, i in new_holdings.items()]
