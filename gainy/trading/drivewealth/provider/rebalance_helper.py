from decimal import Decimal
from typing import Dict, Any, Optional, List

from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, PRECISION
from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.models import TradingCollectionVersion, TradingOrder
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderRebalanceHelper:

    def __init__(self, provider: DriveWealthProviderBase):
        self.provider = provider
        self.repository = provider.repository
        self.api = provider.api

    def upsert_fund(
            self, profile_id,
            collection_version: TradingCollectionVersion) -> DriveWealthFund:
        collection_id = collection_version.collection_id
        weights = collection_version.weights
        repository = self.repository

        fund = self.repository.get_profile_fund(
            profile_id, collection_id=collection_id) or DriveWealthFund()
        fund.profile_id = profile_id
        fund.collection_id = collection_id
        fund.holdings = self._generate_new_fund_holdings(weights, fund)
        fund.normalize_weights()

        if fund.ref_id:
            self.api.update_fund(fund)
        else:
            user = repository.get_user(profile_id)
            user_id = user.ref_id
            name = f"Gainy {user_id}'s fund for collection {collection_id}"
            client_fund_id = f"{profile_id}_{collection_id}"
            description = name
            self.api.create_fund(fund, name, client_fund_id, description)

        fund.weights = weights
        fund.trading_collection_version_id = collection_version.id
        repository.persist(fund)

        return fund

    def upsert_stock_fund(self, profile_id,
                          trading_order: TradingOrder) -> DriveWealthFund:
        symbol = trading_order.symbol
        weights = {symbol: Decimal(1)}
        repository = self.repository

        fund = self.repository.get_profile_fund(
            profile_id, symbol=symbol) or DriveWealthFund()
        fund.profile_id = profile_id
        fund.symbol = symbol
        fund.holdings = self._generate_new_fund_holdings(weights, fund)
        fund.normalize_weights()

        if fund.ref_id:
            self.api.update_fund(fund)
        else:
            user = repository.get_user(profile_id)
            user_id = user.ref_id
            name = f"Gainy {user_id}'s fund for symbol {symbol}"
            client_fund_id = f"{profile_id}_{symbol}"
            description = name
            self.api.create_fund(fund, name, client_fund_id, description)

        fund.weights = weights
        fund.trading_order_id = trading_order.id
        repository.persist(fund)

        return fund

    def handle_cash_amount_change(self, target_amount_delta: Decimal,
                                  portfolio: DriveWealthPortfolio,
                                  chosen_fund: DriveWealthFund):
        if not target_amount_delta:
            return 0

        portfolio_status = self.provider.sync_portfolio_status(portfolio)
        if portfolio.is_pending_rebalance():
            cash_actual_weight = portfolio_status.cash_target_weight
            cash_value = cash_actual_weight * portfolio_status.equity_value
            fund_actual_weight = portfolio.get_fund_weight(chosen_fund.ref_id)
            fund_value = fund_actual_weight * portfolio_status.equity_value
        else:
            cash_value = portfolio_status.cash_value
            cash_actual_weight = portfolio_status.cash_actual_weight

            fund_actual_weight = portfolio_status.get_fund_actual_weight(
                chosen_fund.ref_id)
            fund_value = portfolio_status.get_fund_value(chosen_fund.ref_id)
            portfolio.set_target_weights_from_status_actual_weights(
                portfolio_status)

        logging_extra = {
            "target_amount_delta": target_amount_delta,
            "portfolio_status": portfolio_status.to_dict(),
            "portfolio": portfolio.to_dict(),
            "is_pending_rebalance": portfolio.is_pending_rebalance(),
            "chosen_fund": chosen_fund.to_dict(),
            "cash_actual_weight": cash_actual_weight,
            "cash_value": cash_value,
            "fund_actual_weight": fund_actual_weight,
            "fund_value": fund_value,
        }
        logger.info('_handle_cash_amount_change step0', extra=logging_extra)

        # TODO handle initial buy after deposit?
        if target_amount_delta > 0:
            if target_amount_delta - cash_value > PRECISION:
                raise InsufficientFundsException()
            weight_delta = target_amount_delta / cash_value * cash_actual_weight
        else:
            if abs(target_amount_delta) - fund_value > PRECISION:
                raise InsufficientFundsException()
            weight_delta = target_amount_delta / fund_value * fund_actual_weight

        logging_extra["weight_delta"] = weight_delta
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('_handle_cash_amount_change step1', extra=logging_extra)

        portfolio.move_cash_to_fund(chosen_fund, weight_delta)
        self.repository.persist(portfolio)
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('_handle_cash_amount_change step2', extra=logging_extra)

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
