from decimal import Decimal
from itertools import groupby
from typing import Optional, Tuple, TypeVar

from gainy.data_access.operators import OperatorIn
from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth import DriveWealthRepository
from gainy.trading.drivewealth.models import DriveWealthPortfolio, DriveWealthFund, \
    DriveWealthSpinOffTransaction, DriveWealthPortfolioStatus, DriveWealthCorporateActionTransactionLink, \
    DriveWealthAccountPositions, DriveWealthDividendTransaction, \
    DriveWealthTransactionInterface, PRECISION, DriveWealthTransaction
from gainy.trading.drivewealth.provider.interface import DriveWealthProviderInterface
from gainy.trading.models import CorporateActionAdjustment, AbstractTradingOrder, TradingOrderSource
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService
from gainy.utils import get_logger

logger = get_logger(__name__)
T = TypeVar('T')


class DriveWealthTransactionHandler:

    def __init__(self, provider: DriveWealthProviderInterface,
                 drivewealth_repository: DriveWealthRepository,
                 trading_repository: TradingRepository,
                 trading_service: TradingService):
        self.provider = provider
        self.drivewealth_repository = drivewealth_repository
        self.trading_repository = trading_repository
        self.trading_service = trading_service

    def handle_new_transactions(
            self, portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus) -> bool:

        portfolio_changed = self._handle_transactions(portfolio,
                                                      portfolio_status)
        portfolio_changed = self._handle_redemptions(
            portfolio) or portfolio_changed

        if portfolio_changed:
            self.drivewealth_repository.persist(portfolio)

        return portfolio_changed

    def _handle_transactions(
            self, portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus) -> bool:
        transactions = self.drivewealth_repository.get_new_transactions(
            portfolio.drivewealth_account_id, portfolio.last_transaction_id)

        def key_func(tx):
            return str(tx.__class__)

        for cls, cls_transactions in groupby(
                sorted(transactions, key=key_func), key_func):
            cls_transactions = list(cls_transactions)
            if cls == str(DriveWealthSpinOffTransaction):
                self._handle_spinoff_transactions(cls_transactions, portfolio,
                                                  portfolio_status)
            elif cls == str(DriveWealthDividendTransaction):
                self._handle_dividend_transactions(cls_transactions, portfolio,
                                                   portfolio_status)

        for transaction in transactions:
            if portfolio.last_transaction_id:
                portfolio.last_transaction_id = max(
                    portfolio.last_transaction_id, transaction.id)
            else:
                portfolio.last_transaction_id = transaction.id

        result = len(transactions) > 0
        logging_extra = {
            "profile_id": portfolio.profile_id,
            "new_transactions": [i.to_dict() for i in transactions],
            "portfolio_pre": portfolio.to_dict(),
            "portfolio_status": portfolio_status.to_dict(),
            "result": result,
        }
        logger.info('DriveWealthTransactionHandler _handle_transactions',
                    extra=logging_extra)

        return result

    def _handle_dividend_transactions(
            self, transactions: list[DriveWealthDividendTransaction],
            portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus):
        """
        A certain amount of cash has been added to the account, we need to reinvest it.
        """

        account_positions = self.provider.sync_account_positions(
            portfolio.drivewealth_account_id, force=True)
        logger_extra = {
            "profile_id": portfolio.profile_id,
            "portfolio": portfolio.to_dict(),
            "portfolio_status": portfolio_status.to_dict(),
            "account_positions": account_positions.to_dict(),
            "transactions": [i.to_dict() for i in transactions],
            "symbol_weights": {},
        }

        try:

            def key_func(tx: DriveWealthDividendTransaction):
                return tx.symbol

            for symbol, symbol_transactions in groupby(
                    sorted(transactions, key=key_func), key_func):
                symbol_transactions: list[
                    DriveWealthDividendTransaction] = list(symbol_transactions)

                filtered_transactions = self._filter_linked_transactions(
                    symbol_transactions)
                account_amount_delta_sum: Decimal = sum(
                    tx.account_amount_delta for tx in filtered_transactions)

                symbol_weights = self._get_portfolio_symbol_weights(
                    symbol, portfolio_status)
                logger_extra["symbol_weights"][symbol] = symbol_weights
                for collection_id, weight in symbol_weights:
                    caa = CorporateActionAdjustment()
                    caa.profile_id = portfolio.profile_id
                    caa.trading_account_id = self.provider.get_trading_account_by_portfolio(
                        portfolio).id
                    caa.collection_id = collection_id
                    caa.symbol = symbol
                    caa.amount = account_amount_delta_sum * weight
                    self.drivewealth_repository.persist(caa)

                    self._link_caa(caa, filtered_transactions)
                    self._create_order(caa)
        finally:
            logger.error(
                "DriveWealthTransactionHandler _handle_dividend_transactions",
                extra=logger_extra)

    def _handle_spinoff_transactions(
            self, transactions: list[DriveWealthSpinOffTransaction],
            portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus):
        """
        There are some new stocks on the account, we need to add them to appropriate portfolio funds.
        """

        account_positions = self.provider.sync_account_positions(
            portfolio.drivewealth_account_id, force=True)
        logger_extra = {
            "profile_id": portfolio.profile_id,
            "portfolio": portfolio.to_dict(),
            "portfolio_status": portfolio_status.to_dict(),
            "account_positions": account_positions.to_dict(),
            "transactions": [i.to_dict() for i in transactions],
            "symbol_weights": {},
        }

        try:

            def key_func(tx: DriveWealthSpinOffTransaction):
                return tx.from_symbol, tx.to_symbol, tx.symbol

            for (from_symbol, to_symbol,
                 symbol), symbol_transactions in groupby(
                     sorted(transactions, key=key_func), key_func):
                symbol_transactions: list[
                    DriveWealthSpinOffTransaction] = list(symbol_transactions)

                filtered_transactions = self._filter_linked_transactions(
                    symbol_transactions)
                position_delta_sum: Decimal = sum(
                    tx.position_delta for tx in filtered_transactions)

                symbol_weights = self._get_portfolio_symbol_weights(
                    from_symbol, portfolio_status)
                logger_extra["symbol_weights"][symbol] = symbol_weights
                for collection_id, weight in symbol_weights:
                    caa = CorporateActionAdjustment()
                    caa.profile_id = portfolio.profile_id
                    caa.trading_account_id = self.provider.get_trading_account_by_portfolio(
                        portfolio).id
                    caa.collection_id = collection_id
                    caa.symbol = from_symbol
                    caa.amount = position_delta_sum * self._get_actual_price(
                        to_symbol, account_positions) * weight
                    self.drivewealth_repository.persist(caa)

                    self._link_caa(caa, filtered_transactions)

            # TODO add the new stock to the appropriate portfolio funds and cause automatic weight rebalance
        finally:
            logger.error(
                "DriveWealthTransactionHandler _handle_spinoff_transactions",
                extra=logger_extra)

    def _get_actual_price(
            self,
            symbol: str,
            account_positions: DriveWealthAccountPositions = None) -> Decimal:
        """
        :raises EntityNotFoundException:
        """
        if account_positions:
            try:
                return account_positions.get_symbol_market_price(symbol)
            except EntityNotFoundException:
                pass
            except Exception as e:
                logger.exception(e)

        return self.trading_repository.get_ticker_actual_price(symbol)

    def _get_portfolio_symbol_weights(
        self, symbol, portfolio_status: DriveWealthPortfolioStatus
    ) -> list[Tuple[Optional[int], Decimal]]:
        selected_holdings = []
        value_sum = Decimal(0)
        for fund_id, holdings in portfolio_status.holdings.items():
            for holding in holdings.holdings:
                if holding.symbol != symbol:
                    continue

                fund = self.drivewealth_repository.find_one(
                    DriveWealthFund, {"ref_id": fund_id})
                selected_holdings.append((fund.collection_id, holding.value))
                value_sum += holding.value
                break

        return [(i[0], i[1] / value_sum) for i in selected_holdings]

    def _filter_linked_transactions(self, transactions: list[T]) -> list[T]:
        links: list[
            DriveWealthCorporateActionTransactionLink] = self.trading_repository.find_all(
                DriveWealthCorporateActionTransactionLink, {
                    "drivewealth_transaction_id":
                    OperatorIn([tx.id for tx in transactions])
                })
        found_transaction_ids = {
            link.drivewealth_transaction_id
            for link in links
        }
        return [
            tx for tx in transactions if tx.id not in found_transaction_ids
        ]

    def _link_caa(self, caa: CorporateActionAdjustment,
                  transactions: list[DriveWealthTransactionInterface]):
        links = []
        for tx in transactions:
            dw_caa_tx_link = DriveWealthCorporateActionTransactionLink()
            dw_caa_tx_link.drivewealth_transaction_id = tx.id
            dw_caa_tx_link.corporate_action_adjustment_id = caa.id
            links.append(dw_caa_tx_link)
        self.trading_repository.persist(links)

    def _create_order(self,
                      caa: CorporateActionAdjustment) -> AbstractTradingOrder:
        note = "caa #%d" % caa.id
        if caa.collection_id:
            return self.trading_service.create_collection_version(
                caa.profile_id,
                TradingOrderSource.AUTOMATIC,
                caa.collection_id,
                caa.trading_account_id,
                target_amount_delta=caa.amount,
                note=note)

        return self.trading_service.create_stock_order(
            caa.profile_id,
            TradingOrderSource.AUTOMATIC,
            caa.symbol,
            caa.trading_account_id,
            target_amount_delta=caa.amount,
            note=note)

    def _handle_redemptions(self, portfolio: DriveWealthPortfolio) -> bool:
        # pending redemptions do not have transactions, but are already accounted in portfolio balance.
        pending_redemptions_amount_sum = Decimal(0)
        pending_redemptions = self.drivewealth_repository.get_pending_redemptions(
            portfolio.drivewealth_account_id)
        for redemption in pending_redemptions:
            pending_redemptions_amount_sum += redemption.amount

        prev_pending_redemptions_amount_sum = portfolio.pending_redemptions_amount_sum
        if abs(portfolio.pending_redemptions_amount_sum -
               pending_redemptions_amount_sum) > PRECISION:
            portfolio.pending_redemptions_amount_sum = pending_redemptions_amount_sum
            result = True
        else:
            result = False

        logging_extra = {
            "profile_id": portfolio.profile_id,
            "prev_pending_redemptions_amount_sum":
            prev_pending_redemptions_amount_sum,
            "new_pending_redemptions_amount_sum":
            pending_redemptions_amount_sum,
            "portfolio_pre": portfolio.to_dict(),
            "result": result,
        }
        logger.info('DriveWealthTransactionHandler _handle_redemptions',
                    extra=logging_extra)

        return result
