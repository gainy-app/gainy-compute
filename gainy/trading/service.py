import datetime

from decimal import Decimal

from typing import Iterable, Dict, List, Any

from gainy.exceptions import EntityNotFoundException
from gainy.plaid.exceptions import AccessTokenLoginRequiredException, InvalidAccountIdException, NoAccountsException, \
    InstitutionNotRespondingException
from gainy.plaid.models import PlaidAccessToken
from gainy.plaid.service import PlaidService
from gainy.trading.drivewealth.provider.interface import DriveWealthProviderInterface
from gainy.trading.exceptions import InsufficientFundsException, InsufficientHoldingValueException
from gainy.trading.repository import TradingRepository
from gainy.trading.models import TradingAccount, FundingAccount, TradingCollectionVersion, \
    TradingOrderStatus, TradingOrderSource, TradingOrder
from gainy.utils import get_logger

logger = get_logger(__name__)
PRECISION = Decimal(10)**-3


class TradingService:
    drivewealth_provider: DriveWealthProviderInterface

    def __init__(self, trading_repository: TradingRepository,
                 drivewealth_provider: DriveWealthProviderInterface,
                 plaid_service: PlaidService):
        self.trading_repository = trading_repository
        self.drivewealth_provider = drivewealth_provider
        self.plaid_service = plaid_service

    def sync_profile_trading_accounts(self, profile_id):
        self._get_provider_service().sync_profile_trading_accounts(profile_id)

    def sync_balances(self, account: TradingAccount):
        self._get_provider_service().sync_balances(account)

    def update_funding_accounts_balance(
            self, funding_accounts: Iterable[FundingAccount]):
        by_at_id = {}
        for funding_account in funding_accounts:
            if not funding_account.plaid_access_token_id:
                continue
            if funding_account.plaid_access_token_id not in by_at_id:
                by_at_id[funding_account.plaid_access_token_id] = []
            by_at_id[funding_account.plaid_access_token_id].append(
                funding_account)

        for plaid_access_token_id, funding_accounts in by_at_id.items():
            access_token: PlaidAccessToken = self.trading_repository.find_one(
                PlaidAccessToken, {"id": plaid_access_token_id})

            logger_extra = {
                "profile_id":
                access_token.profile_id,
                "funding_accounts": [
                    funding_account.to_dict()
                    for funding_account in funding_accounts
                ],
            }

            for funding_account in funding_accounts:
                funding_account.needs_reauth = bool(
                    access_token.needs_reauth_since)

            if access_token.needs_reauth_since:
                self.trading_repository.persist(funding_accounts)
                continue

            funding_accounts_by_account_id: Dict[int, FundingAccount] = {
                funding_account.plaid_account_id: funding_account
                for funding_account in funding_accounts
                if funding_account.plaid_account_id
            }

            try:
                plaid_accounts = self.plaid_service.get_item_accounts_balances(
                    access_token, list(funding_accounts_by_account_id.keys()))
            except (InvalidAccountIdException, NoAccountsException) as e:
                for funding_account in funding_accounts:
                    funding_account.needs_reauth = True
                self.trading_repository.persist(funding_accounts)
                logger.info('update_funding_accounts_balance: %s',
                            e,
                            extra=logger_extra)
                continue
            except InstitutionNotRespondingException as e:
                logger.info('update_funding_accounts_balance: %s',
                            e,
                            extra=logger_extra)
                continue
            except AccessTokenLoginRequiredException as e:
                logger.info('update_funding_accounts_balance: %s',
                            e,
                            extra=logger_extra)
                continue

            for plaid_account in plaid_accounts:
                if plaid_account.account_id not in funding_accounts_by_account_id:
                    continue
                funding_accounts_by_account_id[
                    plaid_account.
                    account_id].balance = plaid_account.balance_current

            logger_extra["plaid_accounts"] = [
                plaid_account.to_dict() for plaid_account in plaid_accounts
            ]
            logger.info('update_funding_accounts_balance', extra=logger_extra)
            self.trading_repository.persist(funding_accounts)

    def create_collection_version(self,
                                  profile_id: int,
                                  source: TradingOrderSource,
                                  collection_id: int,
                                  trading_account_id: int,
                                  weights: List[Dict[str, Any]] = None,
                                  target_amount_delta: Decimal = None,
                                  target_amount_delta_relative: Decimal = None,
                                  last_optimization_at: datetime.date = None,
                                  use_static_weights: bool = False,
                                  note: str = None):
        """
        :raises TradingPausedException:
        :raises InsufficientFundsException:
        :raises InsufficientHoldingValueException:
        """
        self.trading_repository.check_profile_trading_not_paused(profile_id)

        if weights:
            # todo mark the newly created collection_version for rebalance job to take these weights into account
            pass
        else:
            weights, last_optimization_at = self.trading_repository.get_collection_actual_weights(
                collection_id)

        weights = self.drivewealth_provider.filter_inactive_symbols_from_weights(
            weights)

        weights = {i["symbol"]: Decimal(i["weight"]) for i in weights}

        if target_amount_delta_relative:
            if target_amount_delta_relative < -1 or target_amount_delta_relative >= 0:
                raise Exception(
                    'target_amount_delta_relative must be within [-1, 0) span.'
                )
            self.check_enough_holding_amount(
                trading_account_id,
                needed_amount_relative=-target_amount_delta_relative,
                collection_id=collection_id)

        if target_amount_delta:
            if target_amount_delta_relative:
                raise Exception(
                    'Can not specify both target_amount_delta and target_amount_delta_relative'
                )

            if target_amount_delta > Decimal(0):
                self.check_enough_buying_power(trading_account_id,
                                               target_amount_delta)
            elif target_amount_delta < Decimal(0):
                self.check_enough_holding_amount(
                    trading_account_id,
                    needed_amount=-target_amount_delta,
                    collection_id=collection_id)

        collection_version = TradingCollectionVersion()
        collection_version.source = source
        collection_version.status = TradingOrderStatus.PENDING
        collection_version.profile_id = profile_id
        collection_version.collection_id = collection_id
        collection_version.weights = weights
        collection_version.target_amount_delta = target_amount_delta
        collection_version.target_amount_delta_relative = target_amount_delta_relative
        collection_version.trading_account_id = trading_account_id
        collection_version.last_optimization_at = last_optimization_at
        collection_version.use_static_weights = use_static_weights
        collection_version.note = note

        self.trading_repository.persist(collection_version)

        return collection_version

    def create_stock_order(self,
                           profile_id: int,
                           source: TradingOrderSource,
                           symbol: str,
                           trading_account_id: int,
                           target_amount_delta: Decimal = None,
                           target_amount_delta_relative: Decimal = None,
                           note: str = None):
        """
        :raises TradingPausedException:
        :raises InsufficientFundsException:
        :raises InsufficientHoldingValueException:
        """
        self.check_tradeable_symbol(symbol)
        self.trading_repository.check_profile_trading_not_paused(profile_id)

        if target_amount_delta_relative:
            if target_amount_delta_relative < -1 or target_amount_delta_relative >= 0:
                raise Exception(
                    'target_amount_delta_relative must be within [-1, 0) span.'
                )
            self.check_enough_holding_amount(
                trading_account_id,
                needed_amount_relative=-target_amount_delta_relative,
                symbol=symbol)

        if target_amount_delta:
            if target_amount_delta_relative:
                raise Exception(
                    'Can not specify both target_amount_delta and target_amount_delta_relative'
                )

            if target_amount_delta > 0:
                self.check_enough_buying_power(trading_account_id,
                                               target_amount_delta)
            else:
                self.check_enough_holding_amount(
                    trading_account_id,
                    needed_amount=-target_amount_delta,
                    symbol=symbol)

        trading_order = TradingOrder()
        trading_order.profile_id = profile_id
        trading_order.source = source
        trading_order.symbol = symbol
        trading_order.status = TradingOrderStatus.PENDING
        trading_order.target_amount_delta = target_amount_delta
        trading_order.target_amount_delta_relative = target_amount_delta_relative
        trading_order.trading_account_id = trading_account_id
        trading_order.note = note

        self.trading_repository.persist(trading_order)

        return trading_order

    def check_enough_buying_power(self, trading_account_id: int,
                                  target_amount_delta: Decimal):
        buying_power = self.trading_repository.get_buying_power(
            trading_account_id)

        if target_amount_delta > buying_power:
            raise InsufficientFundsException()

    def check_enough_holding_amount(self,
                                    trading_account_id: int,
                                    needed_amount: Decimal = None,
                                    needed_amount_relative: Decimal = None,
                                    collection_id: int = None,
                                    symbol: str = None):
        if collection_id is not None and symbol is not None:
            raise Exception('You must specify either collection_id or symbol.')
        if collection_id is None and symbol is None:
            raise Exception('You must specify either collection_id or symbol.')

        trading_account = self.get_trading_account(trading_account_id)

        profile_id = trading_account.profile_id
        if collection_id is not None:
            holding_amount = self.trading_repository.get_collection_holding_value(
                profile_id, collection_id)
        elif symbol is not None:
            holding_amount = self.trading_repository.get_ticker_holding_value(
                profile_id, symbol)
        else:
            raise Exception('You must specify either collection_id or symbol.')

        pending_amount, pending_amount_relative = self.trading_repository.get_pending_orders_amounts(
            profile_id, symbol=symbol, collection_id=collection_id)

        logger.info('check_enough_holding_amount',
                    extra={
                        "profile_id": profile_id,
                        "trading_account_id": trading_account_id,
                        "needed_amount": needed_amount,
                        "needed_amount_relative": needed_amount_relative,
                        "collection_id": collection_id,
                        "symbol": symbol,
                        "pending_amount": pending_amount,
                        "pending_amount_relative": pending_amount_relative,
                        "holding_amount": holding_amount,
                    })

        if needed_amount is not None:
            if needed_amount > holding_amount + pending_amount:
                raise InsufficientHoldingValueException()
        if needed_amount_relative is not None:
            if needed_amount_relative > 1 + pending_amount_relative or holding_amount < PRECISION:
                raise InsufficientHoldingValueException()

    def check_enough_withdrawable_cash(self, trading_account_id: int,
                                       needed_amount: Decimal):
        trading_account = self.get_trading_account(trading_account_id)
        logger.info('check_enough_withdrawable_cash 0',
                    extra={
                        "profile_id": trading_account.profile_id,
                        "account": trading_account.to_dict(),
                    })
        self.sync_balances(trading_account)
        logger.info('check_enough_withdrawable_cash 1',
                    extra={
                        "profile_id": trading_account.profile_id,
                        "account": trading_account.to_dict(),
                    })

        if Decimal(trading_account.cash_available_for_withdrawal
                   or 0) < needed_amount:
            raise InsufficientFundsException()

    def check_tradeable_symbol(self, symbol: str):
        return self._get_provider_service().check_tradeable_symbol(symbol)

    def get_trading_account(self, trading_account_id: int) -> TradingAccount:
        trading_account = self.trading_repository.find_one(
            TradingAccount, {"id": trading_account_id})
        if not trading_account:
            raise EntityNotFoundException(TradingAccount)

        return trading_account

    def _get_provider_service(self):
        return self.drivewealth_provider
