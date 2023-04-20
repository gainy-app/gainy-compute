from decimal import Decimal

from typing import Optional

import datetime

from gainy.billing.models import PaymentTransaction, Invoice, InvoiceStatus
from gainy.data_access.operators import OperatorGt
from gainy.exceptions import EntityNotFoundException, NotFoundException
from gainy.trading import MIN_FIRST_DEPOSIT_AMOUNT
from gainy.trading.drivewealth.exceptions import TradingAccountNotOpenException, BadMissingParametersBodyException, \
    InvalidDriveWealthPortfolioStatusException
from gainy.trading.drivewealth.models import DriveWealthAccountMoney, DriveWealthAccountPositions, DriveWealthAccount, \
    DriveWealthUser, DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthPortfolioStatus, PRECISION, \
    DriveWealthAccountStatus, BaseDriveWealthMoneyFlowModel, DriveWealthRedemptionStatus, DriveWealthInstrument, \
    DriveWealthOrder, DriveWealthRedemption, DriveWealthStatement

from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.exceptions import SymbolIsNotTradeableException
from gainy.trading.models import TradingAccount, TradingCollectionVersion, TradingOrder, TradingOrderStatus, \
    TradingMoneyFlow, TradingStatement
from gainy.utils import get_logger, ENV_PRODUCTION, env

logger = get_logger(__name__)

DRIVE_WEALTH_ACCOUNT_MONEY_STATUS_TTL = 300  # in seconds
DRIVE_WEALTH_ACCOUNT_POSITIONS_STATUS_TTL = 300  # in seconds


class DriveWealthProvider(DriveWealthProviderBase):

    def sync_user(self, user_ref_id) -> DriveWealthUser:
        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": user_ref_id}) or DriveWealthUser()

        data = self.api.get_user(user_ref_id)
        user.set_from_response(data)
        self.repository.persist(user)
        return user

    def sync_profile_trading_accounts(self, profile_id: int):
        repository = self.repository
        user_ref_id = self._get_user(profile_id).ref_id

        accounts_data = self.api.get_user_accounts(user_ref_id)
        for account_data in accounts_data:
            account_ref_id = account_data["id"]

            account: DriveWealthAccount = repository.find_one(
                DriveWealthAccount,
                {"ref_id": account_ref_id}) or DriveWealthAccount()
            account.set_from_response(account_data)
            repository.persist(account)

            self.sync_trading_account(account_ref_id=account_ref_id)

    def sync_balances(self, account: TradingAccount, force: bool = False):
        dw_account = self.sync_trading_account(trading_account_id=account.id,
                                               force=force)
        if dw_account and dw_account.is_open():
            self.sync_portfolios(account.profile_id, force=force)
        self.repository.refresh(account)

    def sync_trading_account(
            self,
            account_ref_id: str = None,
            trading_account_id: int = None,
            fetch_info: bool = False,
            force: bool = False) -> Optional[DriveWealthAccount]:
        repository = self.repository

        _filter = {}
        if account_ref_id:
            _filter["ref_id"] = account_ref_id
        if trading_account_id:
            _filter["trading_account_id"] = trading_account_id
        if not _filter:
            raise Exception("At least one of the filters must be specified")
        account: DriveWealthAccount = repository.find_one(
            DriveWealthAccount, _filter)

        if account and account.is_artificial:
            return account

        if account:
            account_ref_id = account.ref_id
        else:
            if not account_ref_id:
                return None

            account = DriveWealthAccount()
            account.ref_id = account_ref_id
            fetch_info = True

        if fetch_info:
            self.sync_account(account)

        account_money = self.sync_account_money(account_ref_id, force=force)
        account_positions = self.sync_account_positions(account_ref_id,
                                                        force=force)

        if account.trading_account_id is None:
            return account

        trading_account = repository.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if trading_account is None:
            return account

        account.update_trading_account(trading_account)
        account_money.update_trading_account(trading_account)
        account_positions.update_trading_account(trading_account)

        repository.persist(trading_account)

        return account

    def actualize_portfolio(
            self, portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus) -> bool:
        if not portfolio_status.is_pending_rebalance():
            logging_extra = {
                "profile_id": portfolio.profile_id,
                "portfolio_status": portfolio_status.to_dict(),
                "portfolio_pre": portfolio.to_dict(),
            }
            portfolio.set_target_weights_from_status_actual_weights(
                portfolio_status)
            logging_extra["portfolio_post"] = portfolio.to_dict()
            logger.info('set_target_weights_from_status_actual_weights',
                        extra=logging_extra)

        return self.rebalance_portfolio_cash(portfolio, portfolio_status)

    def rebalance_portfolio_cash(
            self, portfolio: DriveWealthPortfolio,
            portfolio_status: DriveWealthPortfolioStatus) -> bool:
        new_equity_value = portfolio_status.equity_value

        new_transactions_amount_sum = Decimal(0)
        new_transactions = self.repository.get_new_transactions(
            portfolio.drivewealth_account_id, portfolio.last_transaction_id)
        for transaction in new_transactions:
            if portfolio.last_transaction_id:
                portfolio.last_transaction_id = max(
                    portfolio.last_transaction_id, transaction.id)
            else:
                portfolio.last_transaction_id = transaction.id

            new_transactions_amount_sum += transaction.account_amount_delta

        # pending redemptions do not have transactions, but are already accounted in portfolio balance.
        pending_redemptions_amount_sum = Decimal(0)
        pending_redemptions = self.repository.get_pending_redemptions(
            portfolio.drivewealth_account_id)
        for redemption in pending_redemptions:
            pending_redemptions_amount_sum += redemption.amount

        new_transactions_amount_sum += pending_redemptions_amount_sum - portfolio.pending_redemptions_amount_sum

        logging_extra = {
            "profile_id": portfolio.profile_id,
            "prev_pending_redemptions_amount_sum":
            portfolio.pending_redemptions_amount_sum,
            "new_pending_redemptions_amount_sum":
            pending_redemptions_amount_sum,
            "new_transactions_amount_sum": new_transactions_amount_sum,
            "new_transactions": [i.to_dict() for i in new_transactions],
            "portfolio_pre": portfolio.to_dict(),
            "portfolio_status": portfolio_status.to_dict(),
        }

        if abs(new_transactions_amount_sum) < PRECISION:
            portfolio.last_equity_value = new_equity_value
            portfolio.pending_redemptions_amount_sum = pending_redemptions_amount_sum
            self.repository.persist(portfolio)
            return False

        if not new_equity_value:
            # todo handle?
            return False
        '''
        new_transactions_amount_sum=200
        cash_weight 0.5     0.8333
        fund_weight 0.5     0.1667
        equity_value 100    300
        
        cash_weight_delta = (0.5 * 100 + 200) / 300 - 0.5 = 0.3333
        '''

        if portfolio.last_equity_value:
            last_equity_value = portfolio.last_equity_value
        else:
            last_equity_value = Decimal(0)

        try:
            cash_weight_delta = (
                portfolio.cash_target_weight * last_equity_value +
                new_transactions_amount_sum
            ) / new_equity_value - portfolio.cash_target_weight
            logging_extra["cash_weight_delta"] = cash_weight_delta
            portfolio.rebalance_cash(cash_weight_delta)
            portfolio.last_equity_value = new_equity_value
            portfolio.pending_redemptions_amount_sum = pending_redemptions_amount_sum
            logging_extra["portfolio_post"] = portfolio.to_dict()

            logger.info('rebalance_portfolio_cash', extra=logging_extra)
            self.repository.persist(portfolio)
            return True
        except Exception as e:
            logging_extra["exc"] = e
            logger.exception('rebalance_portfolio_cash', extra=logging_extra)
            raise e

    def reconfigure_collection_holdings(
            self, portfolio: DriveWealthPortfolio,
            collection_version: TradingCollectionVersion,
            is_pending_rebalance: bool):
        helper = DriveWealthProviderRebalanceHelper(self,
                                                    self.trading_repository)
        profile_id = collection_version.profile_id
        chosen_fund = helper.upsert_fund(profile_id, collection_version)
        helper.handle_cash_amount_change(collection_version, portfolio,
                                         chosen_fund, is_pending_rebalance)
        collection_version.status = TradingOrderStatus.PENDING_EXECUTION
        collection_version.pending_execution_since = datetime.datetime.now()
        self.repository.persist(collection_version)
        portfolio.set_pending_rebalance()
        self.repository.persist(portfolio)

    def execute_order_in_portfolio(self, portfolio: DriveWealthPortfolio,
                                   trading_order: TradingOrder,
                                   is_pending_rebalance: bool):
        helper = DriveWealthProviderRebalanceHelper(self,
                                                    self.trading_repository)
        profile_id = trading_order.profile_id
        chosen_fund = helper.upsert_stock_fund(profile_id, trading_order)
        helper.handle_cash_amount_change(trading_order, portfolio, chosen_fund,
                                         is_pending_rebalance)
        trading_order.status = TradingOrderStatus.PENDING_EXECUTION
        trading_order.pending_execution_since = datetime.datetime.now()
        self.repository.persist(trading_order)
        portfolio.set_pending_rebalance()
        self.repository.persist(portfolio)

    def ensure_portfolio(self, profile_id, trading_account_id):
        repository = self.repository
        account: DriveWealthAccount = repository.find_one(
            DriveWealthAccount, {"trading_account_id": trading_account_id})
        if not account or not account.is_open():
            raise TradingAccountNotOpenException()

        portfolio = repository.get_profile_portfolio(profile_id,
                                                     trading_account_id)

        if not portfolio:
            name = f"Gainy profile #{profile_id}'s portfolio"
            client_portfolio_id = profile_id  # TODO change to some other entity
            description = name

            portfolio = DriveWealthPortfolio()
            portfolio.profile_id = profile_id
            self.api.create_portfolio(portfolio, name, client_portfolio_id,
                                      description)

        if not portfolio.drivewealth_account_id:
            account = self.repository.get_account(trading_account_id)
            self.api.update_account(account.ref_id, portfolio.ref_id)
            portfolio.drivewealth_account_id = account.ref_id

        repository.persist(portfolio)

        return portfolio

    def send_portfolio_to_api(self, portfolio: DriveWealthPortfolio):
        if portfolio.is_artificial:
            return
        self.api.update_portfolio(portfolio)
        portfolio.set_pending_rebalance()
        self.repository.persist(portfolio)

    def sync_account_money(self,
                           account_ref_id: str,
                           force: bool = False) -> DriveWealthAccountMoney:

        account_money: DriveWealthAccountMoney = self.repository.find_one(
            DriveWealthAccountMoney, {
                "drivewealth_account_id":
                account_ref_id,
                "created_at":
                OperatorGt(
                    datetime.datetime.now(datetime.timezone.utc) -
                    datetime.timedelta(
                        seconds=DRIVE_WEALTH_ACCOUNT_MONEY_STATUS_TTL)),
            }, [("created_at", "DESC")])

        if not force and account_money:
            return account_money

        account_money_data = self.api.get_account_money(account_ref_id)
        account_money = DriveWealthAccountMoney()
        account_money.set_from_response(account_money_data)
        self.repository.persist(account_money)
        return account_money

    def sync_account_positions(
            self,
            account_ref_id: str,
            force: bool = False) -> DriveWealthAccountPositions:

        account_positions: DriveWealthAccountPositions = self.repository.find_one(
            DriveWealthAccountPositions, {
                "drivewealth_account_id":
                account_ref_id,
                "created_at":
                OperatorGt(
                    datetime.datetime.now(datetime.timezone.utc) -
                    datetime.timedelta(
                        seconds=DRIVE_WEALTH_ACCOUNT_POSITIONS_STATUS_TTL)),
            }, [("created_at", "DESC")])

        if not force and account_positions:
            return account_positions

        account_positions_data = self.api.get_account_positions(account_ref_id)
        account_positions = DriveWealthAccountPositions()
        account_positions.set_from_response(account_positions_data)
        self.repository.persist(account_positions)
        return account_positions

    def check_tradeable_symbol(self, symbol: str):
        try:
            instrument = self.repository.get_instrument_by_symbol(symbol)
            if instrument.status != DriveWealthInstrumentStatus.ACTIVE:
                raise SymbolIsNotTradeableException(symbol)
        except EntityNotFoundException:
            raise SymbolIsNotTradeableException(symbol)

    def sync_account(self, account: DriveWealthAccount):
        account_data = self.api.get_account(account.ref_id)
        account.set_from_response(account_data)

        if not self.repository.find_one(
                DriveWealthUser, {"ref_id": account.drivewealth_user_id}):
            self.sync_user(account.drivewealth_user_id)

        self.repository.persist(account)

    def handle_account_status_change(self, account: DriveWealthAccount,
                                     old_status: Optional[str]):
        if env() != ENV_PRODUCTION:
            return

        if old_status != DriveWealthAccountStatus.OPEN:
            return
        if old_status == account.status:
            return

        self.notification_service.notify_dw_account_status_changed(
            account.ref_id, old_status, account.status)

    def handle_money_flow_status_change(
            self, money_flow: BaseDriveWealthMoneyFlowModel,
            old_status: Optional[str]):
        if env() != ENV_PRODUCTION:
            return

        if old_status != DriveWealthRedemptionStatus.Successful:
            return
        if old_status == money_flow.status:
            return

        self.notification_service.notify_dw_money_flow_status_changed(
            money_flow.__class__.__name__, money_flow.ref_id, old_status,
            money_flow.status)

    def update_money_flow_from_dw(
            self,
            entity: BaseDriveWealthMoneyFlowModel,
            money_flow: TradingMoneyFlow = None) -> Optional[TradingMoneyFlow]:
        if not money_flow:
            if not entity.money_flow_id:
                return
            money_flow: TradingMoneyFlow = self.repository.find_one(
                TradingMoneyFlow, {"id": entity.money_flow_id})

        if not money_flow:
            return

        money_flow.status = entity.get_money_flow_status()
        money_flow.fees_total_amount = entity.fees_total_amount
        self.repository.persist(money_flow)
        return money_flow

    def handle_instrument_status_change(self,
                                        instrument: DriveWealthInstrument,
                                        new_status: str):
        if env() != ENV_PRODUCTION:
            return

        if DriveWealthInstrumentStatus.ACTIVE not in [
                instrument.status, new_status
        ]:
            return

        if not self.repository.symbol_is_in_collection(instrument.symbol):
            return

        self.notification_service.notify_dw_instrument_status_changed(
            instrument.symbol, instrument.status, new_status)

    def get_profile_id_by_user_id(self, user_ref_id: str) -> int:
        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": user_ref_id})
        if not user or not user.profile_id:
            raise NotFoundException

        return user.profile_id

    def handle_order(self, order: DriveWealthOrder):
        if not order.last_executed_at:
            return

        account: DriveWealthAccount = self.repository.find_one(
            DriveWealthAccount, {"ref_id": order.account_id})
        if not account:
            return

        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio, {"drivewealth_account_id": account.ref_id})
        if not portfolio:
            return

        if not portfolio.last_order_executed_at or order.last_executed_at > portfolio.last_order_executed_at:
            portfolio.last_order_executed_at = order.last_executed_at
            self.repository.persist(portfolio)

    def sync_redemption(self, redemption_ref_id: str) -> DriveWealthRedemption:
        repository = self.repository

        entity = repository.find_one(
            DriveWealthRedemption,
            {"ref_id": redemption_ref_id}) or DriveWealthRedemption()

        redemption_data = self.api.get_redemption(redemption_ref_id)
        entity.set_from_response(redemption_data)
        self.ensure_account_exists(entity.trading_account_ref_id)
        repository.persist(entity)

        self.update_money_flow_from_dw(entity)
        return entity

    def ensure_account_exists(self, ref_id: str):
        if self.repository.find_one(DriveWealthAccount, {"ref_id": ref_id}):
            return

        self.sync_trading_account(account_ref_id=ref_id, fetch_info=True)

    def handle_redemption_status(self, redemption: DriveWealthRedemption):
        if redemption.status == DriveWealthRedemptionStatus.RIA_Pending:
            try:
                self.api.update_redemption(
                    redemption,
                    status=DriveWealthRedemptionStatus.RIA_Approved)
            except BadMissingParametersBodyException as e:
                redemption = self.sync_redemption(redemption.ref_id)
                if redemption.status != DriveWealthRedemptionStatus.RIA_Pending:
                    return
                raise e

    def on_new_transaction(self, account_ref_id: str):
        #todo thread-safe!
        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio, {"drivewealth_account_id": account_ref_id})
        if not portfolio:
            return

        self.sync_portfolio(portfolio)
        try:
            portfolio_status = self.sync_portfolio_status(portfolio,
                                                          force=True)
        except InvalidDriveWealthPortfolioStatusException as e:
            portfolio_status = self.get_latest_portfolio_status(
                portfolio.ref_id)

            # in case we received an invalid portfolio status - look for a valid one, which is not more than an hour old
            min_created_at = datetime.datetime.now(
                datetime.timezone.utc) - datetime.timedelta(hours=1)
            if not portfolio_status or portfolio_status.created_at < min_created_at:
                logger.exception(e)
                # use invalid portfolio status anyway, what choice do we have?..
                portfolio_status = e.portfolio_status

        portfolio_changed = self.actualize_portfolio(portfolio,
                                                     portfolio_status)
        if not portfolio_changed:
            return

        portfolio.normalize_weights()
        self.send_portfolio_to_api(portfolio)

    def update_payment_transaction_from_dw(self,
                                           redemption: DriveWealthRedemption):
        if redemption.payment_transaction_id is None:
            return

        payment_transaction: PaymentTransaction = self.repository.find_one(
            PaymentTransaction, {"id": redemption.payment_transaction_id})
        if not payment_transaction:
            return

        redemption.update_payment_transaction(payment_transaction)
        self.repository.persist(payment_transaction)

        invoice: Invoice = self.repository.find_one(
            Invoice, {"id": payment_transaction.invoice_id})
        prev_invoice_status = invoice.status
        invoice.on_new_transaction(payment_transaction)
        self.repository.persist(invoice)

        if invoice.status == InvoiceStatus.PAID and prev_invoice_status != InvoiceStatus.PAID:
            self.analytics_service.on_commission_withdrawn(
                invoice.profile_id, float(invoice.amount))

    def create_trading_statements(self, entities: list[DriveWealthStatement],
                                  profile_id):
        for dw_statement in entities:
            trading_statement = None
            if dw_statement.trading_statement_id:
                trading_statement = self.repository.find_one(
                    TradingStatement,
                    {"id": dw_statement.trading_statement_id})
            if not trading_statement:
                trading_statement = TradingStatement()

            trading_statement.profile_id = profile_id
            trading_statement.type = dw_statement.type
            trading_statement.display_name = dw_statement.display_name
            trading_statement.date = dw_statement.date
            self.repository.persist(trading_statement)
            dw_statement.trading_statement_id = trading_statement.id
            self.repository.persist(dw_statement)

    def notify_low_balance(self, trading_account: TradingAccount):
        if env() != ENV_PRODUCTION:
            return

        balance = trading_account.equity_value + trading_account.cash_balance
        if balance >= MIN_FIRST_DEPOSIT_AMOUNT:
            return

        self.notification_service.notify_low_balance(
            trading_account.profile_id, balance)

    def ensure_account_created(self, user: DriveWealthUser):
        repository = self.repository

        account: DriveWealthAccount = repository.find_one(
            DriveWealthAccount, {"drivewealth_user_id": user.ref_id})
        if not account:
            account_data = self.api.create_account(user.ref_id)
            account = repository.upsert_user_account(user.ref_id, account_data)

        if account.trading_account_id:
            trading_account: TradingAccount = repository.find_one(
                TradingAccount, {"id": account.trading_account_id})
        elif user.profile_id:
            trading_account: TradingAccount = repository.find_one(
                TradingAccount, {"profile_id": user.profile_id})
        else:
            raise Exception('No profile_id assigned to the DW user.')

        if not trading_account:
            trading_account = TradingAccount()
        trading_account.profile_id = user.profile_id
        trading_account.name = account.nickname
        account.update_trading_account(trading_account)
        repository.persist(trading_account)

        account.trading_account_id = trading_account.id
        repository.persist(account)

    def _get_trading_account(self, user_ref_id) -> DriveWealthAccount:
        return self.repository.get_user_accounts(user_ref_id)[0]
