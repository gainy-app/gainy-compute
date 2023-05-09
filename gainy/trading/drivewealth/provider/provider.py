from typing import Optional

import datetime

from gainy.billing.models import PaymentTransaction, Invoice, InvoiceStatus
from gainy.data_access.operators import OperatorGt
from gainy.exceptions import EntityNotFoundException, NotFoundException
from gainy.models import AbstractEntityLock
from gainy.trading import MIN_FIRST_DEPOSIT_AMOUNT
from gainy.trading.drivewealth.exceptions import TradingAccountNotOpenException, BadMissingParametersBodyException
from gainy.trading.drivewealth.locking_functions.handle_new_transaction import HandleNewTransaction
from gainy.trading.drivewealth.models import DriveWealthAccountMoney, DriveWealthAccount, \
    DriveWealthUser, DriveWealthPortfolio, DriveWealthInstrumentStatus, \
    DriveWealthAccountStatus, BaseDriveWealthMoneyFlowModel, DriveWealthRedemptionStatus, DriveWealthInstrument, \
    DriveWealthOrder, DriveWealthRedemption, DriveWealthStatement, DriveWealthTransactionInterface

from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase, DRIVE_WEALTH_ACCOUNT_MONEY_STATUS_TTL
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.exceptions import SymbolIsNotTradeableException
from gainy.trading.models import TradingAccount, TradingOrderStatus, TradingMoneyFlow, TradingStatement, AbstractTradingOrder
from gainy.utils import get_logger, ENV_PRODUCTION, env

logger = get_logger(__name__)


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

    def execute_order_in_portfolio(self, portfolio: DriveWealthPortfolio,
                                   trading_order: AbstractTradingOrder):
        helper = DriveWealthProviderRebalanceHelper(self,
                                                    self.trading_repository)
        profile_id = trading_order.profile_id
        chosen_fund = helper.ensure_fund(profile_id, trading_order)
        helper.handle_cash_amount_change(trading_order, portfolio, chosen_fund)
        trading_order.status = TradingOrderStatus.PENDING_EXECUTION
        trading_order.pending_execution_since = datetime.datetime.now()
        self.repository.persist(trading_order)
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

        if old_status not in [
                DriveWealthAccountStatus.OPEN,
                DriveWealthAccountStatus.OPEN_NO_NEW_TRADES
        ]:
            return
        if old_status == account.status:
            return

        self.notification_service.notify_dw_account_status_changed(
            account.ref_no, old_status, account.status)

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
        redemption_pre = entity.to_dict()

        redemption_data = self.api.get_redemption(redemption_ref_id)
        entity.set_from_response(redemption_data)
        self.ensure_account_exists(entity.trading_account_ref_id)
        repository.persist(entity)

        logger.info("Updated redemption",
                    extra={
                        "file": __file__,
                        "redemption_pre": redemption_pre,
                        "redemption": entity.to_dict(),
                    })

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

    # disabled in favor of batch transaction handler in the rebalance job
    # def on_new_transaction(self, transaction: DriveWealthTransactionInterface):
    #     entity_lock = AbstractEntityLock(DriveWealthAccount,
    #                                      transaction.account_id)
    #     self.repository.persist(entity_lock)
    #
    #     func = HandleNewTransaction(self.repository, self, entity_lock,
    #                                 transaction)
    #     func.execute()

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
