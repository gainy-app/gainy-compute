from gainy.trading.drivewealth.models import DriveWealthAccountMoney, DriveWealthAccountPositions, DriveWealthAccount, \
    DriveWealthUser, DriveWealthPortfolio, PRECISION, ONE

from gainy.trading.drivewealth.provider.base import DriveWealthProviderBase
from gainy.trading.drivewealth.provider.rebalance_helper import DriveWealthProviderRebalanceHelper
from gainy.trading.models import TradingAccount, TradingCollectionVersion
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProvider(DriveWealthProviderBase):

    def sync_user(self, user_ref_id):
        user: DriveWealthUser = self.repository.find_one(
            DriveWealthUser, {"ref_id": user_ref_id}) or DriveWealthUser()

        data = self.api.get_user(user_ref_id)
        user.set_from_response(data)
        self.repository.persist(user)

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

    def sync_balances(self, account: TradingAccount):
        self.sync_trading_account(trading_account_id=account.id,
                                  fetch_info=True)
        self.sync_portfolios(account.profile_id)

    def sync_trading_account(self,
                             account_ref_id: str = None,
                             trading_account_id: int = None,
                             fetch_info: bool = False):
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
            return

        if account:
            account_ref_id = account.ref_id
        else:
            if not account_ref_id:
                return

            account = DriveWealthAccount()
            account.ref_id = account_ref_id
            fetch_info = True

        if fetch_info:
            self._sync_account(account)

        account_money = self._sync_account_money(account_ref_id)

        account_positions_data = self.api.get_account_positions(account_ref_id)
        account_positions = DriveWealthAccountPositions()
        account_positions.set_from_response(account_positions_data)
        repository.persist(account_positions)

        if account.trading_account_id is None:
            return

        trading_account = repository.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if trading_account is None:
            return

        account.update_trading_account(trading_account)
        account_money.update_trading_account(trading_account)
        account_positions.update_trading_account(trading_account)

        repository.persist(trading_account)

    def rebalance_portfolio_cash(self, portfolio: DriveWealthPortfolio):
        self.repository.calculate_portfolio_cash_target_value(portfolio)
        self.sync_portfolio(portfolio)
        portfolio_status = self.sync_portfolio_status(portfolio)
        if portfolio_status.equity_value < ONE:
            return

        if abs(portfolio.cash_target_value -
               portfolio_status.cash_value) < PRECISION:
            return

        cash_delta = portfolio.cash_target_value - portfolio_status.cash_value
        cash_weight_delta = cash_delta / portfolio_status.equity_value

        logging_extra = {
            "portfolio_status": portfolio_status.to_dict(),
            "portfolio": portfolio.to_dict(),
            "cash_delta": cash_delta,
            "cash_weight_delta": cash_weight_delta,
        }
        logger.info('rebalance_portfolio_cash step0', extra=logging_extra)

        portfolio.set_target_weights_from_status_actual_weights(
            portfolio_status)
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('rebalance_portfolio_cash step1', extra=logging_extra)

        portfolio.rebalance_cash(cash_weight_delta)
        logging_extra["portfolio"] = portfolio.to_dict()
        logger.info('rebalance_portfolio_cash step2', extra=logging_extra)

    def reconfigure_collection_holdings(
            self, collection_version: TradingCollectionVersion):
        helper = DriveWealthProviderRebalanceHelper(self)

        profile_id = collection_version.profile_id
        portfolio = self.repository.get_profile_portfolio(profile_id)
        if not portfolio:
            raise Exception('Portfolio not found')

        chosen_fund = helper.upsert_fund(profile_id, collection_version)
        helper.handle_cash_amount_change(
            collection_version.target_amount_delta, portfolio, chosen_fund)
        self.repository.persist(portfolio)

    def ensure_portfolio(self, profile_id):
        repository = self.repository
        portfolio = repository.get_profile_portfolio(profile_id)

        if not portfolio:
            name = f"Gainy profile #{profile_id}'s portfolio"
            client_portfolio_id = profile_id  # TODO change to some other entity
            description = name

            portfolio = DriveWealthPortfolio()
            portfolio.profile_id = profile_id
            self.api.create_portfolio(portfolio, name, client_portfolio_id,
                                      description)
            repository.persist(portfolio)

        if not portfolio.drivewealth_account_id:
            user = self._get_user(profile_id)
            account = self._get_trading_account(user.ref_id)
            self.api.update_account(account.ref_id, portfolio.ref_id)
            portfolio.drivewealth_account_id = account.ref_id
            repository.persist(portfolio)

        return portfolio

    def send_portfolio_to_api(self, portfolio: DriveWealthPortfolio):
        self.api.update_portfolio(portfolio)
        portfolio.set_pending_rebalance()
        self.repository.persist(portfolio)

    def _get_trading_account(self, user_ref_id) -> DriveWealthAccount:
        return self.repository.get_user_accounts(user_ref_id)[0]

    def _sync_account(self, account: DriveWealthAccount):
        account_data = self.api.get_account(account.ref_id)
        account.set_from_response(account_data)

        if not self.repository.find_one(
                DriveWealthUser, {"ref_id": account.drivewealth_user_id}):
            self.sync_user(account.drivewealth_user_id)

        self.repository.persist(account)

    def _sync_account_money(self,
                            account_ref_id: str) -> DriveWealthAccountMoney:
        account_money_data = self.api.get_account_money(account_ref_id)
        account_money = DriveWealthAccountMoney()
        account_money.set_from_response(account_money_data)
        self.repository.persist(account_money)
        return account_money
