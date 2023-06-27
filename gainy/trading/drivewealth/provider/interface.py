from abc import ABC, abstractmethod

from _decimal import Decimal

from gainy.trading.drivewealth.models import DriveWealthAccountPositions, DriveWealthPortfolio, DriveWealthAccount, \
    DriveWealthPortfolioStatus, DriveWealthDeposit
from gainy.trading.models import TradingAccount


class DriveWealthProviderInterface(ABC):

    @abstractmethod
    def sync_account_positions(
            self,
            account_ref_id: str,
            force: bool = False) -> DriveWealthAccountPositions:
        pass

    @abstractmethod
    def get_trading_account_by_portfolio(
            self, portfolio: DriveWealthPortfolio) -> TradingAccount:
        pass

    @abstractmethod
    def sync_profile_trading_accounts(self, profile_id: int):
        pass

    @abstractmethod
    def sync_balances(self, account: TradingAccount, force: bool = False):
        pass

    @abstractmethod
    def check_tradeable_symbol(self, symbol: str):
        """
        :raises SymbolIsNotTradeableException:
        """
        pass

    @abstractmethod
    def filter_inactive_symbols_from_weights(self, weights):
        pass

    @abstractmethod
    def ensure_portfolio(self, profile_id: int, account: DriveWealthAccount):
        """
        :raises TradingAccountNotOpenException:
        """
        pass

    @abstractmethod
    def sync_portfolio(self,
                       portfolio: DriveWealthPortfolio,
                       force: bool = False):
        pass

    @abstractmethod
    def sync_portfolio_status(
            self,
            portfolio: DriveWealthPortfolio,
            force: bool = False,
            allow_invalid: bool = False) -> DriveWealthPortfolioStatus:
        pass

    @abstractmethod
    def reward_with_cash(self, trading_account_id: int,
                         amount: Decimal) -> DriveWealthDeposit:
        pass
