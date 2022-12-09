import datetime
from typing import List, Optional, Iterable

from gainy.data_access.operators import OperatorGt
from gainy.exceptions import KYCFormHasNotBeenSentException
from gainy.trading.drivewealth import DriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthPortfolio, DriveWealthPortfolioStatus, \
    DriveWealthFund, DriveWealthInstrument, DriveWealthAccount
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.trading.models import TradingCollectionVersionStatus, TradingAccount
from gainy.trading.repository import TradingRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

DRIVE_WEALTH_PORTFOLIO_STATUS_TTL = 300  # in seconds


class DriveWealthProviderBase:
    repository: DriveWealthRepository = None
    trading_repository: TradingRepository = None

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi,
                 trading_repository: TradingRepository):
        self.repository = repository
        self.trading_repository = trading_repository
        self.api = api

    def sync_portfolios(self, profile_id):
        repository = self.repository

        portfolios: List[DriveWealthPortfolio] = repository.find_all(
            DriveWealthPortfolio, {"profile_id": profile_id})
        for portfolio in portfolios:
            if portfolio.is_artificial:
                return
            self.sync_portfolio(portfolio)
            self.sync_portfolio_status(portfolio)

    def sync_portfolio(self, portfolio: DriveWealthPortfolio):
        data = self.api.get_portfolio(portfolio)
        portfolio.set_from_response(data)
        portfolio.last_sync_at = datetime.datetime.now()
        self.repository.persist(portfolio)

    def sync_portfolio_status(self, portfolio: DriveWealthPortfolio):
        portfolio_status = self._get_portfolio_status(portfolio)
        portfolio.update_from_status(portfolio_status)
        self.repository.persist(portfolio)
        self.update_trading_collection_versions_pending_execution_from_portfolio_status(
            portfolio_status)
        return portfolio_status

    def update_trading_collection_versions_pending_execution_from_portfolio_status(
            self, portfolio_status: DriveWealthPortfolioStatus):
        if portfolio_status.last_portfolio_rebalance_at is None:
            return

        portfolio: DriveWealthPortfolio = self.repository.find_one(
            DriveWealthPortfolio,
            {"ref_id": portfolio_status.drivewealth_portfolio_id})
        if not portfolio or not portfolio.drivewealth_account_id:
            return

        dw_account: DriveWealthAccount = self.repository.find_one(
            DriveWealthAccount, {"ref_id": portfolio.drivewealth_account_id})
        if not dw_account or not dw_account.trading_account_id:
            return

        trading_account: TradingAccount = self.repository.find_one(
            TradingAccount, {"id": dw_account.trading_account_id})
        if not trading_account:
            return

        for trading_collection_version in self.trading_repository.iterate_trading_collection_versions(
                profile_id=portfolio.profile_id,
                trading_account_id=trading_account.id,
                status=TradingCollectionVersionStatus.PENDING_EXECUTION,
                pending_execution_to=portfolio_status.
                last_portfolio_rebalance_at):

            if trading_collection_version.pending_execution_since and trading_collection_version.pending_execution_since >= portfolio_status.last_portfolio_rebalance_at:
                raise Exception(
                    'Trying to update trading_collection_version %d, which is pending execution since %s, '
                    'with latest rebalance happened on %s' %
                    (trading_collection_version.id,
                     trading_collection_version.pending_execution_since,
                     portfolio_status.last_portfolio_rebalance_at))
            trading_collection_version.status = TradingCollectionVersionStatus.EXECUTED_FULLY
            self.repository.persist(trading_collection_version)

    def get_fund(self, profile_id: int,
                 collection_id: int) -> Optional[DriveWealthFund]:
        repository = self.repository
        fund = repository.get_profile_fund(profile_id, collection_id)

        if not fund:
            return None

        return fund

    def iterate_profile_funds(self,
                              profile_id: int) -> Iterable[DriveWealthFund]:
        yield from self.repository.iterate_all(DriveWealthFund, {
            "profile_id": profile_id,
        })

    def sync_instrument(self, ref_id: str = None, symbol: str = None):
        data = self.api.get_instrument_details(ref_id=ref_id, symbol=symbol)
        instrument = DriveWealthInstrument()
        instrument.set_from_response(data)
        self.repository.persist(instrument)
        return instrument

    def _get_portfolio_status(
            self,
            portfolio: DriveWealthPortfolio) -> DriveWealthPortfolioStatus:

        portfolio_status: DriveWealthPortfolioStatus = self.repository.find_one(
            DriveWealthPortfolioStatus, {
                "drivewealth_portfolio_id":
                portfolio.ref_id,
                "created_at":
                OperatorGt(
                    datetime.datetime.now(datetime.timezone.utc) -
                    datetime.timedelta(
                        seconds=DRIVE_WEALTH_PORTFOLIO_STATUS_TTL)),
            }, [("created_at", "DESC")])

        if portfolio_status:
            return portfolio_status

        data = self.api.get_portfolio_status(portfolio)
        portfolio_status = DriveWealthPortfolioStatus()
        portfolio_status.set_from_response(data)
        self.repository.persist(portfolio_status)
        return portfolio_status

    def _get_user(self, profile_id) -> DriveWealthUser:
        repository = self.repository
        user = repository.get_user(profile_id)
        if user is None:
            raise KYCFormHasNotBeenSentException("KYC form has not been sent")
        return user
