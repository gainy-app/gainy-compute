import datetime
from typing import List, Optional

from gainy.data_access.operators import OperatorGt
from gainy.exceptions import KYCFormHasNotBeenSentException
from gainy.trading.drivewealth import DriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthUser, DriveWealthPortfolio, DriveWealthPortfolioStatus, \
    DriveWealthFund, DriveWealthInstrument
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

DRIVE_WEALTH_PORTFOLIO_STATUS_TTL = 300  # in seconds


class DriveWealthProviderBase:
    repository: DriveWealthRepository = None

    def __init__(self, repository: DriveWealthRepository, api: DriveWealthApi):
        self.repository = repository
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
        self.repository.persist(portfolio)

    def sync_portfolio_status(self, portfolio: DriveWealthPortfolio):
        portfolio_status = self._get_portfolio_status(portfolio)
        portfolio.update_from_status(portfolio_status)
        self.repository.persist(portfolio)
        return portfolio_status

    def get_fund(self, profile_id: int,
                 collection_id: int) -> Optional[DriveWealthFund]:
        repository = self.repository
        fund = repository.get_profile_fund(profile_id, collection_id)

        if not fund:
            return None

        return fund

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
