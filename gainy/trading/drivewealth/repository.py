from typing import List, Iterable

from gainy.data_access.repository import Repository
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthUser, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolio
from gainy.trading.models import TradingMoneyFlowStatus, TradingCollectionVersionStatus
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthRepository(Repository):

    def get_user(self, profile_id) -> DriveWealthUser:
        return self.find_one(DriveWealthUser, {"profile_id": profile_id})

    def get_latest_auth_token(self):
        return self.find_one(DriveWealthAuthToken, None, [("version", "desc")])

    def get_user_accounts(self,
                          drivewealth_user_id) -> List[DriveWealthAccount]:
        return self.find_all(DriveWealthAccount,
                             {"drivewealth_user_id": drivewealth_user_id})

    def get_profile_fund(self, profile_id: int,
                         collection_id) -> DriveWealthFund:
        return self.find_one(DriveWealthFund, {
            "profile_id": profile_id,
            "collection_id": collection_id,
        })

    def get_profile_portfolio(self, profile_id: int) -> DriveWealthPortfolio:
        return self.find_one(DriveWealthPortfolio, {"profile_id": profile_id})

    def upsert_user_account(self, drivewealth_user_id,
                            data) -> DriveWealthAccount:
        entity = DriveWealthAccount()
        entity.drivewealth_user_id = drivewealth_user_id
        entity.set_from_response(data)

        self.persist(entity)

        return entity

    def calculate_portfolio_cash_target_value(self,
                                              portfolio: DriveWealthPortfolio):
        query = """
            select sum(amount)
            from (
                     select amount
                     from app.trading_money_flow
                     where profile_id = %(profile_id)s
                       and status in %(trading_money_flow_statuses)s
            
                     union all
            
                     select -target_amount_delta as amount
                     from app.trading_collection_versions
                     where profile_id = %(profile_id)s
                       and status in %(trading_collection_version_statuses)s
                 ) t
        """
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "profile_id":
                    portfolio.profile_id,
                    "trading_money_flow_statuses":
                    (TradingMoneyFlowStatus.SUCCESS),
                    "trading_collection_version_statuses":
                    (TradingCollectionVersionStatus.PENDING_EXECUTION,
                     TradingCollectionVersionStatus.EXECUTED_FULLY),
                })
            row = cursor.fetch_one()

            if row and row[0]:
                cash_target_value = row[0]
            else:
                cash_target_value = 0

        portfolio.cash_target_value = cash_target_value
        self.persist(portfolio)

    def iterate_profile_portfolios(
            self, profile_id: int) -> Iterable[DriveWealthPortfolio]:
        yield from self.iterate_all(DriveWealthPortfolio,
                                    {"profile_id": profile_id})

    def iterate_profiles_with_portfolio(self) -> Iterable[int]:
        query = "select distinct profile_id from app.drivewealth_portfolios"
        with self.db_conn.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                yield row[0]

    def iterate_profiles_with_pending_trading_collection_versions(
            self) -> Iterable[int]:
        query = "select distinct profile_id from app.trading_collection_versions where status = %(status)s"
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                query, {"status": TradingCollectionVersionStatus.PENDING.name})
            for row in cursor:
                yield row[0]
