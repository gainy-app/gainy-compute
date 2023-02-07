from psycopg2.extras import RealDictCursor

from typing import List, Iterable, Optional

from gainy.data_access.repository import Repository
from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthUser, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthInstrument
from gainy.trading.models import TradingMoneyFlowStatus, TradingOrderStatus
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

    def get_profile_fund(self,
                         profile_id: int,
                         collection_id: int = None,
                         symbol: str = None) -> DriveWealthFund:
        params = {
            "profile_id": profile_id,
        }

        if collection_id:
            params["collection_id"] = collection_id
        if symbol:
            params["symbol"] = symbol

        return self.find_one(DriveWealthFund, params)

    def get_account(self, trading_account_id=None) -> DriveWealthAccount:
        account = self.find_one(DriveWealthAccount,
                                {"trading_account_id": trading_account_id})
        if not account:
            raise Exception("Could not link portfolio to account: no account")

        return account

    def get_profile_portfolio(self, profile_id: int,
                              trading_account_id: int) -> DriveWealthPortfolio:
        account = self.get_account(trading_account_id)
        if not account:
            raise EntityNotFoundException(DriveWealthFund)

        params = {
            "profile_id": profile_id,
            "drivewealth_account_id": account.ref_id,
        }

        return self.find_one(DriveWealthPortfolio, params)

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
                     select amount + coalesce(fees_total_amount, 0) as amount
                     from app.trading_money_flow
                     where profile_id = %(profile_id)s
                       and status in %(trading_money_flow_statuses)s
            
                     union all
            
                     select -target_amount_delta as amount
                     from app.trading_collection_versions
                     where profile_id = %(profile_id)s
                       and status in %(trading_collection_version_statuses)s
            
                     union all
            
                     select -target_amount_delta as amount
                     from app.trading_orders
                     where profile_id = %(profile_id)s
                       and status in %(trading_order_statuses)s
                 ) t
        """

        trading_money_flow_statuses = [
            TradingMoneyFlowStatus.SUCCESS.name,
            TradingMoneyFlowStatus.APPROVED.name,
        ]

        trading_collection_version_statuses = (
            TradingOrderStatus.PENDING_EXECUTION.name,
            TradingOrderStatus.EXECUTED_FULLY.name,
        )

        trading_order_statuses = (
            TradingOrderStatus.PENDING_EXECUTION.name,
            TradingOrderStatus.EXECUTED_FULLY.name,
        )

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "profile_id":
                    portfolio.profile_id,
                    "trading_money_flow_statuses":
                    tuple(trading_money_flow_statuses),
                    "trading_collection_version_statuses":
                    trading_collection_version_statuses,
                    "trading_order_statuses":
                    trading_order_statuses,
                })
            row = cursor.fetchone()

            if row and row[0]:
                cash_target_value = row[0]
            else:
                cash_target_value = 0

        portfolio.cash_target_value = cash_target_value
        self.persist(portfolio)

    # todo add to tests?
    def get_instrument_by_symbol(
            self, symbol: str) -> Optional[DriveWealthInstrument]:
        query = """
            select ref_id 
            from app.drivewealth_instruments 
            where public.normalize_drivewealth_symbol(symbol) = %(symbol)s 
              and status = %(status)s"""
        with self.db_conn.cursor() as cursor:
            cursor.execute(query, {
                "symbol": symbol,
                "status": DriveWealthInstrumentStatus.ACTIVE
            })
            row = cursor.fetchone()
            if not row:
                raise EntityNotFoundException(DriveWealthInstrument)

            instrument = self.find_one(DriveWealthInstrument, {
                "ref_id": row[0],
            })

        if instrument:
            return instrument

        raise EntityNotFoundException(DriveWealthInstrument)

    def iterate_portfolios_to_sync(self) -> Iterable[DriveWealthPortfolio]:
        # allow resync if last order was executed up to a minute before last sync
        query = """
            select *
            from app.drivewealth_portfolios 
            where last_sync_at is null or last_order_executed_at > last_sync_at - interval '1 minute'"""

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)

            for row in cursor:
                entity = DriveWealthPortfolio()
                entity.set_from_dict(row)
                yield entity

    def is_portfolio_pending_rebalance(
            self, portfolio: DriveWealthPortfolio) -> bool:
        query = """
            select exists(
                           select trading_collection_versions.id
                           from app.trading_collection_versions
                                    join app.drivewealth_accounts using (trading_account_id)
                           where drivewealth_accounts.ref_id = %(drivewealth_account_id)s
                             and trading_collection_versions.status = %(status)s
                       ) or exists(
                           select trading_orders.id
                           from app.trading_orders
                                    join app.drivewealth_accounts using (trading_account_id)
                           where drivewealth_accounts.ref_id = %(drivewealth_account_id)s
                             and trading_orders.status = %(status)s
                       )
            """

        with self.db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "drivewealth_account_id": portfolio.drivewealth_account_id,
                    "status": TradingOrderStatus.PENDING_EXECUTION.name
                })
            row = cursor.fetchone()
        return row[0]
