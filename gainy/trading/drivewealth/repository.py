from psycopg2.extras import RealDictCursor

from typing import List, Iterable, Optional

from gainy.data_access.operators import OperatorGt
from gainy.data_access.repository import Repository
from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthUser, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthInstrument, DriveWealthTransaction
from gainy.trading.models import TradingOrderStatus
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

    def portfolio_has_pending_orders(self,
                                     portfolio: DriveWealthPortfolio) -> bool:
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

    def get_new_transactions(
            self, account_id: str, last_transaction_id: Optional[int]
    ) -> list[DriveWealthTransaction]:

        params = {
            "account_id": account_id,
        }
        if last_transaction_id:
            params["id"] = OperatorGt(last_transaction_id)

        return self.find_all(DriveWealthTransaction, params)
