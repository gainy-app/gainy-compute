from psycopg2.extras import RealDictCursor

from typing import List, Optional, Any, Dict

from gainy.data_access.operators import OperatorGt, OperatorIn
from gainy.data_access.repository import Repository
from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthUser, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolio, DriveWealthInstrumentStatus, DriveWealthInstrument, DriveWealthTransaction, \
    DriveWealthRedemption, DriveWealthRedemptionStatus, DriveWealthTransactionInterface
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
        """
        :raises EntityNotFoundException:
        """
        query = """
            select ref_id 
            from app.drivewealth_instruments 
            where public.normalize_drivewealth_symbol(symbol) = %(symbol)s 
              and status = %(status)s"""
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "symbol": symbol,
                    "status": DriveWealthInstrumentStatus.ACTIVE.name
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
    ) -> list[DriveWealthTransactionInterface]:

        params = {
            "account_id": account_id,
        }
        if last_transaction_id:
            params["id"] = OperatorGt(last_transaction_id)

        transactions = self.find_all(DriveWealthTransaction, params)
        transactions = [
            DriveWealthTransaction.create_typed_transaction(tx)
            for tx in transactions
        ]
        return transactions

    def get_pending_redemptions(
            self, account_id: str) -> list[DriveWealthRedemption]:
        params = {
            "trading_account_ref_id":
            account_id,
            "status":
            OperatorIn([
                DriveWealthRedemptionStatus.Approved.name,
                DriveWealthRedemptionStatus.RIA_Approved.name
            ])
        }

        return self.find_all(DriveWealthRedemption, params)

    def symbol_is_in_collection(self, symbol: str) -> bool:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                '''select exists(select ttf_name 
                                 from raw_data.ticker_collections_weights
                                 where symbol = %(symbol)s
                                   and _sdc_extracted_at > (select max(_sdc_extracted_at) from raw_data.ticker_collections_weights) - interval '1 hour') 
                ''', {"symbol": symbol})
            return cursor.fetchone()[0]

    def filter_inactive_symbols_from_weights(
            self, weights: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        symbols = [i["symbol"] for i in weights]
        tradeable_symbols = []
        for symbol in symbols:
            try:
                self.get_instrument_by_symbol(symbol)
                tradeable_symbols.append(symbol)
            except EntityNotFoundException:
                pass
        return [i for i in weights if i["symbol"] in tradeable_symbols]
