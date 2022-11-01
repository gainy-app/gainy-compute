from typing import List

from gainy.data_access.repository import Repository
from gainy.trading.drivewealth.models import DriveWealthAuthToken, DriveWealthUser, DriveWealthAccount, DriveWealthFund, \
    DriveWealthPortfolio
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
