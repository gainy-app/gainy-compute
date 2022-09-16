from gainy.data_access.repository import Repository
from gainy.trading.drivewealth.models import DriveWealthAuthToken
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthRepository(Repository):

    def get_latest_auth_token(self):
        return self.find_one(DriveWealthAuthToken, None, [("version", "desc")])
