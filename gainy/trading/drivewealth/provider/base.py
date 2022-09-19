from gainy.trading.drivewealth.models import DriveWealthUser
from gainy.trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderBase:
    repository: DriveWealthRepository = None

    def __init__(self, repository: DriveWealthRepository):
        self.repository = repository

    def _get_user(self, profile_id) -> DriveWealthUser:
        repository = self.repository
        user = repository.get_user(profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")
        return user
