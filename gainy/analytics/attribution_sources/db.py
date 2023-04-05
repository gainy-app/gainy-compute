from psycopg2.extras import RealDictCursor

from gainy.analytics.interfaces import ProfilePropertiesSourceInterface
from gainy.data_access.repository import Repository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DBProfilePropertiesSource(ProfilePropertiesSourceInterface):

    def __init__(self, repository: Repository):
        self.repository = repository

    def get_properties(self, profile_id: int) -> dict:
        with self.repository.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            query = "select * from analytics_profile_properties where profile_id = %(profile_id)s"
            params = {"profile_id": profile_id}
            cursor.execute(query, params)
            return dict(cursor.fetchone())
