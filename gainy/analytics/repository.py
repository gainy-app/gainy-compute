from gainy.analytics.exceptions import AnalyticsMetadataNotFound
from gainy.data_access.repository import Repository

ANALYTICS_METADATA_SERVICE_FIREBASE = 'FIREBASE'


class AnalyticsRepository(Repository):

    def get_analytics_metadata(self, profile_id: int, service_name: str):
        with self.db_conn.cursor() as cursor:
            query = "select metadata from app.analytics_profile_data where profile_id = %(profile_id)s and service_name = %(service_name)s"
            params = {"profile_id": profile_id, "service_name": service_name}
            cursor.execute(query, params)

            row = cursor.fetchone()

        if row and row[0]:
            return row[0]

        raise AnalyticsMetadataNotFound()
