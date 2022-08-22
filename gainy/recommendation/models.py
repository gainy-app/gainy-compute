import json
from gainy.data_access.models import BaseModel, classproperty
from gainy.recommendation.match_score import MatchScore
from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import ResourceVersion


class ProfileRecommendationsMetadata(BaseModel, ResourceVersion):

    profile_id = None
    recommendations_version = None
    updated_at = None

    key_fields = ["profile_id"]

    db_excluded_fields = ["updated_at"]
    non_persistent_fields = ["updated_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profile_recommendations_metadata"

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.PROFILE_RECOMMENDATIONS

    @property
    def resource_id(self) -> int:
        return self.profile_id

    @property
    def resource_version(self):
        return self.recommendations_version

    def update_version(self):
        self.recommendations_version = self.recommendations_version + 1 if self.recommendations_version else 1
