import json

from gainy.data_access.models import BaseModel, classproperty
from gainy.recommendation.match_score import MatchScore


class MatchScoreModel(BaseModel):
    key_fields = ["profile_id", "symbol"]

    db_excluded_fields = ["updated_at"]
    non_persistent_fields = ["updated_at"]

    def __init__(self, profile_id: int, symbol: str, match_score: MatchScore):
        self.profile_id = profile_id
        self.symbol = symbol
        self.match_score = match_score.match_score()

        explanation = match_score.explain()
        self.fits_risk = explanation.risk_level.value
        self.risk_similarity = explanation.risk_similarity
        self.fits_categories = explanation.category_level.value
        self.category_matches = json.dumps(explanation.category_matches)
        self.fits_interests = explanation.interest_level.value
        self.interest_matches = json.dumps(explanation.interest_matches)

        self.updated_at = None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profile_ticker_match_score"
