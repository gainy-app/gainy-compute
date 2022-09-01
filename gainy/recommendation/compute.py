from typing import List

from psycopg2._psycopg import connection

from gainy.data_access.optimistic_lock import AbstractOptimisticLockingFunction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import ProfileRecommendationsMetadata
from gainy.utils import get_logger


def generate_all_match_scores(db_conn, profile_ids: List[int]):
    RecommendationRepository(db_conn).generate_match_scores(profile_ids)


class ComputeRecommendationsAndPersist(AbstractOptimisticLockingFunction):

    def __init__(self, db_conn, profile_id):
        super().__init__(RecommendationRepository(db_conn))
        self.profile_id = profile_id
        self.logger = get_logger(__name__)

    def load_version(self,
                     db_conn: connection) -> ProfileRecommendationsMetadata:
        profile_metadata_list = self.repo.find_all(
            db_conn, ProfileRecommendationsMetadata,
            {"profile_id": self.profile_id})
        if len(profile_metadata_list) == 0:
            profile_metadata = ProfileRecommendationsMetadata()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        else:
            return profile_metadata_list[0]

    def get_entities(self, db_conn: connection):
        return []

    def _do_persist(self, db_conn, entities):
        RecommendationRepository(db_conn).generate_match_scores(
            [self.profile_id])

        top_20_tickers = self.repo.read_top_match_score_tickers(
            self.profile_id, 20)
        self.repo.update_personalized_collection(self.profile_id,
                                                 TOP_20_FOR_YOU_COLLECTION_ID,
                                                 top_20_tickers)
