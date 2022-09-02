from typing import List

from psycopg2._psycopg import connection

from gainy.data_access.optimistic_lock import AbstractOptimisticLockingFunction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import ProfileRecommendationsMetadata
from gainy.utils import get_logger


class ComputeRecommendationsAndPersist(AbstractOptimisticLockingFunction):

    def __init__(self, repo, profile_id):
        super().__init__(repo)
        self.profile_id = profile_id
        self.logger = get_logger(__name__)

    def load_version(self) -> ProfileRecommendationsMetadata:
        profile_metadata_list = self.repo.find_all(
            ProfileRecommendationsMetadata, {"profile_id": self.profile_id})
        if len(profile_metadata_list) == 0:
            profile_metadata = ProfileRecommendationsMetadata()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        else:
            return profile_metadata_list[0]

    def get_entities(self):
        return []

    def _do_persist(self, entities):
        self.repo.generate_match_scores([self.profile_id])

        top_20_tickers = self.repo.read_top_match_score_tickers(
            self.profile_id, 20)
        self.repo.update_personalized_collection(self.profile_id,
                                                 TOP_20_FOR_YOU_COLLECTION_ID,
                                                 top_20_tickers)
        self.repo.commit()
