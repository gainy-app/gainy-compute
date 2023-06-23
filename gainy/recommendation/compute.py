from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.recommendation.models import ProfileRecommendationsMetadata
from gainy.utils import get_logger


class ComputeRecommendationsAndPersist(AbstractPessimisticLockingFunction):

    def __init__(self, repo, profile_id):
        super().__init__(repo)
        self.profile_id = profile_id
        self.logger = get_logger(__name__)

    def load_version(self) -> ProfileRecommendationsMetadata:
        profile_metadata = self.repo.find_one(
            ProfileRecommendationsMetadata, {"profile_id": self.profile_id},
            [("recommendations_version", "desc")])

        if not profile_metadata:
            profile_metadata = ProfileRecommendationsMetadata()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        return profile_metadata

    def _do(self, version):
        self.repo.generate_ticker_match_scores([self.profile_id])
        self.repo.generate_collection_match_scores([self.profile_id])

        # top_20_tickers = self.repo.read_top_match_score_tickers(
        #     self.profile_id, 20)
        # self.repo.update_personalized_collection(self.profile_id,
        #                                          TOP_20_FOR_YOU_COLLECTION_ID,
        #                                          top_20_tickers)
        self.repo.commit()
