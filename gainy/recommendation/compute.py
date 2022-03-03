from typing import Tuple, List

from psycopg2._psycopg import connection

from gainy.data_access.optimistic_lock import AbstractOptimisticLockingFunction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.match_score import MatchScore, profile_ticker_similarity
from gainy.recommendation.models import MatchScoreModel
from gainy.recommendation.repository import RecommendationRepository
from recommendation.models import ProfileRecommendationsMetadata


class ComputeRecommendationsAndPersist(AbstractOptimisticLockingFunction):

    def __init__(self, db_conn, profile_id):
        super().__init__(RecommendationRepository(db_conn))
        self.profile_id = profile_id

    def load_version(self, db_conn: connection):
        profile_metadata_list = self.repo.load(db_conn,
                                               ProfileRecommendationsMetadata,
                                               {"profile_id": self.profile_id})
        if len(profile_metadata_list) == 0:
            profile_metadata = ProfileRecommendationsMetadata()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        else:
            return profile_metadata_list[0]

    def get_entities(self, db_conn: connection):
        tickers_with_match_score = self._get_and_sort_by_match_score()

        return [
            MatchScoreModel(self.profile_id, ticker, match_score)
            for ticker, match_score in tickers_with_match_score
        ]

    def _get_and_sort_by_match_score(self,
                                     top_k: int = None
                                     ) -> List[Tuple[str, MatchScore]]:
        profile_category_v = self.repo.read_profile_category_vector(
            self.profile_id)
        profile_interest_vs = self.repo.read_profile_interest_vectors(
            self.profile_id)

        risk_mapping = self.repo.read_categories_risks()

        ticker_vs_list = self.repo.read_all_ticker_category_and_industry_vectors(
        )

        match_score_list = []
        for ticker_vs in ticker_vs_list:
            match_score = profile_ticker_similarity(profile_category_v,
                                                    ticker_vs[1], risk_mapping,
                                                    profile_interest_vs,
                                                    ticker_vs[0])
            match_score_list.append((ticker_vs[0].name, match_score))

        # Uses minus `match_score` to correctly sort the list by both score and symbol
        match_score_list.sort(key=lambda m: (-m[1].match_score(), m[0]))

        return match_score_list[:top_k] if top_k else match_score_list

    def _do_persist(self, db_conn, entities):
        super()._do_persist(db_conn, entities)

        top_20_tickers = [match_score.symbol for match_score in entities[:20]]
        self.repo.update_personalized_collection(self.profile_id,
                                                 TOP_20_FOR_YOU_COLLECTION_ID,
                                                 top_20_tickers)
