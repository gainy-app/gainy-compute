from typing import Tuple, List, Iterable

from psycopg2._psycopg import connection
import numpy as np
import time

from gainy.data_access.optimistic_lock import AbstractOptimisticLockingFunction
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.match_score import MatchScore, profile_ticker_similarity
from gainy.recommendation.models import MatchScoreModel
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import ProfileRecommendationsMetadata
from gainy.utils import db_connect, get_logger

MS_EXPL_CATEGORIES_MAX_COUNT = 2
MS_EXPL_INTERESTS_MAX_COUNT = 2


def generate_match_scores(db_conn, profile_id=None):
    RecommendationRepository(db_conn).generate_match_scores(profile_id)


class ComputeRecommendationsAndPersist(AbstractOptimisticLockingFunction):

    def __init__(self, db_conn, profile_id, long_term_cache={}):
        super().__init__(RecommendationRepository(db_conn))
        self.profile_id = profile_id
        self.logger = get_logger(__name__)
        self.long_term_cache = long_term_cache

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
        []

    def _do_persist(self, db_conn, entities):
        RecommendationRepository(db_conn).generate_match_scores(
            self.profile_id)

        top_20_tickers = self.repo.read_top_match_score_tickers()
        self.repo.update_personalized_collection(self.profile_id,
                                                 TOP_20_FOR_YOU_COLLECTION_ID,
                                                 top_20_tickers)
