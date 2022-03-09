import os
import traceback
from operator import itemgetter
import psycopg2
import sys
from gainy.recommendation.match_score import profile_ticker_similarity
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import MatchScoreModel
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.utils import db_connect, get_logger

logger = get_logger(__name__)


class MatchScoreJob:

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.repo = RecommendationRepository(db_conn)

    def run(self):
        profile_ids = self.repo.read_all_profile_ids()
        for profile_id in profile_ids:
            logger.info(f"Calculate match score for profile: {profile_id}")

            recommendations_func = ComputeRecommendationsAndPersist(
                self.db_conn, profile_id)
            recommendations_func.get_and_persist(self.db_conn, max_tries=3)


def cli(args=None):
    try:
        with db_connect() as db_conn:
            job = MatchScoreJob(db_conn)
            job.run()

    except Exception as e:
        traceback.print_exc()
        raise e
