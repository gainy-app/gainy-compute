import os
import traceback
from operator import itemgetter
import psycopg2
import sys
import time
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.compute import generate_match_scores
from gainy.recommendation.models import MatchScoreModel
from gainy.utils import db_connect, get_logger

logger = get_logger(__name__)


class MatchScoreJob:

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.repo = RecommendationRepository(db_conn)

    def run(self):
        start_time = time.time()

        #         generate_match_scores(self.db_conn)

        profile_ids = self.repo.read_all_profile_ids()
        for profile_id in profile_ids:
            top_20_tickers = self.repo.read_top_match_score_tickers(
                profile_id, 20)
            self.repo.update_personalized_collection(
                profile_id, TOP_20_FOR_YOU_COLLECTION_ID, top_20_tickers)

        logger.info("Calculated match score in %f", time.time() - start_time)


def cli(args=None):
    try:
        with db_connect() as db_conn:
            job = MatchScoreJob(db_conn)
            job.run()

    except Exception as e:
        traceback.print_exc()
        raise e
