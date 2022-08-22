import argparse
import os
import traceback
from operator import itemgetter
import psycopg2
from psycopg2._psycopg import connection
import sys
import time
import json
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.compute import generate_all_match_scores
from gainy.utils import db_connect, get_logger

logger = get_logger(__name__)


class MatchScoreJob:

    def __init__(self, db_conn: connection, batch_size: int):
        self.db_conn = db_conn
        self.repo = RecommendationRepository(db_conn)
        self.batch_size = batch_size

    def run(self):
        for profile_ids_batch in self.repo.read_batch_profile_ids(
                self.batch_size):
            start_time = time.time()
            generate_all_match_scores(self.db_conn, profile_ids_batch)

            for profile_id in profile_ids_batch:
                top_20_tickers = self.repo.read_top_match_score_tickers(
                    profile_id, 20)
                self.repo.update_personalized_collection(
                    profile_id, TOP_20_FOR_YOU_COLLECTION_ID, top_20_tickers)

            logger.info("Calculated match score profiles %s in %f",
                        json.dumps(profile_ids_batch),
                        time.time() - start_time)


def cli(args=None):
    parser = argparse.ArgumentParser(
        description='Update recommendations for all profiles.')
    parser.add_argument('--batch_size', dest='batch_size', type=int)
    args = parser.parse_args(args)

    try:
        with db_connect() as db_conn:
            job = MatchScoreJob(db_conn, args.batch_size)
            job.run()

    except Exception as e:
        traceback.print_exc()
        raise e
