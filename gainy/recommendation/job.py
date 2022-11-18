import argparse
import traceback
import time
import json
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID
from gainy.recommendation.repository import RecommendationRepository
from gainy.utils import db_connect, get_logger

logger = get_logger(__name__)


class MatchScoreJob:

    def __init__(self, repo: RecommendationRepository, batch_size: int):
        self.repo = repo
        self.batch_size = batch_size

    def run(self):
        for profile_ids_batch in self.repo.read_batch_profile_ids(
                self.batch_size):
            start_time = time.time()
            self.repo.generate_match_scores(profile_ids_batch)

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
            repo = RecommendationRepository(db_conn)
            job = MatchScoreJob(repo, args.batch_size)
            job.run()

    except Exception as e:
        traceback.print_exc()
        raise e
