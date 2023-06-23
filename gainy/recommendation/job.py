import argparse
import traceback
import time
import json
from gainy.recommendation.repository import RecommendationRepository
from gainy.utils import db_connect, get_logger

logger = get_logger(__name__)


def split_in_chunks(lst, batch_size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


class MatchScoreJob:

    def __init__(self, repo: RecommendationRepository, batch_size: int):
        self.repo = repo
        self.batch_size = batch_size

    def run(self):
        tickers_to_update = self.repo.get_tickers_to_update_ms()
        profiles_to_update = self.repo.get_profiles_to_update_ms()

        self._calculate_for_tickers(tickers_to_update)
        self._calculate_for_profiles(profiles_to_update)
        self._calculate_for_collections()

        self.repo.save_tickers_state()
        self.repo.save_profiles_state()

    def _calculate_for_tickers(self, tickers):
        if not tickers:
            return

        for profile_ids_batch in self.repo.read_ms_batch_profile_ids(
                self.batch_size):
            start_time = time.time()
            self.repo.generate_ticker_match_scores(profile_ids_batch,
                                                   tickers=tickers)

            logger.info(
                "Calculated ticker match scores for tickers %s and profiles %s in %f",
                json.dumps(tickers), json.dumps(profile_ids_batch),
                time.time() - start_time)

    def _calculate_for_collections(self):
        for profile_ids_batch in self.repo.read_ms_batch_profile_ids(
                self.batch_size):
            start_time = time.time()
            self.repo.generate_collection_match_scores(profile_ids_batch)

            logger.info(
                "Calculated collection match scores for profiles %s in %f",
                json.dumps(profile_ids_batch),
                time.time() - start_time)

    def _calculate_for_profiles(self, profile_ids):
        if not profile_ids:
            return

        for profile_ids_batch in split_in_chunks(profile_ids, self.batch_size):
            start_time = time.time()
            self.repo.generate_ticker_match_scores(profile_ids_batch)

            logger.info("Calculated ticker match scores for profiles %s in %f",
                        json.dumps(profile_ids_batch),
                        time.time() - start_time)


def cli(args=None):
    parser = argparse.ArgumentParser(
        description='Update recommendations for all profiles.')
    parser.add_argument('--batch_size',
                        dest='batch_size',
                        type=int,
                        default=15)
    args = parser.parse_args(args)

    try:
        with db_connect() as db_conn:
            repo = RecommendationRepository(db_conn)
            job = MatchScoreJob(repo, args.batch_size)
            job.run()

    except Exception as e:
        traceback.print_exc()
        raise e
