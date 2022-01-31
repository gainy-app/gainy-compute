import logging
import os
from operator import itemgetter

import psycopg2

from gainy.recommendation.match_score import profile_ticker_similarity
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.models import MatchScoreModel
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID


class MatchScoreJob:

    def __init__(
            self,
            db_host=None,
            db_port=None,
            db_user=None,
            db_password=None,
            db_name=None
    ):
        db_host = os.getenv("PG_ADDRESS") if not db_host else db_host
        db_port = os.getenv("PG_PORT") if not db_port else db_port
        db_user = os.getenv("PG_USERNAME") if not db_user else db_user
        db_password = os.getenv("PG_PASSWORD") if not db_password else db_password
        db_name = os.getenv("PG_DATABASE") if not db_name else db_name

        self.db_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    def run(self):
        with psycopg2.connect(self.db_string) as db_conn:
            repo = RecommendationRepository(db_conn=db_conn)

            ticker_vectors_list = repo.read_all_ticker_category_and_industry_vectors()
            categories_risks = repo.read_categories_risks()

            profile_ids = repo.read_all_profile_ids()
            for profile_id in profile_ids:
                logging.info(f"Calculate match score for profile: {profile_id}")

                profile_category_vector = repo.read_profile_category_vector(profile_id)
                profile_interest_vectors = repo.read_profile_interest_vectors(profile_id)

                tickers_with_match_score = []
                for ticker_vectors in ticker_vectors_list:
                    match_score = profile_ticker_similarity(
                        profile_category_vector,
                        ticker_vectors[1],
                        categories_risks,
                        profile_interest_vectors,
                        ticker_vectors[0]
                    )

                    tickers_with_match_score.append((ticker_vectors[0].name, match_score))

                logging.debug(f"Save {len(tickers_with_match_score)} match scores for profile: {profile_id}")
                match_score_entities = [
                    MatchScoreModel(profile_id, ticker, match_score)
                    for ticker, match_score in tickers_with_match_score
                ]
                repo.persist(repo.db_conn, match_score_entities)

                logging.debug(f"Save {len(tickers_with_match_score)} top-20 collection for profile: {profile_id}")
                # Uses minus `match_score` to correctly sort the list by both score and symbol
                tickers_with_match_score.sort(key=lambda m: (-m[1].match_score(), m[0]))
                top_20_tickers = list(map(itemgetter(0), tickers_with_match_score[:20]))
                repo.update_personalized_collection(profile_id, TOP_20_FOR_YOU_COLLECTION_ID, top_20_tickers)
