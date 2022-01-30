import json
import os
from operator import itemgetter
from typing import List, Tuple, Dict

from psycopg2.extras import execute_values

from common.hasura_exception import HasuraActionException
from data_access.repository import Repository
from recommendation.core import DimVector
from recommendation.match_score import MatchScore

script_dir = os.path.dirname(__file__)


class RecommendationRepository(Repository):

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def read_categories_risks(self) -> Dict[str, int]:
        cursor = self.db_conn.cursor()
        cursor.execute(
            "SELECT id::varchar, risk_score from public.categories WHERE risk_score IS NOT NULL;"
        )
        return dict(cursor.fetchall())

    def read_profile_category_vector(self, profile_id) -> DimVector:
        with open(os.path.join(script_dir, "sql/profile_categories.sql")
                  ) as _profile_category_vector_query_file:
            _profile_category_vector_query = _profile_category_vector_query_file.read(
            )

        vectors = self._query_vectors(_profile_category_vector_query,
                                      {"profile_id": profile_id})
        if not vectors:
            raise HasuraActionException(400, f"Profile {profile_id} not found")
        return vectors[0]

    def read_profile_interest_vectors(self, profile_id) -> List[DimVector]:
        with open(os.path.join(script_dir, "sql/profile_interests.sql")
                  ) as _profile_interest_vectors_query_file:
            _profile_interest_vectors_query = _profile_interest_vectors_query_file.read(
            )

        vectors = self._query_vectors(_profile_interest_vectors_query,
                                      {"profile_id": profile_id})
        if not vectors:
            raise HasuraActionException(400, f"Missing profile `{profile_id}`")

        return vectors

    def read_all_ticker_category_and_industry_vectors(
            self) -> list[(DimVector, DimVector)]:

        with open(
                os.path.join(script_dir, "sql/ticker_categories_industries.sql"
                             )) as _ticker_categories_industries_query_file:
            _ticker_categories_industries_query = _ticker_categories_industries_query_file.read(
            )

        cursor = self.db_conn.cursor()
        cursor.execute(_ticker_categories_industries_query)

        return [(DimVector(row[0], row[1]), DimVector(row[0], row[2]))
                for row in cursor.fetchall()]

    def _query_vectors(self, query, variables=None) -> List[DimVector]:
        cursor = self.db_conn.cursor()
        cursor.execute(query, variables)

        vectors = []
        for row in cursor.fetchall():
            vectors.append(DimVector(row[0], row[1]))

        return vectors

    def read_sorted_collection_match_scores(
            self, profile_id: str, limit: int) -> List[Tuple[int, float]]:
        with open(os.path.join(script_dir, "sql/collection_ranking_scores.sql")
                  ) as _collection_ranking_scores_query_file:
            _collection_ranking_scores_query = _collection_ranking_scores_query_file.read(
            )

        with self.db_conn.cursor() as cursor:
            cursor.execute(_collection_ranking_scores_query, {
                "profile_id": profile_id,
                "limit": limit
            })

            return list(cursor.fetchall())

    # Deprecated
    def read_collection_tickers(self, profile_id: str,
                                collection_id: str) -> List[str]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """SELECT symbol FROM public.profile_ticker_collections 
                WHERE (profile_id=%(profile_id)s OR profile_id IS NULL) AND collection_id=%(collection_id)s""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                })

            return list(map(itemgetter(0), cursor.fetchall()))

    # Deprecated
    def read_ticker_match_scores(self, profile_id: str,
                                 symbols: List[str]) -> list:
        _ticker_match_scores_query = """select symbol, match_score, fits_risk, risk_similarity, fits_categories, category_matches, fits_interests, interest_matches
        from app.profile_ticker_match_score
        where profile_id = %(profile_id)s and symbol in %(symbols)s;"""

        with self.db_conn.cursor() as cursor:
            cursor.execute(_ticker_match_scores_query, {
                "profile_id": profile_id,
                "symbols": tuple(symbols)
            })

            return list(cursor.fetchall())

    def update_personalized_collection(self, profile_id, collection_id,
                                       ticker_list):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """SELECT profile_id, collection_id FROM app.personalized_collection_sizes 
                 WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s FOR UPDATE""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                })

            if not cursor.fetchone() is None:
                cursor.execute(
                    """DELETE FROM app.personalized_ticker_collections 
                    WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                    {
                        "profile_id": profile_id,
                        "collection_id": collection_id
                    })

                cursor.execute(
                    """UPDATE app.personalized_collection_sizes SET size = %(size)s
                    WHERE profile_id = %(profile_id)s AND collection_id = %(collection_id)s""",
                    {
                        "profile_id": profile_id,
                        "collection_id": collection_id,
                        "size": len(ticker_list)
                    })
            else:
                cursor.execute(
                    "INSERT INTO app.personalized_collection_sizes(profile_id, collection_id, size) "
                    "VALUES (%(profile_id)s, %(collection_id)s, %(size)s)", {
                        "profile_id": profile_id,
                        "collection_id": collection_id,
                        "size": len(ticker_list)
                    })

            execute_values(
                cursor,
                "INSERT INTO app.personalized_ticker_collections(profile_id, collection_id, symbol) VALUES %s",
                [(profile_id, collection_id, symbol)
                 for symbol in ticker_list])
