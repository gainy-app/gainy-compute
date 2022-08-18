import enum
import os
from operator import itemgetter
from typing import List, Tuple, Iterable

from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

from gainy.data_access.repository import Repository
from gainy.recommendation import TOP_20_FOR_YOU_COLLECTION_ID

script_dir = os.path.dirname(__file__)


class RecommendedCollectionAlgorithm(enum.Enum):
    MATCH_SCORE = 0
    TOP_FAVORITED = 1


class RecommendationRepository(Repository):

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def get_recommended_collections(
        self,
        profile_id: int,
        limit: int,
        algorithm:
        RecommendedCollectionAlgorithm = RecommendedCollectionAlgorithm.
        MATCH_SCORE
    ) -> List[Tuple[int, str]]:

        if algorithm == RecommendedCollectionAlgorithm.MATCH_SCORE:
            sorted_collection_match_scores = self.read_sorted_collection_match_scores(
                profile_id, limit)
        elif algorithm == RecommendedCollectionAlgorithm.TOP_FAVORITED:
            sorted_collection_match_scores = self.read_sorted_collection_top_favorited(
                limit)
        else:
            raise Exception('Unsupported algorithm')

        sorted_collections_ids = list(
            map(itemgetter(0), sorted_collection_match_scores))
        sorted_collections_uniq_ids = [
            f"0_{i}" for i in sorted_collections_ids
        ]

        # Add `top-20 for you` collection as the top item
        is_top_20_enabled = self.is_collection_enabled(
            profile_id, TOP_20_FOR_YOU_COLLECTION_ID)
        if is_top_20_enabled:
            sorted_collections_ids = [TOP_20_FOR_YOU_COLLECTION_ID
                                      ] + sorted_collections_ids
            sorted_collections_uniq_ids = [
                f"{profile_id}_{TOP_20_FOR_YOU_COLLECTION_ID}"
            ] + sorted_collections_uniq_ids

        return list(zip(sorted_collections_ids, sorted_collections_uniq_ids))

    def read_batch_profile_ids(self, batch_size: int) -> Iterable[List[int]]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM app.profiles where email not ilike '%test%@gainy.app'"
            )

            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break

                yield list(map(itemgetter(0), batch))

    def read_top_match_score_tickers(self, profile_id: int,
                                     limit: int) -> List[int]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT symbol
                FROM app.profile_ticker_match_score
                join tickers using (symbol)
                where profile_id = %(profile_id)s
                order by match_score desc
                limit %(limit)s
            """, {
                    "profile_id": profile_id,
                    "limit": limit
                })
            return list(map(itemgetter(0), cursor.fetchall()))

    def read_sorted_collection_match_scores(
            self, profile_id: str, limit: int) -> List[Tuple[int, float]]:
        query_filename = os.path.join(script_dir,
                                      "sql/collection_ranking_scores.sql")
        with open(query_filename) as f:
            query = f.read()

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, {"profile_id": profile_id, "limit": limit})

            return list(cursor.fetchall())

    def read_sorted_collection_top_favorited(
            self, limit: int) -> List[Tuple[int, float]]:
        query_filename = os.path.join(script_dir,
                                      "sql/collection_top_favorited.sql")
        with open(query_filename) as f:
            query = f.read()

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, {"limit": limit})

            return list(cursor.fetchall())

    def is_collection_enabled(self, profile_id, collection_id) -> bool:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """SELECT enabled FROM profile_collections
                WHERE (profile_id=%(profile_id)s OR profile_id IS NULL) AND id=%(collection_id)s""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                })

            row = cursor.fetchone()
            return row and int(row[0]) == 1

    # Deprecated
    def read_collection_tickers(self, profile_id: str,
                                collection_id: str) -> List[str]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                """SELECT symbol FROM profile_ticker_collections
                WHERE (profile_id=%(profile_id)s OR profile_id IS NULL) AND collection_id=%(collection_id)s""",
                {
                    "profile_id": profile_id,
                    "collection_id": collection_id
                })

            return list(map(itemgetter(0), cursor.fetchall()))

    # Deprecated
    def read_ticker_match_scores(self, profile_id: str,
                                 symbols: List[str]) -> list:
        _ticker_match_scores_query = """select symbol, match_score, fits_risk, risk_similarity, fits_categories, category_matches, fits_interests, interest_matches, matches_portfolio
        from app.profile_ticker_match_score
        where profile_id = %(profile_id)s and symbol in %(symbols)s;"""

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
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

            if cursor.fetchone() is not None:
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

    def generate_match_scores(self, profile_ids: List[int]):
        query_filename = os.path.join(script_dir,
                                      "sql/generate_match_scores.sql")
        with open(query_filename) as f:
            generate_query = f.read()

        query_filename = os.path.join(script_dir,
                                      "sql/cleanup_match_scores.sql")
        with open(query_filename) as f:
            cleanup_query = f.read()

        where_clause = []
        params = {}
        if profile_ids is not None:
            where_clause.append(sql.SQL("id IN %(profile_ids)s"))
            params['profile_ids'] = tuple(profile_ids)
        else:
            where_clause.append(sql.SQL("email not ilike '%test%@gainy.app'"))

        if where_clause:
            where_clause = sql.SQL('where ') + sql.SQL(' and ').join(
                where_clause)
        else:
            where_clause = sql.SQL('')

        if not params:
            params = None

        generate_query = sql.SQL(generate_query).format(
            where_clause=where_clause)
        cleanup_query = sql.SQL(cleanup_query).format(
            where_clause=where_clause)
        with self.db_conn.cursor() as cursor:
            cursor.execute(generate_query, params)
            cursor.execute(cleanup_query, params)
