import os
from operator import itemgetter
from typing import List, Tuple

from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

from gainy.data_access.repository import Repository

script_dir = os.path.dirname(__file__)


class RecommendationRepository(Repository):

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def read_batch_profile_ids(self, batch_size: int) -> List[int]:
        with self.db_conn.cursor() as cursor:
            cursor.execute("SELECT id FROM app.profiles")

            while True:
                batch = cursor.fetchmany()
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

    def generate_match_scores(self, profile_ids: List[int]):
        query_filename = os.path.join(script_dir,
                                      "sql/generate_match_scores.sql")
        with open(query_filename) as f:
            query = f.read()

        where_clause = []
        params = {}
        if profile_ids is not None:
            where_clause.append(sql.SQL("profile_id IN (%(profile_ids)s)"))
            params['profile_ids'] = tuple(profile_ids)

        if where_clause:
            where_clause = sql.SQL('where ') + sql.SQL(' and ').join(
                where_clause)
        else:
            where_clause = sql.SQL('')

        query = sql.SQL(query).format(where_clause=where_clause)

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)
