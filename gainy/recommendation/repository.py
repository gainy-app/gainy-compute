import os
from operator import itemgetter
from typing import List, Tuple, Dict

import pandas as pd
from psycopg2.extras import execute_values

from gainy.data_access.exceptions import ObjectNotFoundException
from gainy.data_access.repository import Repository
from gainy.recommendation.core import DimVector
from gainy.utils import get_db_connection_string

script_dir = os.path.dirname(__file__)


class RecommendationRepository(Repository):

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.db_conn_string = get_db_connection_string()

    def read_all_profile_ids(self) -> List[int]:
        with self.db_conn.cursor() as cursor:
            cursor.execute("SELECT id::int4 FROM app.profiles;")
            return list(map(itemgetter(0), cursor.fetchall()))

    def read_categories_risks(self) -> Dict[str, int]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT id::varchar, risk_score from categories WHERE risk_score IS NOT NULL;"
            )
            return dict(cursor.fetchall())

    def read_profile_category_vector(self, profile_id) -> DimVector:
        query_filename = os.path.join(script_dir, "sql/profile_categories.sql")
        with open(query_filename) as f:
            query = f.read()

        vectors = self._query_vectors(query, {"profile_id": profile_id})
        if not vectors:
            return None

        return vectors[0]

    def read_profile_interest_vectors(self, profile_id) -> List[DimVector]:
        query_filename = os.path.join(script_dir, "sql/profile_interests.sql")
        with open(query_filename) as f:
            query = f.read()

        vectors = self._query_vectors(query, {"profile_id": profile_id})

        return vectors

    def read_all_ticker_category_and_industry_vectors(
            self) -> List[Tuple[DimVector, DimVector]]:

        query_filename = os.path.join(script_dir,
                                      "sql/ticker_categories_industries.sql")
        with open(query_filename) as f:
            query = f.read()

        with self.db_conn.cursor() as cursor:
            cursor.execute(query)

            # symbol, ticker_industry_vector, ticker_category_vector
            return [(DimVector(row[0], row[1]), DimVector(row[0], row[2]))
                    for row in cursor.fetchall()]

    def _query_vectors(self, query, variables=None) -> List[DimVector]:
        with self.db_conn.cursor() as cursor:
            cursor.execute(query, variables)

            vectors = []
            for row in cursor.fetchall():
                vectors.append(DimVector(row[0], row[1]))

        return vectors

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

    def get_df_ticker_symbols(self):
        sql_tickersymbols = "select symbol from tickers where type ilike 'common stock'"
        return pd.read_sql_query(sql_tickersymbols, self.db_conn_string)

    def get_df_profile_categories(self, profile_id):
        query = """
            select profile_id,
                   category_id
            from app.profile_categories
            where profile_id = %(profile_id)s
        """
        return pd.read_sql_query(query,
                                 self.db_conn_string,
                                 params={"profile_id": profile_id})

    def get_df_profile_interests(self, profile_id):
        query = """
            select profile_id,
                   interest_id
            from app.profile_interests
            where profile_id = %(profile_id)s
        """
        return pd.read_sql_query(query,
                                 self.db_conn_string,
                                 params={"profile_id": profile_id})

    def get_df_profile_scoring_settings(self, profile_id):
        query = """
            select *
            from app.profile_scoring_settings
            where profile_id = %(profile_id)s
        """
        return pd.read_sql_query(query,
                                 self.db_conn_string,
                                 params={"profile_id": profile_id})

    def get_df_ticker_interests_continuous(self):
        query = "select interest_id, symbol, sim_dif from ticker_interests"
        return pd.read_sql_query(query, self.db_conn_string)

    def get_df_ticker_categories_continuous(self):
        query = "select category_id, symbol, sim_dif from ticker_categories_continuous"
        return pd.read_sql_query(query, self.db_conn_string)

    def get_df_ticker_riskscore_continuous(self):
        query_filename = os.path.join(script_dir,
                                      "sql/ticker_riskscore_continuous.sql")
        with open(query_filename) as f:
            query = f.read()
        return pd.read_sql_query(query, self.db_conn_string)
