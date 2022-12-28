import enum
import os
from operator import itemgetter
from typing import List, Tuple, Iterable

from psycopg2.extras import execute_values, RealDictCursor
from psycopg2 import sql

from gainy.data_access.repository import Repository
from gainy.utils import get_logger

logger = get_logger(__name__)
script_dir = os.path.dirname(__file__)

RECOMMENDATION_MANUALLY_SELECTED_COLLECTION_IDS = os.getenv(
    "RECOMMENDATION_MANUALLY_SELECTED_COLLECTION_IDS", "").split(",")


class RecommendedCollectionAlgorithm(enum.Enum):
    MATCH_SCORE = 0
    TOP_FAVORITED = 1
    TOP_CLICKED = 2
    MANUAL_SELECTION = 3
    TOP_PERFORMANCE = 4
    TOP_MERGED_ALGORITHM = 5


def _read_sorted_collection_manually_selected(limit: int) -> List[int]:
    data = RECOMMENDATION_MANUALLY_SELECTED_COLLECTION_IDS
    data = [int(i) for i in data if i]
    return data[:limit]


class RecommendationRepository(Repository):

    def get_recommended_collections(
            self, profile_id: int, limit: int,
            algorithm: RecommendedCollectionAlgorithm
    ) -> List[Tuple[int, str]]:

        if algorithm == RecommendedCollectionAlgorithm.MATCH_SCORE:
            collection_ids = self._read_sorted_collection_match_scores(
                profile_id, limit)
        elif algorithm == RecommendedCollectionAlgorithm.TOP_FAVORITED:
            collection_ids = self._read_sorted_collection_top_favorited(limit)
        elif algorithm == RecommendedCollectionAlgorithm.TOP_CLICKED:
            collection_ids = self._read_sorted_collection_top_clicked(limit)
        elif algorithm == RecommendedCollectionAlgorithm.MANUAL_SELECTION:
            collection_ids = _read_sorted_collection_manually_selected(limit)
        elif algorithm == RecommendedCollectionAlgorithm.TOP_PERFORMANCE:
            collection_ids = self._read_sorted_collection_top_performance(
                limit)
        elif algorithm == RecommendedCollectionAlgorithm.TOP_MERGED_ALGORITHM:
            collection_ids = self._read_sorted_collection_top_merged_algorithm(
                limit)
        else:
            raise Exception('Unsupported algorithm')

        collection_uniq_ids = [f"0_{i}" for i in collection_ids]

        return list(zip(collection_ids, collection_uniq_ids))

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

    def is_personalization_enabled(self, profile_id: int) -> bool:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT is_personalization_enabled FROM profile_flags WHERE profile_id=%(profile_id)s",
                {
                    "profile_id": profile_id,
                })

            row = cursor.fetchone()
            result = row and row[0]
            logger.info('is_personalization_enabled',
                        extra={
                            "profile_id": profile_id,
                            "enabled": result
                        })
            return result

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
        queries = []
        query_filenames = [
            'generate_ticker_match_scores.sql',
            'cleanup_ticker_match_scores.sql',
            'generate_collection_match_scores.sql',
            'cleanup_collection_match_scores.sql',
        ]

        for query_filename in query_filenames:
            query_filename = os.path.join(script_dir, "sql", query_filename)
            with open(query_filename) as f:
                queries.append(f.read())

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

        queries = [
            sql.SQL(query).format(where_clause=where_clause)
            for query in queries
        ]

        with self.db_conn.cursor() as cursor:
            for query in queries:
                cursor.execute(query, params)

    def _read_sorted_collection_match_scores(self, profile_id: int,
                                             limit: int) -> List[int]:
        data = self._execute_script("sql/collection_ranking_scores.sql", {
            "profile_id": profile_id,
            "limit": limit
        })
        return list(map(itemgetter(0), data))

    def _read_sorted_collection_top_favorited(self, limit: int) -> List[int]:
        data = self._execute_script("sql/collection_top_favorited.sql",
                                    {"limit": limit})
        return list(map(itemgetter(0), data))

    def _read_sorted_collection_top_clicked(self, limit: int) -> List[int]:
        data = self._execute_script("sql/collection_top_clicked.sql",
                                    {"limit": limit})
        return list(map(itemgetter(0), data))

    def _read_sorted_collection_top_performance(self, limit: int) -> List[int]:
        data = self._execute_script("sql/collection_top_performance.sql",
                                    {"limit": limit})
        return list(map(itemgetter(0), data))

    def _read_sorted_collection_top_merged_algorithm(self,
                                                     limit: int) -> List[int]:
        data = self._execute_script("sql/collection_top_merged_algorithm.sql",
                                    {"limit": limit})
        return list(map(itemgetter(0), data))

    def _execute_script(self, script_rel_path, params):
        query_filename = os.path.join(script_dir, script_rel_path)
        with open(query_filename) as f:
            query = f.read()

        with self.db_conn.cursor() as cursor:
            cursor.execute(query, params)

            return cursor.fetchall()
