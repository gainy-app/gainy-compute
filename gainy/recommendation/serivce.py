import csv
import os
import sys
import time
from math import trunc

from typing import Tuple, List, Iterable, Dict, Any

from psycopg2.extras import RealDictCursor

from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.recommendation.repository import RecommendationRepository, RecommendedCollectionAlgorithm
from gainy.utils import get_logger

logger = get_logger(__name__)

TOP_PERFORMANCE_COUNT = 5


def format_collections(
        collections: Iterable[Tuple[int, str]]) -> List[Dict[str, Any]]:
    return [{"id": id, "uniq_id": uniq_id} for id, uniq_id in collections]


def _unique_collections(
        collections: Iterable[Tuple[int, str]]) -> Iterable[Tuple[int, str]]:
    yielded = set()
    for collection in collections:
        if collection in yielded:
            continue

        yielded.add(collection)
        yield collection


class RecommendationService:

    def __init__(self, repository: RecommendationRepository):
        self.repository = repository

    def get_recommended_collections(
        self,
        profile_id: int,
        limit: int,
    ) -> Iterable[Tuple[int, str]]:
        if limit <= 0:
            return []

        if self.repository.is_personalization_enabled(profile_id):
            return self._get_recommended_collections_personalized(
                profile_id, limit)

        return self._get_recommended_collections_global(profile_id, limit)

    def compute_match_score(self, profile_id, log_error=True, max_tries=2):
        self.compute_risk_score(profile_id)
        recommendations_func = ComputeRecommendationsAndPersist(
            self.repository, profile_id)
        old_version = recommendations_func.load_version()

        try:
            start = time.time()
            recommendations_func.execute(max_tries=max_tries)

            new_version = recommendations_func.load_version()
            logger.info('Calculated Match Scores',
                        extra={
                            'profile_id': profile_id,
                            'old_version': old_version.recommendations_version,
                            'new_version': new_version.recommendations_version,
                            'duration': time.time() - start,
                        })
        except (LockAcquisitionTimeout, ConcurrentVersionUpdate) as e:
            if log_error:
                logger.exception(e,
                                 extra={
                                     'profile_id':
                                     profile_id,
                                     'old_version':
                                     old_version.recommendations_version,
                                 })

    def compute_risk_score(self, profile_id, scoring_settings=None):
        if scoring_settings is None:
            with self.repository.db_conn.cursor(
                    cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "select * from app.profile_scoring_settings where profile_id = %(profile_id)s",
                    {"profile_id": profile_id})
                row = cursor.fetchone()
                if not row:
                    logger.error(
                        "Failed to compute_risk_score, profile_scoring_settings record doesn't exist"
                    )
                    return
                scoring_settings = dict(row)

        risk_score = self.calculate_risk_score(scoring_settings)

        with self.repository.db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_scoring_settings set risk_score = %(risk_score)s where profile_id = %(profile_id)s",
                {
                    'risk_score': risk_score,
                    "profile_id": profile_id
                })

        return risk_score

    def calculate_risk_score(self, payload):
        script_directory = os.path.dirname(os.path.realpath(__file__))
        sys.path.append(script_directory)
        with open(
                os.path.join(script_directory,
                             '../data/user_categories_decision_matrix.csv')
        ) as csv_file:
            reader = csv.DictReader(csv_file, delimiter='\t')
            decision_matrix = list(reader)

        risk_needed = [1, 2, 2, 3][self._list_index(payload['risk_level'], 4)]
        if payload['average_market_return'] == 6 and risk_needed > 1:
            risk_needed = 3
        if payload['average_market_return'] == 50 and risk_needed < 3:
            risk_needed = 2

        investment_horizon_points = [1, 1, 2, 3][self._list_index(
            payload['investment_horizon'], 4)]
        unexpected_purchases_source_points = {
            'checking_savings': 3,
            'stock_investments': 2,
            'credit_card': 1,
            'other_loans': 1
        }[payload['unexpected_purchases_source']]
        damage_of_failure_points = [1, 2, 2, 3][self._list_index(
            payload['damage_of_failure'], 4)]
        risk_taking_ability = round(
            (investment_horizon_points + unexpected_purchases_source_points +
             damage_of_failure_points) / 3)

        stock_market_risk_level_points = {
            'very_risky': 1,
            'somewhat_risky': 2,
            'neutral': 2,
            'somewhat_safe': 3,
            'very_safe': 3,
        }[payload['stock_market_risk_level']]
        trading_experience_points = {
            'never_tried': 2,
            'very_little': 2,
            'companies_i_believe_in': 2,
            'etfs_and_safe_stocks': 2,
            'advanced': 3,
            'daily_trader': 3,
            'investment_funds': 2,
            'professional': 3,
            'dont_trade_after_bad_experience': 1
        }[payload['trading_experience']]

        loss_tolerance = round(
            (stock_market_risk_level_points + trading_experience_points) / 2)

        for i in [
                'if_market_drops_20_i_will_buy',
                'if_market_drops_40_i_will_buy'
        ]:
            if payload[i] is not None:
                buy_rate = payload[i] * 3
                if buy_rate < 1 and loss_tolerance == 3:  # sell
                    loss_tolerance -= 1
                if buy_rate > 2 and loss_tolerance != 3:  # buy
                    loss_tolerance += 1

        for i in decision_matrix:
            if int(i['Risk Need']) != risk_needed:
                continue
            if int(i['Risk Taking Ability']) != risk_taking_ability:
                continue
            if int(i['Loss Tolerance']) != loss_tolerance:
                continue

            return int(i['Hard code matrix'])

        return max(risk_needed, risk_taking_ability, loss_tolerance)

    def _get_recommended_collections_global(
            self, profile_id: int, limit: int) -> Iterable[Tuple[int, str]]:
        logging_extra = {'profile_id': profile_id}

        try:
            manually_selected_collections = self.repository.get_recommended_collections(
                profile_id, limit,
                RecommendedCollectionAlgorithm.MANUAL_SELECTION)
            logging_extra[
                'manually_selected_collections'] = manually_selected_collections

            top_performance_collections = self.repository.get_recommended_collections(
                profile_id, min(TOP_PERFORMANCE_COUNT, limit),
                RecommendedCollectionAlgorithm.TOP_PERFORMANCE)
            logging_extra[
                'top_performance_collections'] = top_performance_collections

            top_merged_algorithm_collections = self.repository.get_recommended_collections(
                profile_id, limit,
                RecommendedCollectionAlgorithm.TOP_MERGED_ALGORITHM)
            logging_extra[
                'top_merged_algorithm_collections'] = top_merged_algorithm_collections

            collections = manually_selected_collections + top_performance_collections + top_merged_algorithm_collections
            logging_extra["collections"] = collections
            if collections:
                collections = _unique_collections(collections)
                collections = list(collections)[:limit]
                logging_extra["collections"] = collections
                return collections

            logger.error(
                '_get_recommended_collections_global: no collections to recommend',
                extra=logging_extra)

            top_favorited_collections = self.repository.get_recommended_collections(
                profile_id, limit,
                RecommendedCollectionAlgorithm.TOP_FAVORITED)
            logging_extra[
                'top_favorited_collections'] = top_favorited_collections
            return top_favorited_collections

        except Exception as e:
            logger.exception(e, extra=logging_extra)
            raise e
        finally:
            logger.info('_get_recommended_collections_global',
                        extra=logging_extra)

    def _get_recommended_collections_personalized(
            self, profile_id: int, limit: int) -> Iterable[Tuple[int, str]]:
        logging_extra = {'profile_id': profile_id}

        try:
            # Step 1: get recommended by MS
            collections = self.repository.get_recommended_collections(
                profile_id, limit, RecommendedCollectionAlgorithm.MATCH_SCORE)
            if collections:
                logging_extra["collections"] = collections
                logger.info('_get_recommended_collections_personalized',
                            extra=logging_extra)
                return collections

            # Step 2: update MS and try again
            logger.info(
                '_get_recommended_collections_personalized: update_match_scores',
                extra=logging_extra)
            self.compute_match_score(profile_id, log_error=False)
            collections = self.repository.get_recommended_collections(
                profile_id, limit, RecommendedCollectionAlgorithm.MATCH_SCORE)
            if collections:
                logging_extra["collections"] = collections
                logger.info('_get_recommended_collections_personalized',
                            extra=logging_extra)
                return collections

            logger.error(
                '_get_recommended_collections_personalized: use global recommendations',
                extra=logging_extra)
            return self._get_recommended_collections_global(profile_id, limit)

        except Exception as e:
            logger.exception(e, extra=logging_extra)
            raise e

    @staticmethod
    def _list_index(value, list_size):
        """
        Select the list index between 0 and `list_size` - 1 based on ``value`` parameter
        :param value: real number between 0 and 1
        :param list_size: the size of the list
        :return: the index between 0 and `list_size` - 1
        """
        if value >= 1.0:
            # covers case where `value` = 1.0
            return list_size - 1

        return trunc(value * list_size)
