from typing import Tuple, List, Iterable, Dict, Any

from gainy.data_access.db_lock import LockAcquisitionTimeout
from gainy.data_access.optimistic_lock import ConcurrentVersionUpdate
from gainy.recommendation.compute import ComputeRecommendationsAndPersist
from gainy.recommendation.repository import RecommendationRepository, RecommendedCollectionAlgorithm
from gainy.utils import get_logger

logger = get_logger(__name__)


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
        if self.repository.is_personalization_enabled(profile_id):
            yield from self._get_recommended_collections_personalized(
                profile_id, limit)

        yield from self._get_recommended_collections_global(profile_id, limit)

    def compute_match_score(self, profile_id, log_error=True):
        recommendations_func = ComputeRecommendationsAndPersist(
            self.repository, profile_id)
        old_version = recommendations_func.load_version()

        try:
            recommendations_func.execute(max_tries=2)

            new_version = recommendations_func.load_version()
            logger.info('Calculated Match Scores',
                        extra={
                            'profile_id': profile_id,
                            'old_version': old_version.recommendations_version,
                            'new_version': new_version.recommendations_version,
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

    def _get_recommended_collections_global(
            self, profile_id: int, limit: int) -> Iterable[Tuple[int, str]]:
        logging_extra = {'profile_id': profile_id}

        try:
            manually_selected_collections = self.repository.get_recommended_collections(
                profile_id, limit,
                RecommendedCollectionAlgorithm.MANUAL_SELECTION)
            logging_extra[
                'manually_selected_collections'] = manually_selected_collections

            top_clicked_collections = self.repository.get_recommended_collections(
                profile_id, limit, RecommendedCollectionAlgorithm.TOP_CLICKED)
            logging_extra['top_clicked_collections'] = top_clicked_collections

            collections = manually_selected_collections + top_clicked_collections
            if collections:
                collections = _unique_collections(collections)
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
