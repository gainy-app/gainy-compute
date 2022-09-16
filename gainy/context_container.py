from contextlib import AbstractContextManager

from psycopg2._psycopg import connection
from functools import cached_property, cache
from gainy.data_access.repository import Repository
from gainy.recommendation.repository import RecommendationRepository
from gainy.trading import TradingService, TradingRepository
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthRepository, DriveWealthApi
from gainy.utils import db_connect


class ContextContainer(AbstractContextManager):
    _db_conn = None

    def __exit__(self, exc_type, exc_value, traceback):
        if self._db_conn:
            self._db_conn.close()

    @property
    def db_conn(self) -> connection:
        if not self._db_conn:
            self._db_conn = db_connect()
        return self._db_conn

    @cache
    def get_repository(self, cls=None):
        if cls:
            raise Exception('get_repository for a class is not supported')

        return Repository(self.db_conn)

    @cached_property
    def recommendation_repository(self) -> RecommendationRepository:
        return RecommendationRepository(self.db_conn)

    # drivewealth
    @cached_property
    def drivewealth_repository(self):
        return DriveWealthRepository(self.db_conn)

    @cached_property
    def drivewealth_api(self):
        return DriveWealthApi(self.drivewealth_repository)

    @cached_property
    def drivewealth_provider(self):
        return DriveWealthProvider(self.drivewealth_repository,
                                   self.drivewealth_api)

    # trading
    @cached_property
    def trading_service(self) -> TradingService:
        return TradingService(self.drivewealth_provider)

    @cached_property
    def trading_repository(self):
        return TradingRepository(self.db_conn)
