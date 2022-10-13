from contextlib import AbstractContextManager

from psycopg2._psycopg import connection
from functools import cached_property, cache

from gainy.billing.repository import BillingRepository
from gainy.billing.service import BillingService
from gainy.billing.stripe.api import StripeApi
from gainy.billing.stripe.provider import StripePaymentProvider
from gainy.billing.stripe.repository import StripeRepository
from gainy.data_access.repository import Repository
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.recommendation.repository import RecommendationRepository
from gainy.trading import TradingService, TradingRepository
from gainy.trading.drivewealth import DriveWealthProvider, DriveWealthRepository, DriveWealthApi
from gainy.utils import db_connect


class ContextContainer(AbstractContextManager):
    _db_conn = None

    def __exit__(self, exc_type, exc_value, traceback):
        if self._db_conn:
            self._db_conn.commit()
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

    @cached_property
    def collection_optimizer_repository(self) -> CollectionOptimizerRepository:
        return CollectionOptimizerRepository(self.db_conn)

    # Stripe
    @cached_property
    def stripe_repository(self) -> StripeRepository:
        return StripeRepository(self.db_conn)

    @cached_property
    def stripe_api(self) -> StripeApi:
        return StripeApi()

    @cached_property
    def stripe_payment_provider(self) -> StripePaymentProvider:
        return StripePaymentProvider(self.stripe_repository, self.stripe_api)

    # Billing
    @cached_property
    def billing_repository(self) -> BillingRepository:
        return BillingRepository(self.db_conn)

    @cached_property
    def billing_service(self) -> BillingService:
        return BillingService(self.billing_repository,
                              self.stripe_payment_provider)

    # DriveWealth
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
