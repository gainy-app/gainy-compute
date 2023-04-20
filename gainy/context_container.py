from contextlib import AbstractContextManager

from psycopg2._psycopg import connection
from functools import cached_property, cache

from gainy.analytics.amplitude.service import AmplitudeService
from gainy.analytics.appsflyer import AppsflyerService
from gainy.analytics.attribution_sources.db import DBProfilePropertiesSource
from gainy.analytics.firebase.sdk import FirebaseClient
from gainy.analytics.firebase.service import FirebaseService
from gainy.analytics.repository import AnalyticsRepository
from gainy.analytics.service import AnalyticsService
from gainy.billing.drivewealth.provider import DriveWealthPaymentProvider
from gainy.billing.repository import BillingRepository
from gainy.billing.service import BillingService
from gainy.billing.stripe.api import StripeApi
from gainy.billing.stripe.provider import StripePaymentProvider
from gainy.billing.stripe.repository import StripeRepository
from gainy.data_access.repository import Repository
from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.plaid.service import PlaidService
from gainy.queue_processing.aws_message_handler import AwsMessageHandler
from gainy.queue_processing.dispatcher import QueueMessageDispatcher
from gainy.recommendation.repository import RecommendationRepository
from gainy.recommendation.serivce import RecommendationService
from gainy.services.notification import NotificationService
from gainy.services.sendgrid import SendGridService
from gainy.trading.drivewealth.queue_message_handler import DriveWealthQueueMessageHandler
from gainy.trading.service import TradingService
from gainy.trading.repository import TradingRepository
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
    def recommendation_service(self) -> RecommendationService:
        return RecommendationService(self.recommendation_repository)

    @cached_property
    def collection_optimizer_repository(self) -> CollectionOptimizerRepository:
        return CollectionOptimizerRepository(self.db_conn)

    @cached_property
    def plaid_service(self) -> PlaidService:
        return PlaidService(self.db_conn)

    # Analytics

    @cached_property
    def analytics_repository(self) -> AnalyticsRepository:
        return AnalyticsRepository(self.db_conn)

    @cached_property
    def amplitude_service(self) -> AmplitudeService:
        return AmplitudeService()

    @cached_property
    def appsflyer_service(self) -> AppsflyerService:
        return AppsflyerService(self.analytics_repository)

    @cached_property
    def firebase_client(self) -> FirebaseClient:
        return FirebaseClient(self.analytics_repository)

    @cached_property
    def firebase_service(self) -> FirebaseService:
        return FirebaseService(self.firebase_client)

    @cached_property
    def analytics_service(self) -> AnalyticsService:
        db_source = DBProfilePropertiesSource(self.get_repository())
        return AnalyticsService([db_source], [
            self.amplitude_service, self.firebase_service,
            self.appsflyer_service
        ], self.get_repository())

    @cached_property
    def sendgrid_service(self) -> SendGridService:
        return SendGridService()

    @cached_property
    def notification_service(self) -> NotificationService:
        return NotificationService(self.get_repository(),
                                   self.sendgrid_service)

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

    @cached_property
    def drivewealth_payment_provider(self) -> DriveWealthPaymentProvider:
        return DriveWealthPaymentProvider(self.drivewealth_provider,
                                          self.drivewealth_repository,
                                          self.drivewealth_api)

    # Billing
    @cached_property
    def billing_repository(self) -> BillingRepository:
        return BillingRepository(self.db_conn)

    @cached_property
    def billing_service(self) -> BillingService:
        return BillingService(
            self.billing_repository, self.analytics_service,
            [self.stripe_payment_provider, self.drivewealth_payment_provider])

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
                                   self.drivewealth_api,
                                   self.trading_repository,
                                   self.notification_service,
                                   self.analytics_service)

    # trading
    @cached_property
    def trading_service(self) -> TradingService:
        return TradingService(self.trading_repository,
                              self.drivewealth_provider, self.plaid_service)

    @cached_property
    def trading_repository(self):
        return TradingRepository(self.db_conn)

    # queues
    @cached_property
    def drivewealth_queue_message_handler(
            self) -> DriveWealthQueueMessageHandler:
        return DriveWealthQueueMessageHandler(self.drivewealth_repository,
                                              self.drivewealth_provider,
                                              self.trading_repository,
                                              self.analytics_service,
                                              self.notification_service)

    @cached_property
    def aws_message_handler(self) -> AwsMessageHandler:
        return AwsMessageHandler()

    @cached_property
    def queue_message_dispatcher(self) -> QueueMessageDispatcher:
        return QueueMessageDispatcher(
            [self.drivewealth_queue_message_handler, self.aws_message_handler])
