import enum
from contextlib import AbstractContextManager

import logging

import backoff
from backoff import full_jitter
from psycopg2._psycopg import connection


class ResourceType(enum.Enum):
    GENERAL = 0
    PROFILE_RECOMMENDATIONS = 1


class LockAcquisitionTimeout(Exception):

    def __init__(self, resource_type: ResourceType, resource_id: int):
        super().__init__(
            f"Lock not acquired for resource: type {resource_type}, id: {resource_id}"
        )


class DatabaseLock:

    def __init__(self, db_conn: connection, resource_type: ResourceType):
        self.db_conn = db_conn
        self.resource_type = resource_type

    def try_lock(self, resource_id: int) -> bool:
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_try_advisory_lock(%(resource_type)s, %(resource_id)s)",
                {
                    "resource_type": self.resource_type.value,
                    "resource_id": resource_id
                })
            return cursor.fetchone()[0]

    def lock(self, resource_id: int, await_sec: float = 1):
        backoff_on_predicate = backoff.on_predicate(
            lambda: backoff.expo(base=2, factor=0.01),
            max_time=await_sec,
            jitter=lambda w: w / 2 + full_jitter(w / 2))
        is_locked = backoff_on_predicate(lambda: self.try_lock(resource_id))()

        if not is_locked:
            raise LockAcquisitionTimeout(self.resource_type, resource_id)

    def unlock(self, resource_id: int):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_unlock(%(resource_type)s, %(resource_id)s)",
                {
                    "resource_type": self.resource_type.value,
                    "resource_id": resource_id
                })


class DatabaseLockContext(AbstractContextManager):

    def __init__(self, db_conn: connection, resource_type: ResourceType,
                 resource_id: int, await_sec: float):
        self.db_lock = DatabaseLock(db_conn, resource_type)
        self.resource_id = resource_id
        self.await_sec = await_sec

    def __enter__(self):
        try:
            self.db_lock.lock(self.resource_id, self.await_sec)
        except LockAcquisitionTimeout as lat:
            logging.info(lat, exc_info=True)
            raise lat

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_lock.unlock(self.resource_id)
        if exc_value:
            raise exc_value


class LockManager:

    @classmethod
    def database_lock(cls,
                      db_conn: connection,
                      resource_type: ResourceType,
                      resource_id: int,
                      await_sec: float = 1) -> DatabaseLockContext:
        return DatabaseLockContext(db_conn, resource_type, resource_id,
                                   await_sec)
