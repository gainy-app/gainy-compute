import logging
import time
from abc import ABC, abstractmethod
from backoff import full_jitter
import backoff
from psycopg2._psycopg import connection

from gainy.data_access.db_lock import LockManager, LockAcquisitionTimeout
from gainy.data_access.models import ResourceVersion
from gainy.utils import get_logger

logger = get_logger(__name__)


class ConcurrentVersionUpdate(Exception):

    def __init__(self, cur_version: ResourceVersion,
                 next_version: ResourceVersion):
        super().__init__(
            f"Concurrent update of resource {cur_version.resource_type.name}:{cur_version.resource_id}, expected version: {cur_version.resource_version}, actual version: {next_version.resource_version}"
        )


class AbstractOptimisticLockingFunction(ABC):

    def __init__(self, repo):
        self.repo = repo
        self.time_spent = {
            "_try_get_and_persist0": 0,
            "_try_get_and_persist1": 0,
            "_try_get_and_persist2": 0,
        }

    @abstractmethod
    def load_version(self, db_conn: connection):
        pass

    @abstractmethod
    def get_entities(self, db_conn: connection):
        pass

    def get_and_persist(self, db_conn: connection, max_tries: int = 3):
        backoff_on_exception = backoff.on_exception(
            lambda: backoff.expo(base=2, factor=0.1),
            exception=(LockAcquisitionTimeout, ConcurrentVersionUpdate),
            max_tries=max_tries,
            giveup_log_level=logging.WARNING,
            jitter=lambda w: w / 2 + full_jitter(w / 2))
        try:
            backoff_on_exception(lambda: self._try_get_and_persist(db_conn))()
        except Exception as e:
            logging.warning(e, exc_info=True)
            raise e

    def _try_get_and_persist(self, db_conn: connection):
        start_time = time.time()

        cur_version = self.load_version(db_conn)
        entities = self.get_entities(db_conn)

        self.time_spent["_try_get_and_persist0"] += time.time() - start_time
        start_time = time.time()

        with LockManager.database_lock(db_conn, cur_version.resource_type,
                                       cur_version.resource_id):
            new_version = self.load_version(db_conn)

            self.time_spent["_try_get_and_persist1"] += time.time(
            ) - start_time
            start_time = time.time()

            is_creation = cur_version.resource_version is None and not new_version
            is_same_version = new_version and new_version.resource_version == cur_version.resource_version
            if is_creation or is_same_version:
                new_version.update_version()
            else:
                raise ConcurrentVersionUpdate(cur_version, new_version)

            self.repo.persist(db_conn, new_version)
            self._do_persist(db_conn, entities)
            db_conn.commit()
            self.time_spent["_try_get_and_persist2"] += time.time(
            ) - start_time

    def _do_persist(self, db_conn, entities):
        self.repo.persist(db_conn, entities)
