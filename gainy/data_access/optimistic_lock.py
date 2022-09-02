import logging
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

    @abstractmethod
    def load_version(self):
        pass

    @abstractmethod
    def get_entities(self):
        pass

    def get_and_persist(self, max_tries: int = 3):
        backoff_on_exception = backoff.on_exception(
            lambda: backoff.expo(base=2, factor=0.1),
            exception=(LockAcquisitionTimeout, ConcurrentVersionUpdate),
            max_tries=max_tries,
            giveup_log_level=logging.WARNING,
            jitter=lambda w: w / 2 + full_jitter(w / 2))
        try:
            backoff_on_exception(self._try_get_and_persist)()
        except Exception as e:
            logging.warning(e, exc_info=True)
            raise e

    def _try_get_and_persist(self):
        cur_version = self.load_version()
        entities = self.get_entities()

        # TODO dependency on repo for database_lock looks bad
        with LockManager.database_lock(self.repo.db_conn,
                                       cur_version.resource_type,
                                       cur_version.resource_id):
            new_version = self.load_version()

            is_creation = cur_version.resource_version is None and not new_version
            is_same_version = new_version and new_version.resource_version == cur_version.resource_version
            if is_creation or is_same_version:
                new_version.update_version()
            else:
                raise ConcurrentVersionUpdate(cur_version, new_version)

            self.repo.persist(new_version)
            self._do_persist(entities)
            self.repo.commit()

    def _do_persist(self, entities):
        self.repo.persist(entities)
