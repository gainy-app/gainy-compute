import logging
from abc import ABC, abstractmethod
from backoff import full_jitter
import backoff

from gainy.data_access.db_lock import LockManager, LockAcquisitionTimeout
from gainy.data_access.models import ResourceVersion
from gainy.utils import get_logger

logger = get_logger(__name__)


class AbstractPessimisticLockingFunction(ABC):

    def __init__(self, repo):
        self.repo = repo

    @abstractmethod
    def load_version(self) -> ResourceVersion:
        pass

    def execute(self, max_tries: int = 3):
        backoff_on_exception = backoff.on_exception(
            lambda: backoff.expo(base=2, factor=0.1),
            exception=LockAcquisitionTimeout,
            max_tries=max_tries,
            giveup_log_level=logging.WARNING,
            jitter=lambda w: w / 2 + full_jitter(w / 2))
        try:
            return backoff_on_exception(self._try_execute)()
        except Exception as e:
            logging.warning(e, exc_info=True)
            raise e

    def _try_execute(self):
        cur_version = self.load_version()

        # TODO dependency on repo for database_lock looks bad
        with LockManager.database_lock(self.repo.db_conn,
                                       cur_version.resource_type,
                                       cur_version.resource_id):
            new_version = self.load_version()
            new_version.update_version()
            self.repo.persist(new_version)

            result = self._do(new_version)
            self.repo.commit()

        return result

    @abstractmethod
    def _do(self, version):
        pass
