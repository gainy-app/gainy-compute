import random
import threading
import time
import traceback
import psycopg2
from gainy.data_access.db_lock import LockManager, ResourceType, DatabaseLock, LockAcquisitionTimeout
from gainy.utils import db_connect


class Value:
    _value: int = 0

    def get(self) -> int:
        return self._value

    def set(self, value: int):
        self._value = value


class _TestThread(threading.Thread):

    def __init__(self, value: Value, resource_type: ResourceType,
                 resource_id: int):
        super().__init__()
        self.value = value
        self.resource_type = resource_type
        self.resource_id = resource_id

    def run(self):
        with db_connect() as db_conn:
            for _ in range(0, 10):
                try:
                    with LockManager.database_lock(db_conn,
                                                   self.resource_type,
                                                   self.resource_id,
                                                   await_sec=5):
                        cur_value = self.value.get()
                        time.sleep(
                            random.choice(range(10)) / 1000
                        )  # wait a little bit for concurrent modifications
                        self.value.set(cur_value + 1)
                except Exception as e:
                    traceback.print_exc()


def test_db_lock_single_thread():
    value = Value()

    thread = _TestThread(value, ResourceType.GENERAL, 0)
    thread.start()
    thread.join()

    assert value.get() == 10
    _assert_not_locked(ResourceType.GENERAL, 0)


def test_db_lock_two_threads():
    value = Value()

    threads = []
    for _ in range(0, 2):
        thread = _TestThread(value, ResourceType.GENERAL, 0)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    assert value.get() == 20
    _assert_not_locked(ResourceType.GENERAL, 0)


def test_db_lock_multiple_threads():
    value = Value()

    threads = []
    for _ in range(0, 5):
        thread = _TestThread(value, ResourceType.GENERAL, 0)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    assert value.get() == 50
    _assert_not_locked(ResourceType.GENERAL, 0)


def test_lock_acquisition_fails_after_timeout():
    resource_id = 100
    resource_type = ResourceType.GENERAL

    with db_connect() as db_conn_1:
        lock_1 = DatabaseLock(db_conn_1, resource_type)
        assert lock_1.try_lock(resource_id)

        with db_connect() as db_conn_2:
            _assert_lock_timeout(db_conn_2, resource_type, resource_id, 0)
            _assert_lock_timeout(db_conn_2, resource_type, resource_id, 0.5)

        lock_1.unlock(resource_id)

    _assert_not_locked(ResourceType.GENERAL, resource_id)


def _assert_not_locked(resource_type: ResourceType, resource_id: int):
    with db_connect() as db_conn:
        lock = DatabaseLock(db_conn, resource_type)
        assert lock.try_lock(resource_id)
        lock.unlock(resource_id)


def _assert_lock_timeout(db_conn, resource_type, resource_id, await_sec):
    start_time = time.time()
    lock_acquisition_timeout = False

    lock = DatabaseLock(db_conn, resource_type)
    try:
        lock.lock(resource_id, await_sec)
    except LockAcquisitionTimeout:
        lock_acquisition_timeout = True
        execution_time_ms = time.time() - start_time
        assert execution_time_ms > await_sec
        assert execution_time_ms < await_sec + 0.05

    assert lock_acquisition_timeout
