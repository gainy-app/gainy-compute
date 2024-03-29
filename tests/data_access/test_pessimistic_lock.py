import threading
import time
import random
import traceback
from typing import List

import pytest
from gainy.data_access.models import BaseModel, ResourceVersion, classproperty
from gainy.data_access.db_lock import ResourceType
from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.data_access.repository import Repository
from gainy.utils import db_connect


class DataClass(BaseModel):
    profile_id = None
    symbol = None
    value_list = None

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profile_data"

    @classproperty
    def key_fields(self) -> List[str]:
        return ["profile_id", "symbol"]


class MetadataClass(BaseModel, ResourceVersion):
    profile_id = None
    version = None

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.GENERAL

    @property
    def resource_id(self) -> int:
        return self.profile_id

    @property
    def resource_version(self):
        return self.version

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profile_metadata"

    @classproperty
    def key_fields(self) -> List[str]:
        return ["profile_id"]

    def update_version(self):
        self.version = self.version + 1 if self.version else 1


class _TestGetAndPersist(AbstractPessimisticLockingFunction):

    def __init__(self,
                 repo: Repository,
                 profile_id: int,
                 objects_per_iter: int = 200):
        super().__init__(repo)
        self.profile_id = profile_id
        self.objects_per_iter = objects_per_iter

    def load_version(self):
        profile_metadata_list = self.repo.find_all(
            MetadataClass, {"profile_id": self.profile_id})
        if len(profile_metadata_list) == 0:
            profile_metadata = MetadataClass()
            profile_metadata.profile_id = self.profile_id
            return profile_metadata
        else:
            return profile_metadata_list[0]

    def _do(self, version):
        entities = self.repo.find_all(DataClass,
                                      {"profile_id": self.profile_id})
        for object_index in range(0, self.objects_per_iter):
            symbol = f"S{object_index}"
            entity_by_symbol = [
                entity for entity in entities if entity.symbol == symbol
            ]
            if len(entity_by_symbol) > 0:
                entity = entity_by_symbol[0]
                entity.value_list.append(len(entity.value_list) + 1)
            else:
                entity = DataClass()
                entity.profile_id = self.profile_id
                entity.symbol = symbol
                entity.value_list = [1]
                entities.append(entity)

        time.sleep(random.choice(range(10, 50)) / 1000)

        self.repo.persist(entities)


class _TestThread(threading.Thread):

    def __init__(self, profile_id: int, max_iter: int = 5):
        super().__init__()
        self.profile_id = profile_id
        self.max_iter = max_iter

    def run(self):
        for iter_index in range(0, self.max_iter):
            try:
                with db_connect() as db_conn:
                    func = _TestGetAndPersist(Repository(db_conn),
                                              self.profile_id)
                    func.execute(50)
            except Exception as e:
                traceback.print_exc()
                raise e


@pytest.fixture(scope='function')
def metadata_table(request):
    metadata_table = f"{MetadataClass.schema_name}.{MetadataClass.table_name}"

    def table_teardown():
        with db_connect() as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {metadata_table}")

    table_teardown()  # Clean from previous executions if needed

    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS app")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {metadata_table} (
                    profile_id int4 PRIMARY KEY,
                    version int4 NOT NULL
                )
            """)

    request.addfinalizer(table_teardown)
    yield metadata_table


@pytest.fixture(scope="function")
def data_table(request):
    data_table = f"{DataClass.schema_name}.{DataClass.table_name}"

    def table_teardown():
        with db_connect() as db_conn:
            with db_conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {data_table}")

    table_teardown()  # Clean from previous executions if needed

    with db_connect() as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {data_table} (
                    profile_id int4,
                    symbol varchar NOT NULL,
                    value_list int4[] NOT NULL,
                    PRIMARY KEY(profile_id, symbol)
                )
            """)

    request.addfinalizer(table_teardown)
    yield data_table


def test_pessimistic_locks_single_thread(metadata_table, data_table):
    _test_pessimistic_locks(1, 1)


def test_pessimistic_locks_two_threads(metadata_table, data_table):
    _test_pessimistic_locks(2, 2)


def test_pessimistic_locks_multiple_threads(metadata_table, data_table):
    _test_pessimistic_locks(3, 5)


def _test_pessimistic_locks(profile_num: int, threads_per_profile: int):
    threads = []
    for thread_id in range(0, threads_per_profile * profile_num):
        thread = _TestThread(thread_id % profile_num)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    with db_connect() as db_conn:
        repo = Repository(db_conn)
        metadata_list = repo.find_all(MetadataClass)
        assert len(metadata_list) == profile_num
        for metadata in metadata_list:
            assert metadata.version == threads_per_profile * 5

        data_list = repo.find_all(DataClass)
        assert len(data_list) == 200 * profile_num
        for data in data_list:
            assert set(data.value_list) == set(
                range(1, threads_per_profile * 5 + 1))
