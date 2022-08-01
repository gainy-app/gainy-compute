from math import trunc
from typing import Iterable, List, Any
import psycopg2
from psycopg2._psycopg import connection
import numpy as np
import logging
from pythonjsonlogger import jsonlogger
import datetime


class CustomJsonFormatter(jsonlogger.JsonFormatter):

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record,
                                                    message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

        log_record['pathname'] = record.pathname
        log_record['lineno'] = record.lineno


def env() -> str:
    import os
    return os.environ.get("ENV", "local")


formatter = CustomJsonFormatter()
LOG_LEVEL = logging.DEBUG if env() == "local" else logging.INFO
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setFormatter(formatter)
logging.basicConfig(level=LOG_LEVEL, handlers=[LOG_HANDLER], force=True)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    return logger


def batch_iter(ary, batch_size: int = 100) -> Iterable[List[Any]]:
    n_chunks = trunc(len(ary) / batch_size) + 1
    return np.array_split(ary, n_chunks)


def current_version() -> str:
    import importlib

    try:
        return importlib.metadata.version("gainy-compute")
    except importlib.metadata.PackageNotFoundError as e:
        logging.error(f"Package not found: {str(e)}")
        return "local"


def db_connect() -> connection:
    import os

    HOST = os.getenv("PG_HOST")
    PORT = os.getenv("PG_PORT")
    USERNAME = os.getenv("PG_USERNAME")
    PASSWORD = os.getenv("PG_PASSWORD")
    DB_NAME = os.getenv('PG_DBNAME')
    PUBLIC_SCHEMA_NAME = os.getenv('PUBLIC_SCHEMA_NAME') or os.getenv(
        "DBT_TARGET_SCHEMA")

    if not PUBLIC_SCHEMA_NAME or not HOST or not PORT or not DB_NAME or not USERNAME or not PASSWORD:
        raise Exception('Missing db connection env variables')

    DB_CONN_STRING = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?options=-csearch_path%3D{PUBLIC_SCHEMA_NAME}"
    return psycopg2.connect(DB_CONN_STRING)
