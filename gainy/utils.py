from math import trunc
from typing import Iterable, List, Any
import psycopg2
from psycopg2._psycopg import connection
import numpy as np
import logging
from pythonjsonlogger import jsonlogger


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


def env() -> str:
    import os
    return os.environ.get("ENV", "local")


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


def get_logger(name):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if env() == "local" else logging.INFO)

    logHandler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    logHandler.setFormatter(formatter)
    logger.addHandler(logHandler)

    return logger
