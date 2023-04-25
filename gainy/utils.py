import os
from math import trunc
from typing import Iterable, List, Any
import logging
import datetime
import traceback
import sys

import psycopg2
from psycopg2._psycopg import connection
import numpy as np
from pythonjsonlogger import jsonlogger

PUBLIC_SCHEMA_NAME = os.getenv('PUBLIC_SCHEMA_NAME') or os.getenv(
    "DBT_TARGET_SCHEMA")

DATE_ISO8601_FORMAT = '%Y-%m-%d'
DATETIME_ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%S'
DATETIME_ISO8601_FORMAT_TZ = DATETIME_ISO8601_FORMAT + '%z'


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

        log_record['name'] = record.name
        log_record['pathname'] = record.pathname
        log_record['lineno'] = record.lineno

        (exc_type, exc_value, exc_tb) = sys.exc_info()
        if exc_type is not None:
            log_record["exc_type"] = exc_type.__name__
            log_record["traceback"] = [{
                "filename": frame.filename,
                "lineno": frame.lineno,
                "name": frame.name
            } for frame in traceback.extract_tb(exc_tb)]


ENV_PRODUCTION = "production"
ENV_TEST = "test"
ENV_LOCAL = "local"


def env() -> str:
    import os
    return os.environ.get("ENV", ENV_LOCAL)


formatter = CustomJsonFormatter()
LOG_LEVEL = logging.DEBUG if env() == ENV_LOCAL else logging.INFO
LOG_HANDLER = logging.StreamHandler()
LOG_HANDLER.setFormatter(formatter)
logging.basicConfig(level=LOG_LEVEL, handlers=[LOG_HANDLER], force=True)
LOGGING_MIDDLEWARES = {}


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    for f in LOGGING_MIDDLEWARES.values():
        logger = f(logger)

    return logger


class LoggerAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if "extra" in kwargs and kwargs["extra"]:
            kwargs["extra"] = {**self.extra, **kwargs["extra"]}
        else:
            kwargs["extra"] = self.extra
        return msg, kwargs


def setup_lambda_logging_middleware(context):
    LOGGING_MIDDLEWARES['aws_middleware'] = lambda _logger: LoggerAdapter(
        _logger, {
            'invoked_function_arn': context.invoked_function_arn,
            'log_stream_name': context.log_stream_name,
            'log_group_name': context.log_group_name,
            'aws_request_id': context.aws_request_id,
            'memory_limit_in_mb': context.memory_limit_in_mb,
        })


def setup_exception_logger_hook():
    sys._excepthook = sys.excepthook

    def exception_hook(exc_type, exc_value, tb):
        get_logger("root").exception(exc_value)

    sys.excepthook = exception_hook


def batch_iter(ary, batch_size: int = 100) -> Iterable[List[Any]]:
    n_chunks = trunc(len(ary) / batch_size) + 1
    return np.array_split(ary, n_chunks)


def current_version() -> str:
    import importlib

    try:
        return importlib.metadata.version("gainy-compute")
    except importlib.metadata.PackageNotFoundError as e:
        logging.error(f"Package not found: {str(e)}")
        return ENV_LOCAL


def db_connect() -> connection:
    HOST = os.getenv("PG_HOST")
    PORT = os.getenv("PG_PORT")
    USERNAME = os.getenv("PG_USERNAME")
    PASSWORD = os.getenv("PG_PASSWORD")
    DB_NAME = os.getenv('PG_DBNAME')

    if not PUBLIC_SCHEMA_NAME or not HOST or not PORT or not DB_NAME or not USERNAME or not PASSWORD:
        raise Exception('Missing db connection env variables')

    DB_CONN_STRING = f"postgresql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?options=-csearch_path%3D{PUBLIC_SCHEMA_NAME}"
    return psycopg2.connect(DB_CONN_STRING)
