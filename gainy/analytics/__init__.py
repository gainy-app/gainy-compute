import datetime
from _decimal import Decimal

from gainy.utils import DATETIME_ISO8601_FORMAT_TZ, DATE_ISO8601_FORMAT


def format_properties(data):
    if isinstance(data, list):
        return [format_properties(i) for i in data]
    if isinstance(data, dict):
        return {k: format_properties(i) for k, i in data.items()}
    if isinstance(data, datetime.datetime):
        return data.strftime(DATETIME_ISO8601_FORMAT_TZ)
    if isinstance(data, datetime.date):
        return data.strftime(DATE_ISO8601_FORMAT)
    if isinstance(data, Decimal):
        return float(data)

    return data
