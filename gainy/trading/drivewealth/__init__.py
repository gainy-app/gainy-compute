import regex

from .api import DriveWealthApi
from .repository import DriveWealthRepository
from .provider import DriveWealthProvider


def normalize_symbol(s: str):
    s = regex.sub(r'\.([AB])$', '-\1', s)
    return regex.sub(r'\.(.*)$', '', s)
