import os

import regex

from .api import DriveWealthApi
from .repository import DriveWealthRepository
from .provider import DriveWealthProvider

IS_UAT = os.getenv("DRIVEWEALTH_IS_UAT", "true") != "false"


# also in https://github.com/gainy-app/gainy-app/blob/main/src/meltano/meltano/seed/00_functions.sql
# also in https://github.com/gainy-app/gainy-compute/blob/main/fixtures/functions.sql
def normalize_symbol(s: str):
    s = regex.sub(r'\.([AB])$', '-\\1', s)
    return regex.sub(r'\.(.*)$', '', s)
