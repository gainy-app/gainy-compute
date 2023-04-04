from typing import Iterable

import argparse
import traceback

from gainy.context_container import ContextContainer
from gainy.utils import get_logger, env, ENV_PRODUCTION

logger = get_logger(__name__)


def iterate_profile_ids(db_conn) -> Iterable[int]:
    with db_conn.cursor() as cursor:
        query = "SELECT id FROM app.profiles"
        if env() == ENV_PRODUCTION:
            query += " where email not ilike '%test%@gainy.app'"

        cursor.execute(query)
        for row in cursor:
            yield row[0]


def cli(args=None):
    parser = argparse.ArgumentParser(
        description='Update profiles\' analytics attributes.')
    parser.add_argument('--profile-id', dest='profile_id', type=int)
    args = parser.parse_args(args)

    profile_id = args.profile_id

    try:
        with ContextContainer() as context_container:
            if profile_id:
                context_container.analytics_service.sync_profile_properties(
                    profile_id)
                logger.info(
                    f'Profile attributes synced for profile {profile_id}')
                return

            for profile_id in iterate_profile_ids(context_container.db_conn):
                context_container.analytics_service.sync_profile_properties(
                    profile_id)
                logger.info(
                    f'Profile attributes synced for profile {profile_id}')
    except Exception as e:
        traceback.print_exc()
        raise e
