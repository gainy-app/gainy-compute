import time
from typing import Iterable

import argparse
import traceback

from gainy.context_container import ContextContainer
from gainy.data_access.repository import Repository
from gainy.queue_processing.exceptions import UnsupportedMessageException
from gainy.queue_processing.models import QueueMessage
from gainy.utils import get_logger

logger = get_logger(__name__)


def iterate_failed_events(repository: Repository) -> Iterable[QueueMessage]:
    yield from repository.iterate_all(QueueMessage, {"handled": False})


def cli(args=None):
    parser = argparse.ArgumentParser(
        description='Re handle failed queue events.')
    parser.add_argument('--max-count', dest='max_count', type=int)
    args = parser.parse_args(args)
    max_count = args.max_count

    try:
        with ContextContainer() as context_container:
            dispatcher = context_container.queue_message_dispatcher
            repo = context_container.get_repository()

            for index, message in enumerate(iterate_failed_events(repo)):
                if index >= max_count:
                    return
                start = time.time()
                logger_extra = {"queue_message_id": message.id}

                try:
                    dispatcher.handle(message)
                    repo.persist(message)
                    context_container.db_conn.commit()
                    logger_extra["duration"] = time.time() - start
                    logger.info('Handled queue message', extra=logger_extra)
                except UnsupportedMessageException as e:
                    logger.warning(e, extra=logger_extra)
                except Exception as e:
                    logger.exception(e, extra=logger_extra)
                    context_container.db_conn.rollback()
    except Exception as e:
        traceback.print_exc()
        raise e
