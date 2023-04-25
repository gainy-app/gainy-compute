from typing import List

from gainy.queue_processing.exceptions import UnsupportedMessageException
from gainy.queue_processing.interfaces import QueueMessageHandlerInterface
from gainy.queue_processing.models import QueueMessage
from gainy.utils import get_logger

logger = get_logger(__name__)


class QueueMessageDispatcher:
    handlers: List[QueueMessageHandlerInterface]

    def __init__(self, handlers: List[QueueMessageHandlerInterface]):
        self.handlers = handlers

    def handle(self, message: QueueMessage):
        self._get_handler(message).handle(message)

    def _get_handler(self,
                     message: QueueMessage) -> QueueMessageHandlerInterface:
        for handler in self.handlers:
            if handler.supports(message):
                return handler

        raise UnsupportedMessageException(
            f'Unsupported message {message.ref_id}')
