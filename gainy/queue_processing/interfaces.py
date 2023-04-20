from abc import ABC, abstractmethod

from gainy.queue_processing.models import QueueMessage


class QueueMessageHandlerInterface(ABC):

    @abstractmethod
    def supports(self, message: QueueMessage) -> bool:
        pass

    @abstractmethod
    def handle(self, message: QueueMessage):
        pass
