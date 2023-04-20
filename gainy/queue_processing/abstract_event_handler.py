from abc import abstractmethod, ABC


class EventHandlerInterface(ABC):

    @abstractmethod
    def supports(self, event_type: str):
        pass

    @abstractmethod
    def handle(self, event_payload: dict):
        pass
