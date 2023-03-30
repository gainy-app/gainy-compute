from abc import ABC, abstractmethod


class AttributionSourceInterface(ABC):

    @abstractmethod
    def get_attributes(self, profile_id: int) -> dict:
        pass


class AnalyticsSinkInterface(ABC):

    @abstractmethod
    def update_user_properties(self, profile_id: int,
                               attributes: dict) -> dict:
        pass

    @abstractmethod
    def send_event(self, profile_id: int, event_name: str, properties: dict):
        pass
