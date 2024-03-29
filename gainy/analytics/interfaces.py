from abc import ABC, abstractmethod


class ProfilePropertiesSourceInterface(ABC):

    @abstractmethod
    def get_properties(self, profile_id: int) -> dict:
        pass


class AnalyticsSinkInterface(ABC):

    @abstractmethod
    def update_user_properties(self, profile_id: int,
                               properties: dict) -> dict:
        pass

    @abstractmethod
    def send_event(self, profile_id: int, event_name: str, properties: dict):
        pass
