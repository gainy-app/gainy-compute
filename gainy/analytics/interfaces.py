from abc import ABC, abstractmethod


class AttributionSourceInterface(ABC):
    @abstractmethod
    def get_attributes(self, profile_id: int) -> dict:
        pass


class AnalyticsSinkInterface(ABC):
    @abstractmethod
    def update_profile_attribution(self, profile_id: int, attributes: dict) -> dict:
        pass

