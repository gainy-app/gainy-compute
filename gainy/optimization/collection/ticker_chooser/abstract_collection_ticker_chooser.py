from abc import ABC, abstractmethod

from gainy.optimization.collection.repository import CollectionOptimizerRepository


class AbstractCollectionTickerChooser(ABC):

    def __init__(self, repository: CollectionOptimizerRepository):
        self.repository = repository

    @abstractmethod
    def get_tickers(self, collection_id: int) -> list[str]:
        pass

    def supports_collection(self, collection_id: int) -> bool:
        return True
