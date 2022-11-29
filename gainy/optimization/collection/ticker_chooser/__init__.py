from gainy.optimization.collection.repository import CollectionOptimizerRepository
from gainy.optimization.collection.ticker_chooser.abstract_collection_ticker_chooser import \
    AbstractCollectionTickerChooser
from gainy.optimization.collection.ticker_chooser.default_collection_ticker_chooser import \
    DefaultCollectionTickerChooser
from gainy.optimization.collection.ticker_chooser.inflation_proof_collection_ticker_chooser import \
    InflationProofCollectionTickerChooser


class TickersChooser:

    def __init__(self, repository: CollectionOptimizerRepository):
        self.repository = repository
        self.default_collection_ticker_chooser = DefaultCollectionTickerChooser(
            repository)
        self.collection_ticker_choosers = [
            InflationProofCollectionTickerChooser(repository)
        ]

    def get_collection_tickers(self, collection_id: int) -> list:
        return self._get_collection_ticker_chooser(collection_id).get_tickers(
            collection_id)

    def _get_collection_ticker_chooser(
            self, collection_id) -> AbstractCollectionTickerChooser:
        for i in self.collection_ticker_choosers:
            if i.supports_collection(collection_id):
                return i

        return DefaultCollectionTickerChooser(self.repository)
