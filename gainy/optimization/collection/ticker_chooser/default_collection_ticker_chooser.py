from gainy.optimization.collection.ticker_chooser import AbstractCollectionTickerChooser


class DefaultCollectionTickerChooser(AbstractCollectionTickerChooser):

    def get_tickers(self, collection_id: int) -> list[str]:
        return self.repository.get_collection_tickers(collection_id)

    def supports_collection(self, collection_id: int) -> bool:
        return True
