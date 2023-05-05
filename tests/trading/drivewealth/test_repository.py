from gainy.exceptions import EntityNotFoundException
from gainy.trading.drivewealth import DriveWealthRepository

from gainy.trading.drivewealth.models import DriveWealthInstrument


def test_filter_inactive_symbols_from_weights(monkeypatch):
    symbol1 = "symbol1"
    symbol2 = "symbol2"
    weights = [{"symbol": symbol1}, {"symbol": symbol2}]

    def mock_get_instrument_by_symbol(symbol):
        if symbol == symbol1:
            return DriveWealthInstrument()

        raise EntityNotFoundException(DriveWealthInstrument)

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(repository, 'get_instrument_by_symbol',
                        mock_get_instrument_by_symbol)

    filtered_weights = repository.filter_inactive_symbols_from_weights(weights)

    assert symbol1 in [i["symbol"] for i in filtered_weights]
    assert symbol2 not in [i["symbol"] for i in filtered_weights]
