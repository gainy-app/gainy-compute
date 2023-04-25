from gainy.trading.drivewealth.provider.misc import normalize_symbol


def test_normalize_symbol():
    assert normalize_symbol('ABC.A') == 'ABC-A'
    assert normalize_symbol('ABC.B') == 'ABC-B'
    assert normalize_symbol('ABC.D') == 'ABC'
    assert normalize_symbol('ABC.EF') == 'ABC'
    assert normalize_symbol('ABC.E.F') == 'ABC'
