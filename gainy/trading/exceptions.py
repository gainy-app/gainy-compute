class InsufficientFundsException(Exception):

    def __init__(self, message='Insufficient funds.'):
        super().__init__(message)


class TradingPausedException(Exception):

    def __init__(
            self,
            message='Trading for this account is paused. Please contact support.'
    ):
        self.message = message
        super().__init__(message)


class InsufficientHoldingValueException(Exception):

    def __init__(self, message='Insufficient holdings.'):
        super().__init__(message)


class SymbolIsNotTradeableException(Exception):

    def __init__(self, symbol):
        super().__init__('Symbol %s is not tradeable.' % symbol)
