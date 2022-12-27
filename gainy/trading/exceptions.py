class InsufficientFundsException(Exception):

    def __init__(self, message='Insufficient funds.'):
        super().__init__(message)


class InsufficientHoldingValueException(Exception):

    def __init__(self, message='Insufficient holdings.'):
        super().__init__(message)
