class NoActivePaymentMethodException(Exception):

    def __init__(self, message="No active payment method.", *args, **kwargs):
        super().__init__(message, *args, **kwargs)


class PaymentProviderNotSupportedException(Exception):

    def __init__(self,
                 message="Payment provider not supported.",
                 *args,
                 **kwargs):
        super().__init__(message, *args, **kwargs)


class InvoiceSealedException(Exception):

    def __init__(self, message="Invoice is sealed.", *args, **kwargs):
        super().__init__(message, *args, **kwargs)
