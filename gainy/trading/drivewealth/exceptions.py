from gainy.exceptions import ApiException


class DriveWealthApiException(ApiException):

    def __init__(self, status_code, code, message):
        self.status_code = status_code
        self.code = code
        self.message = "%s: %s" % (code, message)

    @staticmethod
    def create_from_response(data, status_code):
        error_code_to_exception_class = {
            "I050": InstrumentNotFoundException,
            "E032": BadMissingParametersBodyException,
        }

        message = "Failed: %d" % status_code
        error_code = None
        if data:
            message = data.get("message", message)
            error_code = data.get("errorCode")

        if error_code in error_code_to_exception_class:
            cls = error_code_to_exception_class[error_code]
        else:
            cls = DriveWealthApiException

        return cls(status_code, error_code, message)


class InstrumentNotFoundException(DriveWealthApiException):
    pass


class BadMissingParametersBodyException(DriveWealthApiException):
    pass


class TradingAccountNotOpenException(Exception):
    pass
