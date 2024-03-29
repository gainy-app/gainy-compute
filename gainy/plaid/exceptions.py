class AccessTokenApiException(Exception):

    def __init__(self, parent_exc: Exception, access_token: dict):
        self.parent_exc = parent_exc
        self.access_token = access_token

    def __str__(self):
        return str(self.parent_exc)


class AccessTokenLoginRequiredException(AccessTokenApiException):
    pass


class InvalidAccountIdException(AccessTokenApiException):
    pass


class NoAccountsException(AccessTokenApiException):
    pass


class InstitutionNotRespondingException(AccessTokenApiException):
    pass
