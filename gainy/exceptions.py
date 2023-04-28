class ApiException(Exception):
    pass


class HttpException(Exception):

    def __init__(self, http_code, message, *args):
        super().__init__(message, *args)
        self.http_code = http_code
        self.message = message


class NotFoundException(HttpException):

    def __init__(self, message='Not Found.'):
        super().__init__(404, message)


class BadRequestException(HttpException):

    def __init__(self, message='Bad Request.', *args):
        super().__init__(400, message, *args)


class KYCFormHasNotBeenSentException(BadRequestException):

    def __init__(self, message):
        super().__init__(message)


class EntityNotFoundException(NotFoundException):

    def __init__(self, cls):
        super().__init__(f'Entity {cls} not found.')


class EmailNotSentException(Exception):

    def __init__(self, message='Email not sent.', *args):
        super().__init__(message, *args)


class AccountNeedsReauthException(Exception):
    pass


class AccountNeedsReauthHttpException(BadRequestException):

    def __init__(self,
                 message='You need to reconnect selected bank account.',
                 *args):
        super().__init__(message, *args)
