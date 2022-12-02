class ApiException(Exception):
    pass


class HttpException(Exception):

    def __init__(self, http_code, message):
        super().__init__(message)
        self.http_code = http_code
        self.message = message


class NotFoundException(HttpException):

    def __init__(self, message='Not Found.'):
        super().__init__(404, message)


class BadRequestException(HttpException):

    def __init__(self, message='Bad Request.'):
        super().__init__(400, message)


class KYCFormHasNotBeenSentException(BadRequestException):

    def __init__(self, message):
        super().__init__(message)


class EntityNotFoundException(NotFoundException):

    def __init__(self, cls):
        super().__init__(f'Entity {cls} not found.')
