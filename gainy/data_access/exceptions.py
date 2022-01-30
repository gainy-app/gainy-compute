class RepositoryException(Exception):

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class ObjectNotFoundException(RepositoryException):

    def __init__(self, message):
        super().__init__(message)