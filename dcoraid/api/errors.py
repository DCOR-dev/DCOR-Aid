class APIError(BaseException):
    pass


class APIAuthorizationError(APIError):
    pass


class APIBadGatewayError(APIError):
    pass


class APIConflictError(APIError):
    pass


class APIGatewayTimeoutError(APIError):
    pass


class APINotFoundError(APIError):
    pass


class APIOutdatedError(APIError):
    pass


class NoAPIKeyError(APIError):
    pass
