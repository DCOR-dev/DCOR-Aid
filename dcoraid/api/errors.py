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


class APIKeyError(APIError):
    pass


class APINotFoundError(APIError):
    pass
