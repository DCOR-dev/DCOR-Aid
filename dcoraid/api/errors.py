class APIError(BaseException):
    """General CKANAPI error"""
    pass


class APIAuthorizationError(APIError):
    """User credentials are invalid"""
    pass


class APIBadGatewayError(APIError):
    """Cannot connect to DCOR server"""
    pass


class APIBadRequest(APIError):
    """An API command cannot be found"""
    pass


class APIConflictError(APIError):
    """Invalid payload to DCOR server"""
    pass


class APIGatewayTimeoutError(APIError):
    """Timeout due to network connection"""
    pass


class APINotFoundError(APIError):
    """Requested object not found on DCOR"""
    pass


class APIOutdatedError(APIError):
    """DCOR-Aid is outdated, the server requests a newer version"""
    pass


class NoAPIKeyError(APIError):
    """DCOR does not have an API key"""
    pass


class NoS3UploadAvailableError(BaseException):
    """Used for identifying DCOR servers that don't support direct S3 upload"""
    pass


class S3UploadError(BaseException):
    """raised when an upload to S3 failed"""
    pass
