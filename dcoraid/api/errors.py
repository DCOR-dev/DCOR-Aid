class APIError(BaseException):
    """General CKANAPI error"""


class APIAuthorizationError(APIError):
    """User credentials are invalid"""


class APIBadGatewayError(APIError):
    """Cannot connect to DCOR server"""


class APIBadRequest(APIError):
    """An API command cannot be found"""


class APIConflictError(APIError):
    """Invalid payload to DCOR server"""


class APIGatewayTimeoutError(APIError):
    """Timeout due to network connection"""


class APINotFoundError(APIError):
    """Requested object not found on DCOR"""


class APIOutdatedError(APIError):
    """DCOR-Aid is outdated, the server requests a newer version"""


class NoAPIKeyError(APIError):
    """DCOR does not have an API token"""


class NoS3UploadAvailableError(BaseException):
    """Used for identifying DCOR servers that don't support direct S3 upload"""


class S3UploadError(BaseException):
    """Raised when an upload to S3 failed"""


class ServerUnderMaintenanceError(BaseException):
    """Raised when the DCOR server is under maintenance"""
