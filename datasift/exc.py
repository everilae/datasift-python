#-----------------------------------------------------------------------------
# Module exceptions.
#-----------------------------------------------------------------------------
class AccessDeniedError(Exception):
    """
    This exception is thrown when an access denied error is returned by the
    DataSift API.
    """
    pass


class APIError(Exception):
    """
    Thrown for errors that occur while talking to the DataSift API.
    """
    pass


class CompileFailedError(Exception):
    """
    Thrown when compilation of a definition fails.
    """
    pass


class InvalidDataError(Exception):
    """
    Thrown whenever invalid data is detected.
    """
    pass


class RateLimitExceededError(Exception):
    """
    Thrown when you exceed the API rate limit.
    """
    pass


class StreamError(Exception):
    """
    Thrown for errors to do with the streaming API.
    """
    pass
