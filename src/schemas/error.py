class DisconnectedOriginError(Exception):
    """Raised when the user-provided starting point is too far away from the street network."""

    pass


class BufferExceedsNetworkError(Exception):
    """Raised when the buffer for computing an isochrone extends futher than our network cells."""

    pass
