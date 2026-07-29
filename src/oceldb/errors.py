"""Public exception hierarchy for oceldb."""


class OCELDBError(Exception):
    """Base class for failures reported by oceldb."""


class OCELFormatError(OCELDBError, ValueError):
    """A native snapshot does not conform to the supported storage format."""


class OCELStorageError(OCELDBError, OSError):
    """A native snapshot cannot be installed safely."""


class OCELValidationError(OCELDBError, ValueError):
    """A logical OCEL violates the canonical table contract."""
