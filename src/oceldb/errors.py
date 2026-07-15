"""Public exception hierarchy for oceldb operations."""


class OCELDBError(Exception):
    """Base class for failures reported by oceldb itself."""


class OCELValidationError(OCELDBError):
    """A logical OCEL dataset violates the canonical table contract."""


class OCELIOError(OCELDBError):
    """An OCEL import, export, or storage operation failed."""
