"""Pre-built structural filters for OCEL 2.0 process analysis.

Filters are organized by domain:

- :mod:`oceldb.filters.events` — event-level temporal filters
  (e.g. :class:`EventuallyFollows`, :class:`DirectlyFollows`)

Both the domain-specific import and the convenience shortcut work::

    from oceldb.filters.events import EventuallyFollows  # explicit
    from oceldb.filters import EventuallyFollows          # shortcut
"""

from oceldb.filters.events import DirectlyFollows, EventuallyFollows

__all__ = [
    "DirectlyFollows",
    "EventuallyFollows",
]
