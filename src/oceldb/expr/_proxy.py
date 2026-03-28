"""Proxy singletons — ``event`` and ``obj`` — for ergonomic column access.

These are the primary entry points for building filter expressions::

    from oceldb import event, obj

    event.type == "Create Order"      # event type filter
    event.total_price > 100           # event attribute filter
    obj.customer_name == "Alice"      # object attribute filter
"""

from __future__ import annotations

from oceldb.expr._col import Col
from oceldb.types import Domain

_ALIASES: dict[str, str] = {
    "type": "ocel_type",
    "time": "ocel_time",
    "id": "ocel_id",
}


class Proxy:
    """Attribute-access proxy that produces :class:`~oceldb.expr._col.Col` instances.

    Translates short attribute names to their OCEL column equivalents via
    built-in aliases:

    - ``proxy.type``  → ``Col(domain, "ocel_type")``
    - ``proxy.time``  → ``Col(domain, "ocel_time")``
    - ``proxy.id``    → ``Col(domain, "ocel_id")``

    Any other attribute name is passed through verbatim, allowing access
    to per-type columns like ``event.total_price`` or ``obj.customer_name``.

    Use ``proxy["ocel_type"]`` (bracket syntax) to bypass alias resolution.

    Args:
        domain: The entity domain this proxy creates columns for.
    """

    __slots__ = ("_domain",)

    def __init__(self, domain: Domain) -> None:
        self._domain = domain

    def __getattr__(self, name: str) -> Col:
        """Resolve an attribute name to a :class:`Col`, applying aliases.

        Args:
            name: The attribute name (e.g. ``"type"`` or ``"total_price"``).

        Returns:
            A :class:`Col` bound to this proxy's domain.
        """
        return Col(self._domain, _ALIASES.get(name, name))

    def __getitem__(self, name: str) -> Col:
        """Resolve a column name to a :class:`Col` without alias resolution.

        Args:
            name: The raw column name (e.g. ``"ocel_type"``).

        Returns:
            A :class:`Col` bound to this proxy's domain.
        """
        return Col(self._domain, name)

    def __repr__(self) -> str:
        return self._domain.value


event = Proxy(Domain.EVENT)
"""Proxy singleton for building event filter expressions."""

obj = Proxy(Domain.OBJECT)
"""Proxy singleton for building object filter expressions."""
