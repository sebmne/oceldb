"""Shared type definitions used across the oceldb package."""

from dataclasses import dataclass
from enum import Enum

type ScalarValue = str | int | float
"""A Python value that can be safely converted to a SQL literal.

Booleans are explicitly excluded — use ``1`` / ``0`` or string literals
instead. See :func:`~oceldb.utils.sql_literal`.
"""


@dataclass
class Summary:
    """Lightweight statistics for an OCEL 2.0 event log.

    Returned by :meth:`Ocel.summary() <oceldb.ocel.Ocel.summary>`.

    Attributes:
        num_events: Total number of events.
        num_objects: Total number of objects.
        num_event_types: Number of distinct event types.
        num_object_types: Number of distinct object types.
        event_types: Sorted list of event type names.
        object_types: Sorted list of object type names.
        num_e2o_relations: Number of event-to-object (E2O) links.
        num_o2o_relations: Number of object-to-object (O2O) links.
    """

    num_events: int
    num_objects: int
    num_event_types: int
    num_object_types: int
    event_types: list[str]
    object_types: list[str]
    num_e2o_relations: int
    num_o2o_relations: int


class Domain(Enum):
    """The entity domain an expression or filter operates on.

    Used to enforce that only same-domain expressions can be combined
    with ``&`` / ``|`` and to route conditions to the correct tables
    during view materialization.
    """

    EVENT = "event"
    OBJECT = "object"
