from __future__ import annotations

from dataclasses import dataclass

from oceldb.core.ocel import OCEL
from oceldb.dsl import count, count_distinct, id_
from oceldb.inspect.types import event_types, object_types


@dataclass(frozen=True)
class OCELOverview:
    """
    High-level overview statistics of an OCEL.
    """

    event_count: int
    object_count: int
    object_record_count: int
    event_type_count: int
    object_type_count: int
    event_object_count: int
    object_object_count: int


def overview(ocel: OCEL) -> OCELOverview:
    """
    Return a high-level overview of the OCEL.

    Semantics:
        - `event_count` counts event rows, which are also logical events
        - `object_count` counts distinct logical objects
        - `object_record_count` counts physical object-history rows
    """
    event_count = int(ocel.events().table().agg(count().as_("event_count")).scalar())

    object_count = int(
        ocel.objects().table().agg(count_distinct(id_()).as_("object_count")).scalar()
    )

    object_record_count = int(
        ocel.objects().table().agg(count().as_("object_record_count")).scalar()
    )

    # These two still use the simple type-inspection helpers,
    # which now themselves are DSL-based.
    event_type_count = len(event_types(ocel))
    object_type_count = len(object_types(ocel))

    # Relation tables are not yet represented as root queries in the DSL,
    # so these remain SQL-based for now.
    event_object_count = int(
        ocel.sql(f"SELECT COUNT(*) FROM {ocel.schema}.event_object").fetchone()[0]  # type: ignore
    )
    object_object_count = int(
        ocel.sql(f"SELECT COUNT(*) FROM {ocel.schema}.object_object").fetchone()[0]  # type: ignore
    )

    return OCELOverview(
        event_count=event_count,
        object_count=object_count,
        object_record_count=object_record_count,
        event_type_count=event_type_count,
        object_type_count=object_type_count,
        event_object_count=event_object_count,
        object_object_count=object_object_count,
    )
