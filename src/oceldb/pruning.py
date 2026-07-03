"""Assemble a pruned sub-log from a chosen set of events, objects, and relations.

Every sub-log operation (filters, ``view``, ``project``) reduces to the same
final step: keep a set of events, a set of objects, and the event-to-object
relations between them, then prune ``object_changes`` and the O2O relations down
to the surviving objects. :func:`prune_log` centralizes that assembly so callers
only decide *which* rows survive.
"""

import polars as pl

from oceldb import schema as s
from oceldb.ocel import OCEL


def prune_log(
    ocel: OCEL,
    *,
    events: pl.LazyFrame,
    objects: pl.LazyFrame,
    e2o: pl.LazyFrame,
) -> OCEL:
    """Build a sub-log from surviving *events*, *objects*, and E2O relations.

    ``object_changes`` and O2O relations are pruned to the objects present in
    *objects*. The caller is responsible for making *events*, *objects*, and
    *e2o* mutually consistent (for example by deriving them from a common set of
    kept relations).

    Args:
        ocel: The source log supplying ``object_changes`` and O2O relations.
        events: The events frame to keep.
        objects: The objects frame to keep. Its ``ocel_id`` column defines the
            surviving objects.
        e2o: The event-to-object relations to keep.

    Returns:
        A new ``OCEL`` whose ``object_changes`` and O2O relations reference only
        the objects in *objects*.
    """
    kept_ids = objects.select(s.OCEL_ID)
    kept_object_ids = kept_ids.rename({s.OCEL_ID: s.OCEL_OBJECT_ID})
    return OCEL(
        events=events,
        objects=objects,
        object_changes=ocel.object_changes().join(kept_ids, on=s.OCEL_ID, how="semi"),
        o2o=ocel.object_object()
        .join(
            kept_object_ids,
            left_on=s.OCEL_SOURCE_ID,
            right_on=s.OCEL_OBJECT_ID,
            how="semi",
        )
        .join(
            kept_object_ids,
            left_on=s.OCEL_TARGET_ID,
            right_on=s.OCEL_OBJECT_ID,
            how="semi",
        ),
        e2o=e2o,
    )
