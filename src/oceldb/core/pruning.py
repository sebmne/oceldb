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
    kept_ids = objects.select(s.OCEL_ID).unique()
    kept_object_ids = kept_ids.rename({s.OCEL_ID: s.OCEL_OBJECT_ID})
    return ocel._replace(
        events=events,
        objects=objects,
        object_changes=ocel.object_changes().join(kept_ids, on=s.OCEL_ID, how="semi"),
        object_object=ocel.object_object()
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
        event_object=e2o,
    )


def sublog_from_relations(
    ocel: OCEL,
    relations: pl.LazyFrame,
    *,
    events: pl.LazyFrame | None = None,
) -> OCEL:
    """Build the connected sub-log induced by surviving E2O *relations*.

    Passing *events* preserves selected events without relations. This is useful
    for event predicates and zero-count filters; otherwise both sides are
    derived from the surviving relations.
    """
    kept_events = relations.select(s.OCEL_EVENT_ID).unique()
    kept_objects = relations.select(s.OCEL_OBJECT_ID).unique()
    if events is None:
        events = ocel.events().join(
            kept_events, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
        )
    objects = ocel.objects().join(
        kept_objects, left_on=s.OCEL_ID, right_on=s.OCEL_OBJECT_ID, how="semi"
    )
    return prune_log(ocel, events=events, objects=objects, e2o=relations)


def sublog_from_event_ids(ocel: OCEL, event_ids: pl.LazyFrame) -> OCEL:
    """Build a sub-log from an ``ocel_event_id`` frame."""
    relations = ocel.event_object().join(event_ids, on=s.OCEL_EVENT_ID, how="semi")
    events = ocel.events().join(
        event_ids, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
    )
    return sublog_from_relations(ocel, relations, events=events)


def sublog_from_object_ids(
    ocel: OCEL,
    object_ids: pl.LazyFrame,
    *,
    objects: pl.LazyFrame | None = None,
) -> OCEL:
    """Build a sub-log from an authoritative ``ocel_object_id`` selection.

    Selected objects remain present even when they have no E2O relation. Events
    are induced by the surviving relations, so unrelated events are removed.
    ``objects`` optionally supplies the already-filtered identity frame.
    """
    relations = ocel.event_object().join(object_ids, on=s.OCEL_OBJECT_ID, how="semi")
    event_ids = relations.select(s.OCEL_EVENT_ID).unique()
    events = ocel.events().join(
        event_ids, left_on=s.OCEL_ID, right_on=s.OCEL_EVENT_ID, how="semi"
    )
    if objects is None:
        objects = ocel.objects().join(
            object_ids,
            left_on=s.OCEL_ID,
            right_on=s.OCEL_OBJECT_ID,
            how="semi",
        )
    return prune_log(ocel, events=events, objects=objects, e2o=relations)
