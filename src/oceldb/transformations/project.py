"""Project filter: restrict an OCEL to events involving a set of objects."""

import polars as pl

from oceldb import schema as s
from oceldb.utils.step import step
from oceldb.core.pruning import sublog_from_event_ids
from oceldb.ocel import OCEL


@step
def project(ocel: OCEL, *object_ids: str) -> OCEL:
    """Project *ocel* onto a set of objects, returning the induced sub-log.

    Formalises the standard OCEL projection :math:`L^U`: given a set of
    objects *U*, remove every event in which no member of *U* participates,
    then remove every object that shares no event with the survivors, then
    drop all relations that touch a removed event or object.

    Args:
        ocel: The source log. Omit to get a pipe step instead.
        *object_ids: One or more ``ocel_id`` values to project onto.

    Returns:
        A new ``OCEL`` whose events are exactly those involving at least one
        of *object_ids*, whose objects are all objects co-participating in
        those events, and whose E2O / O2O relations, object changes, and
        attribute columns are pruned to that connected core.

    Examples:
        >>> from oceldb.transformations import project
        >>> sub = project(ocel, "order-42")
        >>> sub = project(ocel, "order-42", "order-99")
        >>> result = ocel >> view(object_types=["order"]) >> project("order-42")
    """
    kept_events = (
        ocel.event_object()
        .filter(pl.col(s.OCEL_OBJECT_ID).is_in(list(object_ids)))
        .select(s.OCEL_EVENT_ID)
        .unique()
    )
    return sublog_from_event_ids(ocel, kept_events)
