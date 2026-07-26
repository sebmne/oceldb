"""Project an OCEL onto events involving selected objects."""

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.pruning import sublog_from_event_ids
from oceldb.operations.step import step
from oceldb.types import normalize_strings


@step
def project(ocel: OCEL, *object_ids: str) -> OCEL:
    """Project *ocel* onto a set of objects, returning the induced sub-log.

    Formalises the standard OCEL projection: given a set of
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
        object-change rows are pruned to that connected core.

    Raises:
        TypeError: If an object id is not a string.
        ValueError: If no object id is supplied or an id is empty.

    Examples:
        >>> from oceldb.operations.transformations import project
        >>> sub = project(ocel, "order-42")
        >>> sub = project(ocel, "order-42", "order-99")
        >>> result = ocel >> view(object_types=["order"]) >> project("order-42")
    """
    selected = normalize_strings(object_ids, name="object_ids", non_empty=True)
    kept_events = ocel.e2o(object=selected).select(s.OCEL_EVENT_ID).unique()
    return sublog_from_event_ids(ocel, kept_events)
