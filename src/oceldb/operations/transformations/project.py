"""Project filter: restrict an OCEL to events involving a set of objects."""

import polars as pl

from oceldb.core import schema as s
from oceldb.ocel import OCEL
from oceldb.operations.pruning import sublog_from_event_ids
from oceldb.operations.step import step


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

    Raises:
        ValueError: If no object id is supplied or an id is unknown.

    Examples:
        >>> from oceldb.operations.transformations import project
        >>> sub = project(ocel, "order-42")
        >>> sub = project(ocel, "order-42", "order-99")
        >>> result = ocel >> view(object_types=["order"]) >> project("order-42")
    """
    selected = _validate_object_ids(ocel, object_ids)
    kept_events = (
        ocel.e2o()
        .filter(pl.col(s.OCEL_OBJECT_ID).is_in(selected))
        .select(s.OCEL_EVENT_ID)
        .unique()
    )
    return sublog_from_event_ids(ocel, kept_events)


def _validate_object_ids(ocel: OCEL, values: tuple[object, ...]) -> list[str]:
    if not values:
        raise ValueError("project requires at least one object id.")
    selected: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise TypeError("object ids must be strings.")
        if not value:
            raise ValueError("object ids must not be empty.")
        if value not in selected:
            selected.append(value)
    known = set(
        ocel.objects()
        .filter(pl.col(s.OCEL_ID).is_in(selected))
        .select(s.OCEL_ID)
        .collect()
        .get_column(s.OCEL_ID)
        .to_list()
    )
    if missing := sorted(set(selected) - known):
        raise ValueError(f"Unknown object ids: {missing}.")
    return selected
