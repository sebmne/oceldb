"""Check and repair the referential integrity of an OCEL.

:class:`OCEL` deliberately trusts the frames it is built from, and the file
readers can produce logs with dangling relations, duplicate ids, or events
without a timestamp. :func:`validate` reports these problems and :func:`clean`
repairs them, so both are useful right after importing an external log.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast, overload

import polars as pl

from oceldb import schema as s
from oceldb.utils._step import _step
from oceldb.ocel import OCEL


@dataclass(frozen=True)
class ValidationReport:
    """Counts of referential-integrity problems found by :func:`validate`.

    Every field is a row count; a fully valid log has zero everywhere and
    :attr:`is_valid` is ``True``. :func:`clean` repairs exactly these problems.
    """

    duplicate_event_ids: int
    duplicate_object_ids: int
    events_missing_time: int
    dangling_e2o_events: int
    dangling_e2o_objects: int
    dangling_o2o_sources: int
    dangling_o2o_targets: int
    orphan_object_changes: int

    @property
    def is_valid(self) -> bool:
        """``True`` when no integrity problems were found."""
        return not any(
            (
                self.duplicate_event_ids,
                self.duplicate_object_ids,
                self.events_missing_time,
                self.dangling_e2o_events,
                self.dangling_e2o_objects,
                self.dangling_o2o_sources,
                self.dangling_o2o_targets,
                self.orphan_object_changes,
            )
        )


def validate(ocel: OCEL) -> ValidationReport:
    """Report referential-integrity problems in *ocel* without changing it.

    Args:
        ocel: The log to inspect.

    Returns:
        A :class:`ValidationReport` counting duplicate event/object ids, events
        with a null timestamp, E2O relations whose event or object id is absent
        from the ``events`` / ``objects`` tables, O2O relations whose source or
        target id is absent from ``objects``, and object changes for objects not
        present in ``objects``.

    Examples:
        >>> from oceldb.validation import validate
        >>> report = validate(ocel)
        >>> report.is_valid
        >>> report.dangling_e2o_objects
    """
    events = ocel.events()
    objects = ocel.objects()
    event_ids = events.select(s.OCEL_ID)
    object_ids = objects.select(s.OCEL_ID)

    return ValidationReport(
        duplicate_event_ids=_duplicate_count(events, s.OCEL_ID),
        duplicate_object_ids=_duplicate_count(objects, s.OCEL_ID),
        events_missing_time=_count(events.filter(pl.col(s.OCEL_TIME).is_null())),
        dangling_e2o_events=_dangling(ocel.event_object(), s.OCEL_EVENT_ID, event_ids),
        dangling_e2o_objects=_dangling(
            ocel.event_object(), s.OCEL_OBJECT_ID, object_ids
        ),
        dangling_o2o_sources=_dangling(
            ocel.object_object(), s.OCEL_SOURCE_ID, object_ids
        ),
        dangling_o2o_targets=_dangling(
            ocel.object_object(), s.OCEL_TARGET_ID, object_ids
        ),
        orphan_object_changes=_dangling(ocel.object_changes(), s.OCEL_ID, object_ids),
    )


@overload
def clean(ocel: OCEL) -> OCEL: ...


@overload
def clean() -> Callable[[OCEL], OCEL]: ...


@_step
def clean(ocel: OCEL) -> OCEL:
    """Repair *ocel*'s referential integrity, returning a corrected log.

    Events without a timestamp are dropped (they cannot take part in temporal
    operations); duplicate event and object ids are collapsed keeping the first
    occurrence; E2O relations pointing at a missing event or object, O2O
    relations pointing at a missing object, and object changes for a missing
    object are dropped; events are sorted by ``ocel_time``. Surviving events and
    objects are otherwise kept — use the filters to drop unconnected rows.

    Args:
        ocel: The source log. Omit to get a pipe step instead.

    Returns:
        A new ``OCEL`` for which :func:`validate` reports no problems.

    Examples:
        >>> from oceldb.validation import clean
        >>> repaired = clean(ocel)
        >>> repaired = ocel >> clean()
    """
    events = (
        ocel.events()
        .filter(pl.col(s.OCEL_TIME).is_not_null())
        .unique(subset=[s.OCEL_ID], keep="first", maintain_order=True)
        .sort(s.OCEL_TIME)
    )
    objects = ocel.objects().unique(
        subset=[s.OCEL_ID], keep="first", maintain_order=True
    )
    event_ids = events.select(pl.col(s.OCEL_ID).alias(s.OCEL_EVENT_ID))
    object_ids = objects.select(s.OCEL_ID)
    object_ids_e2o = objects.select(pl.col(s.OCEL_ID).alias(s.OCEL_OBJECT_ID))
    object_ids_src = objects.select(pl.col(s.OCEL_ID).alias(s.OCEL_SOURCE_ID))
    object_ids_tgt = objects.select(pl.col(s.OCEL_ID).alias(s.OCEL_TARGET_ID))

    e2o = (
        ocel.event_object()
        .join(event_ids, on=s.OCEL_EVENT_ID, how="semi")
        .join(object_ids_e2o, on=s.OCEL_OBJECT_ID, how="semi")
    )
    o2o = (
        ocel.object_object()
        .join(object_ids_src, on=s.OCEL_SOURCE_ID, how="semi")
        .join(object_ids_tgt, on=s.OCEL_TARGET_ID, how="semi")
    )
    object_changes = ocel.object_changes().join(object_ids, on=s.OCEL_ID, how="semi")

    return OCEL(
        events=events,
        objects=objects,
        object_changes=object_changes,
        o2o=o2o,
        e2o=e2o,
    )


def _duplicate_count(frame: pl.LazyFrame, column: str) -> int:
    total = _count(frame.select(column))
    distinct = _count(frame.select(column).unique())
    return total - distinct


def _dangling(frame: pl.LazyFrame, column: str, valid_ids: pl.LazyFrame) -> int:
    reference = valid_ids.rename({valid_ids.collect_schema().names()[0]: column})
    return _count(frame.select(column).join(reference, on=column, how="anti"))


def _count(frame: pl.LazyFrame) -> int:
    return int(cast(int, frame.select(pl.len()).collect().item()))
