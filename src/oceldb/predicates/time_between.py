from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Literal

from oceldb.expr import Predicate, Table, col
from oceldb.io.sql import sql_string
from oceldb.ocel import OCEL

TimeBetweenOperator = Literal["<", "<=", "=", "!=", ">", ">="]


class TimeBetween(Predicate):
    """Comparable object-event duration predicate."""

    __slots__ = (
        "_from_event_type",
        "_object_type",
        "_ocel",
        "_ordered",
        "_to_event_type",
    )

    def __init__(
        self,
        ocel: OCEL,
        from_event_type: str,
        to_event_type: str,
        *,
        object_type: str | None,
        ordered: bool,
    ) -> None:
        self._ocel = ocel
        self._from_event_type = from_event_type
        self._to_event_type = to_event_type
        self._object_type = object_type
        self._ordered = ordered
        super().__init__(self._predicate().raw())

    def between(
        self,
        min: timedelta,
        max: timedelta,
        *,
        inclusive: bool = True,
    ) -> Predicate:
        """Match durations between *min* and *max*."""
        lower_op: TimeBetweenOperator = ">=" if inclusive else ">"
        upper_op: TimeBetweenOperator = "<=" if inclusive else "<"
        return self._predicate(((lower_op, min), (upper_op, max)))

    def __lt__(self, other: object) -> Predicate:
        return self._compare("<", other)

    def __le__(self, other: object) -> Predicate:
        return self._compare("<=", other)

    def __gt__(self, other: object) -> Predicate:
        return self._compare(">", other)

    def __ge__(self, other: object) -> Predicate:
        return self._compare(">=", other)

    def __eq__(self, other: object) -> Predicate:  # type: ignore[override]
        return self._compare("=", other)

    def __ne__(self, other: object) -> Predicate:  # type: ignore[override]
        return self._compare("!=", other)

    def __hash__(self) -> int:
        return id(self)

    def _compare(self, operator: TimeBetweenOperator, other: object) -> Predicate:
        if not isinstance(other, timedelta):
            return NotImplemented  # type: ignore[return-value]
        return self._predicate(((operator, other),))

    def _predicate(
        self,
        bounds: Sequence[tuple[TimeBetweenOperator, timedelta]] = (),
    ) -> Predicate:
        conditions = [
            f"from_rel.ocel_event_type = {sql_string(self._from_event_type)}",
            f"to_rel.ocel_event_type = {sql_string(self._to_event_type)}",
            "from_rel.ocel_event_id <> to_rel.ocel_event_id",
        ]
        if self._object_type is not None:
            conditions.append(
                f"from_rel.ocel_object_type = {sql_string(self._object_type)}"
            )
        if self._ordered:
            conditions.append("to_event.ocel_time >= from_event.ocel_time")
        for operator, value in bounds:
            conditions.append(
                "to_event.ocel_time - from_event.ocel_time "
                f"{operator} {_timedelta_interval_sql(value)}"
            )

        where = "\n            AND ".join(conditions)
        sql = f"""
            SELECT DISTINCT from_rel.ocel_object_id AS ocel_id
            FROM event_object from_rel
            JOIN events from_event
              ON from_event.ocel_id = from_rel.ocel_event_id
            JOIN event_object to_rel
              ON to_rel.ocel_object_id = from_rel.ocel_object_id
             AND to_rel.ocel_object_type = from_rel.ocel_object_type
            JOIN events to_event
              ON to_event.ocel_id = to_rel.ocel_event_id
            WHERE {where}
        """
        matched_ids = Table(self._ocel.con.sql(sql))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
        return col("ocel_id").isin(matched_ids["ocel_id"])


def time_between(
    ocel: OCEL,
    from_event_type: str,
    to_event_type: str,
    *,
    object_type: str | None = None,
    ordered: bool = True,
) -> TimeBetween:
    """Return a comparable duration between two linked event types.

    The returned object can be used directly as an existence predicate, or
    compared with ``datetime.timedelta`` bounds.
    """
    return TimeBetween(
        ocel,
        from_event_type,
        to_event_type,
        object_type=object_type,
        ordered=ordered,
    )


def _timedelta_interval_sql(value: timedelta) -> str:
    microseconds = (
        value.days * 24 * 60 * 60 * 1_000_000
        + value.seconds * 1_000_000
        + value.microseconds
    )
    return f"INTERVAL {sql_string(f'{microseconds} microseconds')}"
