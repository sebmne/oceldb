"""SQL emission for sources that need manifest context.

This module owns two tasks: rendering the root SourcePlan (including the
LAST_VALUE/ROW_NUMBER reconstruction for ``ObjectStateSource``) and rendering
nested "scope sources" used by relation predicates (e.g. the object table a
``cooccurs_with`` candidate is matched against).
"""

from __future__ import annotations

from datetime import date, datetime

from oceldb.compile.context import CompileContext, quote_ident
from oceldb.plan.sources import (
    EventObjectSource,
    EventOccurrenceSource,
    EventSource,
    ObjectChangeSource,
    ObjectObjectSource,
    ObjectSource,
    ObjectStateSource,
    Source,
)

_OBJECT_CHANGE_BASE_COLUMNS = (
    "ocel_id",
    "ocel_type",
    "ocel_time",
    "ocel_changed_field",
)


def render_source(source: Source, alias: str, ctx: CompileContext) -> str:
    """Render the root ``FROM`` fragment for a plan source."""
    if isinstance(source, ObjectStateSource):
        return _render_object_state_root(source, alias, ctx)
    if isinstance(
        source,
        (
            EventSource,
            ObjectSource,
            ObjectChangeSource,
            EventObjectSource,
            ObjectObjectSource,
            EventOccurrenceSource,
        ),
    ):
        return source.render(alias)
    raise TypeError(f"Unsupported source for rendering: {type(source).__name__}")


def _render_object_state_root(
    source: ObjectStateSource,
    alias: str,
    ctx: CompileContext,
) -> str:
    if source.mode is None:
        raise ValueError(
            "object_states(...) queries require an explicit temporal projection; "
            "call .latest() or .as_of(timestamp)"
        )
    mode_name, as_of = source.mode
    if mode_name == "as_of" and as_of is None:
        raise ValueError("object state projection 'as_of' requires a timestamp")

    inner = render_object_state_source(
        ctx.object_change_columns,
        mode=mode_name,
        as_of=as_of,
        object_types=source.selected_types,
    )
    return f"({inner}) AS {alias}"


def render_object_state_source(
    object_change_columns: tuple[str, ...],
    *,
    mode: str,
    as_of: date | datetime | None,
    object_types: tuple[str, ...] = (),
) -> str:
    custom_columns = _custom_columns(object_change_columns)
    batch_updates_sql = render_object_change_batches_source(
        object_change_columns,
        object_types=object_types,
        as_of=as_of if mode == "as_of" else None,
    )

    filled_columns = ",\n                ".join(
        f'LAST_VALUE(bu.{quote_ident(name)} IGNORE NULLS) OVER state_window '
        f"AS {quote_ident(name)}"
        for name in custom_columns
    )
    outer_columns = ",\n            ".join(
        f"s.{quote_ident(name)} AS {quote_ident(name)}"
        for name in custom_columns
    )
    ranked_columns = ",\n                ".join(
        f"ranked.{quote_ident(name)} AS {quote_ident(name)}"
        for name in custom_columns
    )
    select_filled = f",\n                {filled_columns}" if filled_columns else ""
    select_outer = f",\n            {outer_columns}" if outer_columns else ""
    select_ranked = f",\n                {ranked_columns}" if ranked_columns else ""

    object_filter = ""
    if object_types:
        types_sql = ", ".join(_render_literal(t) for t in object_types)
        object_filter = f'WHERE o."ocel_type" IN ({types_sql})'

    return f"""
        SELECT
            o."ocel_id" AS "ocel_id",
            o."ocel_type" AS "ocel_type",
            s."ocel_time" AS "ocel_time"{select_outer}
        FROM "object" o
        LEFT JOIN (
            SELECT
                ranked."ocel_id",
                ranked."ocel_time"{select_ranked},
                ranked."_oceldb_state_rank"
            FROM (
                SELECT
                    bu."ocel_id",
                    bu."ocel_time"{select_filled},
                    ROW_NUMBER() OVER latest_window AS "_oceldb_state_rank"
                FROM ({batch_updates_sql}) bu
                WINDOW
                    state_window AS (
                        PARTITION BY bu."ocel_id"
                        ORDER BY bu."ocel_time"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    latest_window AS (
                        PARTITION BY bu."ocel_id"
                        ORDER BY bu."ocel_time" DESC
                    )
            ) ranked
            WHERE ranked."_oceldb_state_rank" = 1
        ) s
          ON o."ocel_id" = s."ocel_id"
        {object_filter}
    """


def render_object_state_source_at_event(
    object_change_columns: tuple[str, ...],
    *,
    event_alias: str,
) -> str:
    custom_columns = _custom_columns(object_change_columns)
    batch_updates_sql = render_object_change_batches_source(object_change_columns)
    filled_columns = ",\n                ".join(
        f'LAST_VALUE(bu.{quote_ident(name)} IGNORE NULLS) OVER state_window '
        f"AS {quote_ident(name)}"
        for name in custom_columns
    )
    outer_columns = ",\n            ".join(
        f"s.{quote_ident(name)} AS {quote_ident(name)}"
        for name in custom_columns
    )
    ranked_columns = ",\n                ".join(
        f"ranked.{quote_ident(name)} AS {quote_ident(name)}"
        for name in custom_columns
    )
    select_filled = f",\n                {filled_columns}" if filled_columns else ""
    select_outer = f",\n            {outer_columns}" if outer_columns else ""
    select_ranked = f",\n                {ranked_columns}" if ranked_columns else ""
    event_ref = quote_ident(event_alias)

    return f"""
        SELECT
            o."ocel_id" AS "ocel_id",
            o."ocel_type" AS "ocel_type",
            s."ocel_time" AS "ocel_time"{select_outer}
        FROM "object" o
        LEFT JOIN LATERAL (
            SELECT
                ranked."ocel_time"{select_ranked}
            FROM (
                SELECT
                    bu."ocel_time"{select_filled},
                    ROW_NUMBER() OVER latest_window AS "_oceldb_state_rank"
                FROM ({batch_updates_sql}) bu
                WHERE bu."ocel_id" = o."ocel_id"
                  AND bu."ocel_time" <= {event_ref}."ocel_time"
                WINDOW
                    state_window AS (
                        PARTITION BY bu."ocel_id"
                        ORDER BY bu."ocel_time"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    latest_window AS (
                        PARTITION BY bu."ocel_id"
                        ORDER BY bu."ocel_time" DESC
                    )
            ) ranked
            WHERE ranked."_oceldb_state_rank" = 1
        ) s ON TRUE
    """


def render_object_change_batches_source(
    object_change_columns: tuple[str, ...],
    *,
    object_types: tuple[str, ...] = (),
    as_of: date | datetime | str | None = None,
) -> str:
    """Render the GROUP-BY-collapsed object_change source.

    One row per ``(ocel_id, ocel_time)``: attributes updated simultaneously
    are merged together so downstream window functions see a single point in
    time instead of per-attribute rows.
    """
    custom_columns = _custom_columns(object_change_columns)
    grouped_columns = ",\n            ".join(
        f"MAX(c.{quote_ident(name)}) AS {quote_ident(name)}"
        for name in custom_columns
    )
    select_grouped = f",\n            {grouped_columns}" if grouped_columns else ""

    predicates: list[str] = []
    if object_types:
        type_values = ", ".join(_render_literal(value) for value in object_types)
        predicates.append(f'c."ocel_type" IN ({type_values})')
    if as_of is not None:
        predicates.append(f'c."ocel_time" <= {_render_literal(as_of)}')

    where_sql = ""
    if predicates:
        where_sql = "WHERE " + " AND ".join(predicates)

    return f"""
        SELECT
            c."ocel_id",
            c."ocel_time"{select_grouped}
        FROM "object_change" c
        {where_sql}
        GROUP BY c."ocel_id", c."ocel_time"
    """


def render_scope_source(ctx: CompileContext) -> str:
    """Render the FROM fragment for a nested scope introduced by a relation."""
    if ctx.kind == "event":
        return f'{ctx.table("event")} {ctx.alias}'
    if ctx.kind == "object":
        return f'{ctx.table("object")} {ctx.alias}'
    if ctx.kind == "object_state":
        mode_name, as_of = ctx.object_state_mode or ("latest", None)
        inner = render_object_state_source(
            ctx.object_change_columns,
            mode=mode_name,
            as_of=as_of,
        )
        return f"({inner}) {ctx.alias}"
    if ctx.kind == "object_state_at_event":
        if ctx.event_alias is None:
            raise ValueError("object_state_at_event scope requires an event alias")
        inner = render_object_state_source_at_event(
            ctx.object_change_columns,
            event_alias=ctx.event_alias,
        )
        return f"LATERAL ({inner}) {ctx.alias}"
    if ctx.kind == "object_change":
        return f'{ctx.table("object_change")} {ctx.alias}'
    raise TypeError(f"Unsupported scope kind for scope source: {ctx.kind!r}")


def _custom_columns(object_change_columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        name for name in object_change_columns if name not in _OBJECT_CHANGE_BASE_COLUMNS
    )


def _render_literal(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


__all__ = [
    "render_object_change_batches_source",
    "render_object_state_source",
    "render_object_state_source_at_event",
    "render_scope_source",
    "render_source",
]
