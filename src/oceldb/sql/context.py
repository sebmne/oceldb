from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Mapping

from oceldb.core.manifest import LogicalTableName

ExprScopeKind = Literal[
    "event",
    "object",
    "object_state",
    "object_state_at_event",
    "object_change",
    "event_object",
    "object_object",
    "grouped",
]
ObjectStateMode = Literal["latest", "as_of"]
_OBJECT_CHANGE_BASE_COLUMNS = (
    "ocel_id",
    "ocel_type",
    "ocel_time",
    "ocel_changed_field",
)


@dataclass(frozen=True)
class CompileContext:
    alias: str
    kind: ExprScopeKind
    table_refs: Mapping[LogicalTableName, str]
    object_change_columns: tuple[str, ...] = ()
    object_state_mode: ObjectStateMode | None = None
    object_state_as_of: date | datetime | str | None = None
    event_alias: str | None = None

    def table(self, name: LogicalTableName) -> str:
        return _quote_ident(self.table_refs[name])

    def with_alias(
        self,
        alias: str,
        *,
        kind: ExprScopeKind | None = None,
        event_alias: str | None = None,
    ) -> "CompileContext":
        return CompileContext(
            alias=alias,
            kind=self.kind if kind is None else kind,
            table_refs=self.table_refs,
            object_change_columns=self.object_change_columns,
            object_state_mode=self.object_state_mode,
            object_state_as_of=self.object_state_as_of,
            event_alias=self.event_alias if event_alias is None else event_alias,
        )


def render_object_state_source(
    table_refs: Mapping[LogicalTableName, str],
    object_change_columns: tuple[str, ...],
    *,
    mode: ObjectStateMode,
    as_of: date | datetime | str | None = None,
) -> str:
    custom_columns = [
        name
        for name in object_change_columns
        if name not in _OBJECT_CHANGE_BASE_COLUMNS
    ]
    filter_sql = ""
    if mode == "as_of":
        if as_of is None:
            raise ValueError("object state projection 'as_of' requires a timestamp")
        filter_sql = f'WHERE c."ocel_time" <= {_render_literal(as_of)}'

    filled_columns = ",\n                ".join(
        f'LAST_VALUE(c.{_quote_ident(name)} IGNORE NULLS) OVER state_window AS {_quote_ident(name)}'
        for name in custom_columns
    )
    outer_columns = ",\n            ".join(
        f's.{_quote_ident(name)} AS {_quote_ident(name)}'
        for name in custom_columns
    )
    ranked_columns = ",\n                ".join(
        f'ranked.{_quote_ident(name)} AS {_quote_ident(name)}'
        for name in custom_columns
    )
    select_filled = f",\n                {filled_columns}" if filled_columns else ""
    select_outer = f",\n            {outer_columns}" if outer_columns else ""
    select_ranked = f",\n                {ranked_columns}" if ranked_columns else ""

    return f"""
        SELECT
            o."ocel_id" AS "ocel_id",
            o."ocel_type" AS "ocel_type",
            s."ocel_time" AS "ocel_time"{select_outer}
        FROM {_quote_ident(table_refs["object"])} o
        LEFT JOIN (
            SELECT
                ranked."ocel_id",
                ranked."ocel_time"{select_ranked},
                ranked."_oceldb_state_rank"
            FROM (
                SELECT
                    c."ocel_id",
                    c."ocel_time"{select_filled},
                    ROW_NUMBER() OVER latest_window AS "_oceldb_state_rank"
                FROM {_quote_ident(table_refs["object_change"])} c
                {filter_sql}
                WINDOW
                    state_window AS (
                        PARTITION BY c."ocel_id"
                        ORDER BY c."ocel_time"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    latest_window AS (
                        PARTITION BY c."ocel_id"
                        ORDER BY c."ocel_time" DESC
                    )
            ) ranked
            WHERE ranked."_oceldb_state_rank" = 1
        ) s
          ON o."ocel_id" = s."ocel_id"
    """


def render_object_state_source_at_event(
    table_refs: Mapping[LogicalTableName, str],
    object_change_columns: tuple[str, ...],
    *,
    event_alias: str,
) -> str:
    custom_columns = [
        name
        for name in object_change_columns
        if name not in _OBJECT_CHANGE_BASE_COLUMNS
    ]
    filled_columns = ",\n                ".join(
        f'LAST_VALUE(c.{_quote_ident(name)} IGNORE NULLS) OVER state_window AS {_quote_ident(name)}'
        for name in custom_columns
    )
    outer_columns = ",\n            ".join(
        f's.{_quote_ident(name)} AS {_quote_ident(name)}'
        for name in custom_columns
    )
    ranked_columns = ",\n                ".join(
        f'ranked.{_quote_ident(name)} AS {_quote_ident(name)}'
        for name in custom_columns
    )
    select_filled = f",\n                {filled_columns}" if filled_columns else ""
    select_outer = f",\n            {outer_columns}" if outer_columns else ""
    select_ranked = f",\n                {ranked_columns}" if ranked_columns else ""
    event_ref = _quote_ident(event_alias)

    return f"""
        SELECT
            o."ocel_id" AS "ocel_id",
            o."ocel_type" AS "ocel_type",
            s."ocel_time" AS "ocel_time"{select_outer}
        FROM {_quote_ident(table_refs["object"])} o
        LEFT JOIN LATERAL (
            SELECT
                ranked."ocel_time"{select_ranked}
            FROM (
                SELECT
                    c."ocel_time"{select_filled},
                    ROW_NUMBER() OVER latest_window AS "_oceldb_state_rank"
                FROM {_quote_ident(table_refs["object_change"])} c
                WHERE c."ocel_id" = o."ocel_id"
                  AND c."ocel_time" <= {event_ref}."ocel_time"
                WINDOW
                    state_window AS (
                        PARTITION BY c."ocel_id"
                        ORDER BY c."ocel_time"
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),
                    latest_window AS (
                        PARTITION BY c."ocel_id"
                        ORDER BY c."ocel_time" DESC
                    )
            ) ranked
            WHERE ranked."_oceldb_state_rank" = 1
        ) s ON TRUE
    """


def _quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _render_literal(value: date | datetime | str) -> str:
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
