from __future__ import annotations

from datetime import date, datetime

_OBJECT_CHANGE_BASE_COLUMNS = (
    "ocel_id",
    "ocel_type",
    "ocel_time",
    "ocel_changed_field",
)


def custom_object_change_columns(
    object_change_columns: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(
        name
        for name in object_change_columns
        if name not in _OBJECT_CHANGE_BASE_COLUMNS
    )


def render_object_change_batches_source(
    object_change_columns: tuple[str, ...],
    *,
    object_types: tuple[str, ...] = (),
    as_of: date | datetime | str | None = None,
) -> str:
    custom_columns = custom_object_change_columns(object_change_columns)
    grouped_columns = ",\n            ".join(
        f'MAX(c.{_quote_ident(name)}) AS {_quote_ident(name)}'
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
