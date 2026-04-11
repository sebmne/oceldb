from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from oceldb.core.ocel import OCEL
from oceldb.dsl import asc, avg, count, count_distinct, desc, field, max_, min_, sum_
from oceldb.tables.query.table_query import TableQuery

from oceldb_ui.specs.table_spec import (
    AggItemSpec,
    GroupByItemSpec,
    OrderByItemSpec,
    SourceFieldSpec,
    TableSpec,
)

_CASTS: dict[str, type[Any]] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": datetime,
}


def compile_table_spec(ocel: OCEL, spec: TableSpec) -> TableQuery:
    query = _compile_table_source(ocel, spec["source"])

    for item in spec.get("select", []):
        expr = _compile_field_expr(item["expr"])
        alias = item.get("alias")
        query = query.select(expr.as_(alias) if alias else expr)

    group_by_items = spec.get("group_by", [])
    if group_by_items:
        query = query.group_by(
            *[_compile_group_by_item(item) for item in group_by_items]
        )

    agg_items = spec.get("agg", [])
    if agg_items:
        query = query.agg(*[_compile_agg_item(item) for item in agg_items])

    order_by_items = spec.get("order_by", [])
    if order_by_items:
        query = query.order_by(
            *[_compile_order_by_item(item) for item in order_by_items]
        )

    if spec.get("distinct", False):
        query = query.distinct()

    limit = spec.get("limit")
    if limit is not None:
        query = query.limit(limit)

    return query


def _compile_table_source(ocel: OCEL, source: str) -> TableQuery:
    if source == "event":
        return ocel.tables.events()
    if source == "object":
        return ocel.tables.objects()
    if source == "event_object":
        return ocel.tables.event_objects()
    if source == "object_object":
        return ocel.tables.object_objects()

    raise ValueError(f"Unsupported table source: {source!r}")


def _compile_group_by_item(item: GroupByItemSpec):
    return _compile_field_expr(item["expr"])


def _compile_agg_item(item: AggItemSpec):
    kind = item["kind"]
    expr_spec = item.get("expr")
    alias = item.get("alias")

    expr = None if expr_spec is None else _compile_field_expr(expr_spec)

    if kind == "count":
        result = count()
    elif kind == "count_distinct":
        if expr is None:
            raise ValueError("count_distinct requires an expression")
        result = count_distinct(expr)
    elif kind == "min":
        if expr is None:
            raise ValueError("min requires an expression")
        result = min_(expr)
    elif kind == "max":
        if expr is None:
            raise ValueError("max requires an expression")
        result = max_(expr)
    elif kind == "sum":
        if expr is None:
            raise ValueError("sum requires an expression")
        result = sum_(expr)
    elif kind == "avg":
        if expr is None:
            raise ValueError("avg requires an expression")
        result = avg(expr)
    else:
        raise ValueError(f"Unsupported aggregation kind: {kind!r}")

    return result.as_(alias) if alias else result


def _compile_order_by_item(item: OrderByItemSpec):
    direction = item["direction"]
    by = item["by"]

    expr = by if isinstance(by, str) else _compile_field_expr(cast(SourceFieldSpec, by))

    if direction == "ASC":
        return asc(expr)
    if direction == "DESC":
        return desc(expr)

    raise ValueError(f"Unsupported sort direction: {direction!r}")


def _compile_field_expr(spec: SourceFieldSpec):
    if spec["kind"] != "field":
        raise ValueError(f"Unsupported table expression kind: {spec['kind']!r}")

    return field(spec["name"], cast=_compile_cast(spec.get("cast")))


def _compile_cast(cast_name: str | None):
    if cast_name is None:
        return None

    try:
        return _CASTS[cast_name]
    except KeyError as e:
        raise ValueError(f"Unsupported cast name: {cast_name!r}") from e
