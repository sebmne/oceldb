from datetime import datetime
from typing import Any, cast

from oceldb.ast.base import BoolExpr, ScalarExpr
from oceldb.core.ocel import OCEL
from oceldb.dsl import attr, field, has_event, linked, related
from oceldb.views.query.view_query import ViewQuery

from oceldb_ui.specs.view_spec import (
    AttrRefSpec,
    ComparisonFilterSpec,
    ExprRefSpec,
    FieldRefSpec,
    HasEventExistsFilterSpec,
    LinkedExistsFilterSpec,
    NullFilterSpec,
    RelatedExistsFilterSpec,
    ViewFilterSpec,
    ViewSpec,
)

_CASTS: dict[str, type[Any]] = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
    "datetime": datetime,
}


def compile_view_spec(ocel: OCEL, spec: ViewSpec) -> ViewQuery:
    root = spec["root"]
    types = tuple(spec.get("types", []))
    filters = spec.get("filters", [])

    if root == "event":
        query = ocel.events(*types)
    elif root == "object":
        query = ocel.objects(*types)
    else:
        raise ValueError(f"Unsupported view root: {root!r}")

    compiled_filters: list[BoolExpr] = [_compile_view_filter(f) for f in filters]
    if compiled_filters:
        query = query.filter(*compiled_filters)

    return query


def _compile_view_filter(spec: ViewFilterSpec) -> BoolExpr:
    kind = spec["kind"]

    if kind == "comparison":
        return _compile_comparison(cast(ComparisonFilterSpec, spec))

    if kind == "null_check":
        return _compile_null_check(cast(NullFilterSpec, spec))

    if kind == "related_exists":
        related_spec = cast(RelatedExistsFilterSpec, spec)
        return related(related_spec["object_type"]).exists()

    if kind == "linked_exists":
        linked_spec = cast(LinkedExistsFilterSpec, spec)
        return linked(linked_spec["object_type"]).exists()

    if kind == "has_event_exists":
        event_spec = cast(HasEventExistsFilterSpec, spec)
        return has_event(event_spec["event_type"]).exists()

    raise ValueError(f"Unsupported view filter kind: {kind!r}")


def _compile_comparison(spec: ComparisonFilterSpec) -> BoolExpr:
    left = _compile_expr_ref(spec["left"])
    op = spec["op"]
    right = spec["right"]

    if op == "==":
        return cast(BoolExpr, left == right)
    if op == "!=":
        return cast(BoolExpr, left != right)
    if op == ">":
        return cast(BoolExpr, left > right)
    if op == ">=":
        return cast(BoolExpr, left >= right)
    if op == "<":
        return cast(BoolExpr, left < right)
    if op == "<=":
        return cast(BoolExpr, left <= right)

    raise ValueError(f"Unsupported comparison operator: {op!r}")


def _compile_null_check(spec: NullFilterSpec) -> BoolExpr:
    expr = _compile_expr_ref(spec["expr"])
    op = spec["op"]

    if op == "is_null":
        return expr.is_null()
    if op == "is_not_null":
        return expr.not_null()

    raise ValueError(f"Unsupported null-check operator: {op!r}")


def _compile_expr_ref(spec: ExprRefSpec) -> ScalarExpr:
    kind = spec["kind"]
    cast_type = _compile_cast(spec.get("cast"))

    if kind == "field":
        field_spec = cast(FieldRefSpec, spec)
        return field(field_spec["name"], cast=cast_type)

    if kind == "attr":
        attr_spec = cast(AttrRefSpec, spec)
        return attr(attr_spec["name"], cast=cast_type)

    raise ValueError(f"Unsupported expression ref kind: {kind!r}")


def _compile_cast(cast_name: str | None):
    if cast_name is None:
        return None

    try:
        return _CASTS[cast_name]
    except KeyError as e:
        raise ValueError(f"Unsupported cast name: {cast_name!r}") from e
