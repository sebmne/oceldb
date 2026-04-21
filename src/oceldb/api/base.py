"""Shared infrastructure for the fluent query API.

``QueryBase`` holds the ``(ocel, plan_node)`` pair that every fluent state
class carries. The mixins in this module implement the operator methods
(``where``, ``sort``, ``select``, ...) by appending plan nodes.

Expression validation for the mixins lives here as well: each public method
accepts either string column names or full ``Expr`` objects, coerces them,
and rejects aggregates/windows where they aren't allowed.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, overload

import duckdb

from oceldb.compile.plan import compile_query
from oceldb.compile.schema import query_output_columns
from oceldb.compile.validate import contains_aggregate, contains_window
from oceldb.core.ocel import OCEL
from oceldb.expr.nodes import (
    AliasExpr,
    BoolOpExpr,
    CompareExpr,
    Expr,
    InExpr,
    NotExpr,
    PredicateFunctionExpr,
    RelationAllExpr,
    RelationExistsExpr,
    SortExpr,
    coerce_expr,
)
from oceldb.plan.nodes import (
    DistinctPlan,
    ExtendPlan,
    FilterPlan,
    HavingPlan,
    LimitPlan,
    PlanNode,
    ProjectPlan,
    RenamePlan,
    SortPlan,
)

if TYPE_CHECKING:
    from oceldb.api.states import GroupedRows, SelectedRows


_BOOL_EXPR_TYPES: tuple[type, ...] = (
    CompareExpr,
    BoolOpExpr,
    NotExpr,
    InExpr,
    PredicateFunctionExpr,
    RelationExistsExpr,
    RelationAllExpr,
)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class QueryBase:
    def __init__(self, ocel: OCEL, node: PlanNode) -> None:
        self._ocel = ocel
        self._node = node

    @property
    def ocel(self) -> OCEL:
        return self._ocel

    @property
    def node(self) -> PlanNode:
        return self._node

    def _spawn_same(self, node: PlanNode) -> Self:
        return type(self)(self._ocel, node)

    def _with_node(self, node: PlanNode) -> Self:
        return self._spawn_same(node)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


class ExecutionMixin(QueryBase):
    def to_sql(self) -> str:
        return compile_query(self._node, self._ocel.manifest)

    def collect(self) -> duckdb.DuckDBPyRelation:
        return self._ocel.sql(self.to_sql())

    def scalar(self) -> Any:
        row = self.collect().fetchone()
        if row is None:
            raise RuntimeError("Scalar query returned no rows")
        return row[0]

    def count(self) -> int:
        row = self._ocel.sql(
            f"SELECT COUNT(*) FROM ({self.to_sql()}) q"
        ).fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def exists(self) -> bool:
        row = self._ocel.sql(
            f"SELECT EXISTS(SELECT 1 FROM ({self.to_sql()}) q)"
        ).fetchone()
        if row is None:
            raise RuntimeError("EXISTS query returned no rows")
        return bool(row[0])


# ---------------------------------------------------------------------------
# where / having
# ---------------------------------------------------------------------------


class WhereMixin(QueryBase):
    @overload
    def where(self) -> Self: ...
    @overload
    def where(self, *predicates: Expr) -> Self: ...

    def where(self, *predicates: Expr) -> Self:
        if not predicates:
            return self
        validated: list[Expr] = []
        for predicate in predicates:
            if not _is_bool_expr(predicate):
                raise TypeError("where(...) only accepts boolean expressions")
            if contains_aggregate(predicate):
                raise TypeError(
                    "where(...) does not accept aggregate expressions; "
                    "use group_by(...).agg(...)"
                )
            if contains_window(predicate):
                raise TypeError(
                    "where(...) does not accept window expressions directly; "
                    "use with_columns(...) and filter on the resulting alias"
                )
            validated.append(predicate)
        return self._with_node(FilterPlan(self._node, tuple(validated)))


class HavingMixin(QueryBase):
    @overload
    def having(self) -> Self: ...
    @overload
    def having(self, *predicates: Expr) -> Self: ...

    def having(self, *predicates: Expr) -> Self:
        if not predicates:
            return self
        validated: list[Expr] = []
        for predicate in predicates:
            if not _is_bool_expr(predicate):
                raise TypeError("having(...) only accepts boolean expressions")
            if contains_aggregate(predicate):
                raise TypeError(
                    "having(...) does not accept aggregate expressions directly; "
                    "refer to grouped output columns or aliases instead"
                )
            if contains_window(predicate):
                raise TypeError(
                    "having(...) does not accept window expressions directly; "
                    "materialize the window result into a column first"
                )
            validated.append(predicate)
        return self._with_node(HavingPlan(self._node, tuple(validated)))


# ---------------------------------------------------------------------------
# sort / unique / limit
# ---------------------------------------------------------------------------


class SortMixin(QueryBase):
    def sort(
        self,
        *exprs: Expr | str | SortExpr,
        descending: bool = False,
    ) -> Self:
        if not exprs:
            raise ValueError("sort(...) requires at least one expression")
        orderings: list[SortExpr] = []
        for item in exprs:
            if isinstance(item, SortExpr):
                _require_scalar_like(item.expr, context="sort(...)")
                orderings.append(item)
                continue
            expr = coerce_expr(item)
            _require_scalar_like(expr, context="sort(...)")
            orderings.append(SortExpr(expr=expr, descending=descending))
        return self._with_node(SortPlan(self._node, tuple(orderings)))


class UniqueMixin(QueryBase):
    def unique(self) -> Self:
        return self._with_node(DistinctPlan(self._node))


class LimitMixin(QueryBase):
    def limit(self, n: int) -> Self:
        if n < 0:
            raise ValueError("Limit must be non-negative")
        return self._with_node(LimitPlan(self._node, n))


# ---------------------------------------------------------------------------
# with_columns / select / group_by
# ---------------------------------------------------------------------------


class WithColumnsMixin(QueryBase):
    def with_columns(
        self,
        *exprs: Expr | AliasExpr,
        **named_exprs: Expr,
    ) -> Self:
        aliased = _coerce_scalar_named_exprs(
            exprs,
            named_exprs,
            require_alias=True,
            context="with_columns(...)",
        )
        if not aliased:
            return self
        return self._with_node(ExtendPlan(self._node, aliased))


class SelectMixin(QueryBase):
    def select(
        self,
        *exprs: Expr | str,
        **named_exprs: Expr,
    ) -> "SelectedRows":
        from oceldb.api.states import SelectedRows

        selected = (
            _coerce_scalar_exprs(exprs, context="select(...)")
            + _coerce_scalar_named_exprs((), named_exprs, context="select(...)")
        )
        if not selected:
            raise ValueError("select(...) requires at least one expression")
        return SelectedRows(self._ocel, ProjectPlan(self._node, selected))


class GroupByMixin(QueryBase):
    def group_by(self, *exprs: Expr | str) -> "GroupedRows":
        from oceldb.api.states import GroupedRows

        groupings = _coerce_scalar_exprs(exprs, context="group_by(...)")
        if not groupings:
            raise ValueError("group_by(...) requires at least one expression")
        if any(contains_window(expr) for expr in groupings):
            raise TypeError("group_by(...) does not accept window expressions")
        return GroupedRows(self._ocel, self._node, groupings)


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


class RenameMixin(QueryBase):
    def rename(
        self,
        mapping: Mapping[str, str] | None = None,
        /,
        **named_mapping: str,
    ) -> Self:
        rename_map: dict[str, str] = {}
        if mapping is not None:
            rename_map.update(mapping)
        rename_map.update(named_mapping)
        if not rename_map:
            return self

        columns = query_output_columns(self._node, self._ocel.manifest)
        unknown = sorted(name for name in rename_map if name not in columns)
        if unknown:
            raise ValueError(
                "rename(...) received unknown columns: "
                + ", ".join(repr(name) for name in unknown)
            )

        for source, target in rename_map.items():
            if not target:
                raise ValueError(
                    f"rename(...) target for {source!r} must not be empty"
                )

        seen: set[str] = set()
        for name in columns:
            final_name = rename_map.get(name, name)
            if final_name in seen:
                raise ValueError(
                    "rename(...) would produce duplicate output columns; "
                    f"conflicting target {final_name!r}"
                )
            seen.add(final_name)

        return self._with_node(
            RenamePlan(self._node, tuple(rename_map.items()))
        )


# ---------------------------------------------------------------------------
# Materialization
# ---------------------------------------------------------------------------


class MaterializeMixin(QueryBase):
    def ids(self) -> list[str]:
        columns = query_output_columns(self._node, self._ocel.manifest)
        if "ocel_id" not in columns:
            raise ValueError(
                "ids() requires the query result to contain an 'ocel_id' column"
            )
        sql = compile_query(self._node, self._ocel.manifest)
        rows = self._ocel.sql(f'SELECT "ocel_id" FROM ({sql}) q').fetchall()
        return [row[0] for row in rows]

    def to_ocel(self) -> OCEL:
        from oceldb.api.materialize import materialize_query

        return materialize_query(self._ocel, self._node)

    def write(
        self,
        target: str | Path,
        *,
        overwrite: bool = False,
    ) -> Path:
        derived = self.to_ocel()
        try:
            return derived.write(target, overwrite=overwrite)
        finally:
            derived.close()


# ---------------------------------------------------------------------------
# Expression helpers
# ---------------------------------------------------------------------------


def _is_bool_expr(expr: Expr) -> bool:
    if isinstance(expr, _BOOL_EXPR_TYPES):
        return True
    if isinstance(expr, AliasExpr):
        return _is_bool_expr(expr.expr)
    return False


def _coerce_scalar_exprs(
    exprs: tuple[Expr | str, ...],
    *,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []
    for expr in exprs:
        coerced = coerce_expr(expr)
        _require_scalar_expr(coerced, context=context)
        result.append(coerced)
    return tuple(result)


def _coerce_scalar_named_exprs(
    exprs: tuple[Expr | AliasExpr, ...] | tuple[()],
    named_exprs: dict[str, Expr],
    *,
    require_alias: bool = False,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []

    for expr in exprs:
        if require_alias and not isinstance(expr, AliasExpr):
            raise ValueError(
                "with_columns(...) expressions must be aliased; "
                "use expr.alias('name') or keyword arguments"
            )
        _require_scalar_expr(expr, context=context)
        result.append(expr)

    for name, expr in named_exprs.items():
        aliased = expr.alias(name)
        _require_scalar_expr(aliased, context=context)
        result.append(aliased)

    return tuple(result)


def coerce_aggregate_exprs(
    exprs: tuple[Expr | AliasExpr, ...],
    *,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []
    for expr in exprs:
        _require_aggregate_expr(expr, context=context)
        result.append(expr)
    return tuple(result)


def coerce_aggregate_named_exprs(
    exprs: tuple[Expr | AliasExpr, ...] | tuple[()],
    named_exprs: dict[str, Expr],
    *,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []

    for expr in exprs:
        _require_aggregate_expr(expr, context=context)
        result.append(expr)

    for name, expr in named_exprs.items():
        aliased = expr.alias(name)
        _require_aggregate_expr(aliased, context=context)
        result.append(aliased)

    return tuple(result)


def _require_scalar_expr(expr: Expr, *, context: str) -> None:
    inner = _unwrap_alias(expr)
    if contains_aggregate(inner):
        raise TypeError(
            f"{context} does not accept aggregate expressions; "
            "use group_by(...).agg(...)"
        )


def _require_aggregate_expr(expr: Expr, *, context: str) -> None:
    inner = _unwrap_alias(expr)
    if not contains_aggregate(inner):
        raise TypeError(f"{context} only accepts aggregate expressions")


def _require_scalar_like(expr: Expr, *, context: str) -> None:
    if contains_aggregate(expr):
        raise TypeError(
            f"{context} does not accept aggregate expressions; "
            "use group_by(...).agg(...)"
        )


def _unwrap_alias(expr: Expr) -> Expr:
    current = expr
    while isinstance(current, AliasExpr):
        current = current.expr
    return current


__all__ = [
    "ExecutionMixin",
    "GroupByMixin",
    "HavingMixin",
    "LimitMixin",
    "MaterializeMixin",
    "QueryBase",
    "RenameMixin",
    "SelectMixin",
    "SortMixin",
    "UniqueMixin",
    "WhereMixin",
    "WithColumnsMixin",
    "coerce_aggregate_exprs",
    "coerce_aggregate_named_exprs",
]
