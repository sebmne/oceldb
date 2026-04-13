from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Self, overload

import duckdb

from oceldb.ast.base import (
    AggregateExpr,
    AliasExpr,
    AndExpr,
    BoolExpr,
    CastExpr,
    CompareExpr,
    Expr,
    InExpr,
    NotExpr,
    OrExpr,
    ScalarExpr,
    SortExpr,
    UnaryPredicate,
)
from oceldb.ast.relation import RelationAllExpr, RelationCountExpr, RelationExistsExpr
from oceldb.core.ocel import OCEL
from oceldb.dsl.fields import col
from oceldb.query.plan import (
    DistinctPlan,
    ExtendPlan,
    FilterPlan,
    HavingPlan,
    LimitPlan,
    ProjectPlan,
    QueryPlan,
    QueryPlanNode,
    SortPlan,
)

if TYPE_CHECKING:
    from oceldb.query.types import GroupedRows, SelectedRows


class QueryBase:
    def __init__(self, plan: QueryPlan) -> None:
        self._plan = plan

    @property
    def ocel(self) -> OCEL:
        return self._plan.ocel

    def _spawn_same(self, plan: QueryPlan) -> Self:
        return type(self)(plan)

    def _with_node(self, node: QueryPlanNode) -> Self:
        return self._spawn_same(QueryPlan(ocel=self.ocel, node=node))


class ExecutionMixin(QueryBase):
    def collect(self) -> duckdb.DuckDBPyRelation:
        return self.ocel.sql(self.to_sql())

    def scalar(self) -> Any:
        row = self.collect().fetchone()
        if row is None:
            raise RuntimeError("Scalar query returned no rows")
        return row[0]

    def count(self) -> int:
        row = self.ocel.sql(
            f"SELECT COUNT(*) FROM ({self.to_sql()}) q"
        ).fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def exists(self) -> bool:
        row = self.ocel.sql(
            f"SELECT EXISTS(SELECT 1 FROM ({self.to_sql()}) q)"
        ).fetchone()
        if row is None:
            raise RuntimeError("EXISTS query returned no rows")
        return bool(row[0])

    def to_sql(self) -> str:
        from oceldb.query.compiler import compile_query

        return compile_query(self._plan)


class WhereMixin(QueryBase):
    @overload
    def where(self) -> Self: ...

    @overload
    def where(self, *predicates: BoolExpr) -> Self: ...

    def where(self, *predicates: Expr) -> Self:
        if not predicates:
            return self
        validated: list[BoolExpr] = []
        for predicate in predicates:
            if not isinstance(predicate, BoolExpr):
                raise TypeError("where(...) only accepts boolean expressions")
            if _contains_aggregate(predicate):
                raise TypeError(
                    "where(...) does not accept aggregate expressions; use group_by(...).agg(...)"
                )
            validated.append(predicate)
        return self._with_node(FilterPlan(self._plan.node, tuple(validated)))


class HavingMixin(QueryBase):
    @overload
    def having(self) -> Self: ...

    @overload
    def having(self, *predicates: BoolExpr) -> Self: ...

    def having(self, *predicates: Expr) -> Self:
        if not predicates:
            return self
        validated: list[BoolExpr] = []
        for predicate in predicates:
            if not isinstance(predicate, BoolExpr):
                raise TypeError("having(...) only accepts boolean expressions")
            if _contains_aggregate(predicate):
                raise TypeError(
                    "having(...) does not accept aggregate expressions directly; "
                    "refer to grouped output columns or aliases instead"
                )
            validated.append(predicate)
        return self._with_node(HavingPlan(self._plan.node, tuple(validated)))


class SortMixin(QueryBase):
    def sort(
        self,
        *exprs: ScalarExpr | AliasExpr | str | SortExpr,
        descending: bool = False,
    ) -> Self:
        if not exprs:
            raise ValueError("sort(...) requires at least one expression")

        orderings: list[SortExpr] = []
        for expr in exprs:
            if isinstance(expr, SortExpr):
                _require_sort_expr(expr, context="sort(...)")
                orderings.append(expr)
            elif isinstance(expr, str):
                orderings.append(SortExpr(expr=expr, descending=descending))
            else:
                _require_scalar_expr(expr, context="sort(...)")
                orderings.append(SortExpr(expr=expr, descending=descending))

        return self._with_node(SortPlan(self._plan.node, tuple(orderings)))


class UniqueMixin(QueryBase):
    def unique(self) -> Self:
        return self._with_node(DistinctPlan(self._plan.node))


class LimitMixin(QueryBase):
    def limit(self, n: int) -> Self:
        if n < 0:
            raise ValueError("Limit must be non-negative")
        return self._with_node(LimitPlan(self._plan.node, n))


class WithColumnsMixin(QueryBase):
    def with_columns(
        self,
        *exprs: ScalarExpr | AliasExpr,
        **named_exprs: ScalarExpr,
    ) -> Self:
        aliased = _coerce_scalar_named_exprs(
            exprs,
            named_exprs,
            require_alias=True,
            context="with_columns(...)",
        )
        if not aliased:
            return self
        return self._with_node(ExtendPlan(self._plan.node, aliased))


class SelectMixin(QueryBase):
    def select(
        self,
        *exprs: ScalarExpr | AliasExpr | str,
        **named_exprs: ScalarExpr,
    ) -> "SelectedRows":
        from oceldb.query.types import SelectedRows

        selected = _coerce_scalar_exprs(exprs, context="select(...)") + _coerce_scalar_named_exprs(
            (),
            named_exprs,
            context="select(...)",
        )
        if not selected:
            raise ValueError("select(...) requires at least one expression")
        return SelectedRows(QueryPlan(self.ocel, ProjectPlan(self._plan.node, selected)))


class GroupByMixin(QueryBase):
    def group_by(self, *exprs: ScalarExpr | AliasExpr | str) -> "GroupedRows":
        from oceldb.query.types import GroupedRows

        groupings = _coerce_scalar_exprs(exprs, context="group_by(...)")
        if not groupings:
            raise ValueError("group_by(...) requires at least one expression")
        return GroupedRows(self._plan, groupings)


class MaterializeMixin(QueryBase):
    def ids(self) -> list[str]:
        from oceldb.query.compiler import compile_query, query_output_columns

        columns = query_output_columns(self._plan)
        if "ocel_id" not in columns:
            raise ValueError("ids() requires the query result to contain an 'ocel_id' column")

        rows = self.ocel.sql(
            f'SELECT "ocel_id" FROM ({compile_query(self._plan)}) q'
        ).fetchall()
        return [row[0] for row in rows]

    def to_ocel(self) -> OCEL:
        from oceldb.query.materialize import materialize_query

        return materialize_query(self._plan)

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


def _coerce_scalar_exprs(
    exprs: tuple[ScalarExpr | AliasExpr | str, ...],
    *,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []
    for expr in exprs:
        if isinstance(expr, str):
            coerced: Expr = col(expr)
        else:
            coerced = expr
        _require_scalar_expr(coerced, context=context)
        result.append(coerced)
    return tuple(result)


def _coerce_scalar_named_exprs(
    exprs: tuple[ScalarExpr | AliasExpr, ...] | tuple[()],
    named_exprs: dict[str, ScalarExpr],
    *,
    require_alias: bool = False,
    context: str,
) -> tuple[Expr, ...] | tuple[AliasExpr, ...]:
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
    exprs: tuple[AggregateExpr | AliasExpr, ...],
    *,
    context: str,
) -> tuple[Expr, ...]:
    result: list[Expr] = []
    for expr in exprs:
        _require_aggregate_expr(expr, context=context)
        result.append(expr)
    return tuple(result)


def coerce_aggregate_named_exprs(
    exprs: tuple[AggregateExpr | AliasExpr, ...] | tuple[()],
    named_exprs: dict[str, AggregateExpr],
    *,
    context: str,
) -> tuple[Expr, ...] | tuple[AliasExpr, ...]:
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
    scalar_expr = _unwrap_alias(expr)
    if not isinstance(scalar_expr, ScalarExpr):
        raise TypeError(f"{context} only accepts scalar expressions")
    if _contains_aggregate(scalar_expr):
        raise TypeError(
            f"{context} does not accept aggregate expressions; use group_by(...).agg(...)"
        )


def _require_aggregate_expr(expr: Expr, *, context: str) -> None:
    scalar_expr = _unwrap_alias(expr)
    if not isinstance(scalar_expr, ScalarExpr) or not _contains_aggregate(scalar_expr):
        raise TypeError(f"{context} only accepts aggregate expressions")


def _require_sort_expr(expr: SortExpr, *, context: str) -> None:
    if isinstance(expr.expr, str):
        return
    _require_scalar_expr(expr.expr, context=context)


def _unwrap_alias(expr: Expr) -> Expr:
    current = expr
    while isinstance(current, AliasExpr):
        current = current.expr
    return current


def _contains_aggregate(expr: Expr) -> bool:
    if isinstance(expr, AggregateExpr):
        return True

    match expr:
        case AliasExpr(expr=inner) | CastExpr(expr=inner) | UnaryPredicate(expr=inner) | NotExpr(expr=inner):
            return _contains_aggregate(inner)
        case CompareExpr(left=left, right=right):
            return _contains_aggregate(left) or (
                isinstance(right, Expr) and _contains_aggregate(right)
            )
        case AndExpr(left=left, right=right) | OrExpr(left=left, right=right):
            return _contains_aggregate(left) or _contains_aggregate(right)
        case InExpr(expr=inner, values=values):
            return _contains_aggregate(inner) or any(
                isinstance(value, Expr) and _contains_aggregate(value)
                for value in values
            )
        case RelationExistsExpr(spec=spec) | RelationCountExpr(spec=spec):
            return any(_contains_aggregate(value) for value in spec.filters)
        case RelationAllExpr(spec=spec, condition=condition):
            return any(_contains_aggregate(value) for value in spec.filters) or _contains_aggregate(
                condition
            )
        case _:
            return False
