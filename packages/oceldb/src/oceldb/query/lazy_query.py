from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from oceldb.ast.base import AliasExpr, BoolExpr, Expr, SortExpr
from oceldb.core.metadata import LogicalTableName
from oceldb.core.ocel import OCEL
from oceldb.dsl.fields import col

QuerySourceKind = LogicalTableName


@dataclass(frozen=True)
class FilterOp:
    predicates: tuple[BoolExpr, ...]


@dataclass(frozen=True)
class WithColumnsOp:
    exprs: tuple[AliasExpr, ...]


@dataclass(frozen=True)
class SelectOp:
    exprs: tuple[Expr, ...]


@dataclass(frozen=True)
class GroupByAggOp:
    groupings: tuple[Expr, ...]
    aggregations: tuple[Expr, ...]


@dataclass(frozen=True)
class SortOp:
    orderings: tuple[SortExpr, ...]


@dataclass(frozen=True)
class UniqueOp:
    pass


@dataclass(frozen=True)
class LimitOp:
    n: int


@dataclass(frozen=True)
class LazyOCELQuery:
    ocel: OCEL
    source_kind: QuerySourceKind
    selected_types: tuple[str, ...] = field(default_factory=tuple)
    ops: tuple[object, ...] = field(default_factory=tuple)

    @classmethod
    def from_source(
        cls,
        ocel: OCEL,
        *,
        source_kind: QuerySourceKind,
        selected_types: tuple[str, ...] = (),
    ) -> "LazyOCELQuery":
        return cls(
            ocel=ocel,
            source_kind=source_kind,
            selected_types=selected_types,
        )

    def filter(self, *predicates: BoolExpr) -> "LazyOCELQuery":
        if not predicates:
            return self
        return self._append(FilterOp(tuple(predicates)))

    def with_columns(
        self,
        *exprs: Expr,
        **named_exprs: Expr,
    ) -> "LazyOCELQuery":
        aliased = _coerce_named_exprs(exprs, named_exprs, require_alias=True)
        if not aliased:
            return self
        return self._append(WithColumnsOp(aliased))

    def select(
        self,
        *exprs: Expr | str,
        **named_exprs: Expr,
    ) -> "LazyOCELQuery":
        selected = _coerce_exprs(exprs) + _coerce_named_exprs((), named_exprs)
        if not selected:
            raise ValueError("select(...) requires at least one expression")
        return self._append(SelectOp(selected))

    def group_by(self, *exprs: Expr | str) -> "GroupedLazyOCELQuery":
        groupings = _coerce_exprs(exprs)
        if not groupings:
            raise ValueError("group_by(...) requires at least one expression")
        return GroupedLazyOCELQuery(self, groupings)

    def sort(
        self,
        *exprs: Expr | str | SortExpr,
        descending: bool = False,
    ) -> "LazyOCELQuery":
        if not exprs:
            raise ValueError("sort(...) requires at least one expression")

        orderings: list[SortExpr] = []
        for expr in exprs:
            if isinstance(expr, SortExpr):
                orderings.append(expr)
            elif isinstance(expr, str):
                orderings.append(SortExpr(expr=expr, descending=descending))
            else:
                orderings.append(SortExpr(expr=expr, descending=descending))

        return self._append(SortOp(tuple(orderings)))

    def unique(self) -> "LazyOCELQuery":
        return self._append(UniqueOp())

    def limit(self, n: int) -> "LazyOCELQuery":
        if n < 0:
            raise ValueError("Limit must be non-negative")
        return self._append(LimitOp(n))

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

    def ids(self) -> list[str]:
        from oceldb.query.compiler import query_output_columns

        columns = query_output_columns(self)
        if "ocel_id" not in columns:
            raise ValueError("ids() requires the query result to contain an 'ocel_id' column")

        rows = self.ocel.sql(
            f'SELECT "ocel_id" FROM ({self.to_sql()}) q'
        ).fetchall()
        return [row[0] for row in rows]

    def to_sql(self) -> str:
        from oceldb.query.compiler import compile_query

        return compile_query(self)

    def to_ocel(self) -> OCEL:
        from oceldb.query.materialize import materialize_query

        return materialize_query(self)

    def write(
        self,
        target: str | Path,
        *,
        overwrite: bool = False,
        packaged: bool = False,
    ) -> Path:
        derived = self.to_ocel()
        try:
            return derived.write(target, overwrite=overwrite, packaged=packaged)
        finally:
            derived.close()

    def _append(self, op: object) -> "LazyOCELQuery":
        return LazyOCELQuery(
            ocel=self.ocel,
            source_kind=self.source_kind,
            selected_types=self.selected_types,
            ops=self.ops + (op,),
        )


@dataclass(frozen=True)
class GroupedLazyOCELQuery:
    query: LazyOCELQuery
    groupings: tuple[Expr, ...]

    def agg(self, *exprs: Expr, **named_exprs: Expr) -> LazyOCELQuery:
        aggregations = _coerce_exprs(exprs) + _coerce_named_exprs((), named_exprs)
        if not aggregations:
            raise ValueError("agg(...) requires at least one expression")
        return self.query._append(GroupByAggOp(self.groupings, aggregations))


def _coerce_exprs(exprs: tuple[Expr | str, ...]) -> tuple[Expr, ...]:
    result: list[Expr] = []
    for expr in exprs:
        if isinstance(expr, str):
            result.append(col(expr))
        else:
            result.append(expr)
    return tuple(result)


def _coerce_named_exprs(
    exprs: tuple[Expr, ...] | tuple[()],
    named_exprs: dict[str, Expr],
    *,
    require_alias: bool = False,
) -> tuple[Expr, ...] | tuple[AliasExpr, ...]:
    result: list[Expr] = []

    for expr in exprs:
        if require_alias and not isinstance(expr, AliasExpr):
            raise ValueError(
                "with_columns(...) expressions must be aliased; "
                "use expr.alias('name') or keyword arguments"
            )
        result.append(expr)

    for name, expr in named_exprs.items():
        result.append(expr.alias(name))

    return tuple(result)
