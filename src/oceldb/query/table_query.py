from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

import duckdb

from oceldb.ast.base import AggregateExpr, Expr, OrderExpr
from oceldb.compiler.context import CompileContext, ScopeKind
from oceldb.core.ocel import OCEL


@dataclass(frozen=True)
class TableQuery:
    """
    Tabular analytical query built on top of a root OCEL query.

    This query layer supports projection, grouping, aggregation, ordering, and
    limiting over the current root scope.
    """

    ocel: OCEL
    root_table: str
    root_alias: str
    root_kind: ScopeKind
    where_sql: str
    selections: Tuple[Expr, ...] = field(default_factory=tuple)
    groupings: Tuple[Expr, ...] = field(default_factory=tuple)
    aggregations: Tuple[AggregateExpr | Expr, ...] = field(default_factory=tuple)
    orderings: Tuple[OrderExpr, ...] = field(default_factory=tuple)
    is_distinct: bool = False
    limit_n: Optional[int] = None

    def select(self, *exprs: Expr) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections + tuple(exprs),
            groupings=self.groupings,
            aggregations=self.aggregations,
            orderings=self.orderings,
            is_distinct=self.is_distinct,
            limit_n=self.limit_n,
        )

    def distinct(self) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections,
            groupings=self.groupings,
            aggregations=self.aggregations,
            orderings=self.orderings,
            is_distinct=True,
            limit_n=self.limit_n,
        )

    def group_by(self, *exprs: Expr) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections,
            groupings=self.groupings + tuple(exprs),
            aggregations=self.aggregations,
            orderings=self.orderings,
            is_distinct=self.is_distinct,
            limit_n=self.limit_n,
        )

    def agg(self, *exprs: AggregateExpr | Expr) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections,
            groupings=self.groupings,
            aggregations=self.aggregations + tuple(exprs),
            orderings=self.orderings,
            is_distinct=self.is_distinct,
            limit_n=self.limit_n,
        )

    def order_by(self, *exprs: OrderExpr) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections,
            groupings=self.groupings,
            aggregations=self.aggregations,
            orderings=self.orderings + tuple(exprs),
            is_distinct=self.is_distinct,
            limit_n=self.limit_n,
        )

    def limit(self, n: int) -> "TableQuery":
        if n < 0:
            raise ValueError("Limit must be non-negative")
        return TableQuery(
            ocel=self.ocel,
            root_table=self.root_table,
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self.where_sql,
            selections=self.selections,
            groupings=self.groupings,
            aggregations=self.aggregations,
            orderings=self.orderings,
            is_distinct=self.is_distinct,
            limit_n=n,
        )

    def relation(self) -> duckdb.DuckDBPyRelation:
        return self.ocel.sql(self.to_sql())

    def scalar(self) -> Any:
        row = self.relation().fetchone()
        if row is None:
            raise RuntimeError("Scalar query returned no rows")
        return row[0]

    def to_sql(self) -> str:
        from oceldb.compiler.render_expr import render_expr, render_order_expr

        ctx = CompileContext(
            alias=self.root_alias,
            schema=self.ocel.schema,
            kind=self.root_kind,
        )

        select_parts = []

        if self.selections:
            select_parts.extend(render_expr(expr, ctx) for expr in self.selections)

        if self.aggregations:
            select_parts.extend(render_expr(expr, ctx) for expr in self.aggregations)

        if not select_parts:
            if self.groupings:
                select_parts.extend(render_expr(expr, ctx) for expr in self.groupings)
            else:
                select_parts.append("*")

        distinct_sql = "DISTINCT " if self.is_distinct else ""
        select_sql = ", ".join(select_parts)

        sql = f"""
            SELECT {distinct_sql}{select_sql}
            FROM {self.root_table} {self.root_alias}
            WHERE {self.where_sql}
        """

        if self.groupings:
            group_by_sql = ", ".join(render_expr(expr, ctx) for expr in self.groupings)
            sql += f"\nGROUP BY {group_by_sql}"

        if self.orderings:
            order_by_sql = ", ".join(
                render_order_expr(expr, ctx) for expr in self.orderings
            )
            sql += f"\nORDER BY {order_by_sql}"

        if self.limit_n is not None:
            sql += f"\nLIMIT {self.limit_n}"

        return sql
