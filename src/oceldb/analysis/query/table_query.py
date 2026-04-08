from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Tuple

import duckdb

from oceldb.ast.base import AggregateExpr, Expr, OrderExpr
from oceldb.core.ocel import OCEL
from oceldb.dsl import count, count_distinct

AnalysisTableKind = Literal["event", "object", "event_object", "object_object"]


@dataclass(frozen=True)
class TableQuery:
    ocel: OCEL
    table_kind: AnalysisTableKind

    selections: Tuple[Expr, ...] = field(default_factory=tuple)
    groupings: Tuple[Expr, ...] = field(default_factory=tuple)
    aggregations: Tuple[AggregateExpr | Expr, ...] = field(default_factory=tuple)
    orderings: Tuple[OrderExpr, ...] = field(default_factory=tuple)
    is_distinct: bool = False
    limit_n: Optional[int] = None

    @classmethod
    def from_source(cls, ocel: OCEL, table_kind: AnalysisTableKind) -> "TableQuery":
        return cls(ocel=ocel, table_kind=table_kind)

    def select(self, *exprs: Expr) -> "TableQuery":
        return TableQuery(
            ocel=self.ocel,
            table_kind=self.table_kind,
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
            table_kind=self.table_kind,
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
            table_kind=self.table_kind,
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
            table_kind=self.table_kind,
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
            table_kind=self.table_kind,
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
            table_kind=self.table_kind,
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

    def exists(self) -> bool:
        row = self.ocel.sql(
            f"SELECT EXISTS(SELECT 1 FROM ({self.to_sql()}) q)"
        ).fetchone()
        if row is None:
            raise RuntimeError("EXISTS query returned no rows")
        return bool(row[0])

    def count(self) -> int:
        row = self.agg(count().as_("count")).relation().fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def count_distinct(self, expr: Expr) -> int:
        row = self.agg(count_distinct(expr).as_("count")).relation().fetchone()
        if row is None:
            raise RuntimeError("COUNT DISTINCT query returned no rows")
        return int(row[0])

    def to_sql(self) -> str:
        from oceldb.analysis.compiler.render_query import render_table_query

        return render_table_query(self)
