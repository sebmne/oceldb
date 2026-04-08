from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional, Tuple

from oceldb.ast.base import BoolExpr
from oceldb.core.ocel import OCEL

ViewScopeKind = Literal["event", "object"]


@dataclass(frozen=True)
class ViewQuery:
    """
    Immutable semantic sublog/view query.

    This query selects either events or objects from an OCEL and can later be:
    - compiled to SQL
    - materialized to a derived OCEL
    - passed into the analysis subsystem
    """

    ocel: OCEL
    root_kind: ViewScopeKind
    selected_types: Tuple[str, ...] = field(default_factory=tuple)
    filters: Tuple[BoolExpr, ...] = field(default_factory=tuple)

    def filter(self, *exprs: BoolExpr) -> "ViewQuery":
        if not exprs:
            return self
        return ViewQuery(
            ocel=self.ocel,
            root_kind=self.root_kind,
            selected_types=self.selected_types,
            filters=self.filters + tuple(exprs),
        )

    def count(self) -> int:
        sql = self.to_sql()

        if self.root_kind == "object":
            row = self.ocel.sql(
                f"SELECT COUNT(DISTINCT ocel_id) FROM ({sql}) q"
            ).fetchone()
        else:
            row = self.ocel.sql(f"SELECT COUNT(*) FROM ({sql}) q").fetchone()

        if row is None:
            raise RuntimeError("COUNT query returned no rows")

        return int(row[0])

    def ids(self) -> list[str]:
        rows = self.ocel.sql(
            f'SELECT DISTINCT "{self.id_column}" FROM ({self.to_sql()}) q'
        ).fetchall()

        return [row[0] for row in rows]

    def to_sql(self) -> str:
        from oceldb.sublog.compiler.render_query import render_view_query

        return render_view_query(self)

    def to_ocel(self) -> OCEL:
        from oceldb.sublog.materialize import materialize_view_query

        return materialize_view_query(self)

    @property
    def root_alias(self) -> str:
        return "e" if self.root_kind == "event" else "o"

    @property
    def root_table_name(self) -> str:
        return self.root_kind

    @property
    def id_column(self) -> str:
        return "ocel_id"

    def type_filter_expr(self) -> Optional[BoolExpr]:
        if not self.selected_types:
            return None

        from oceldb.dsl import type_
        from oceldb.dsl.functions import in_

        if len(self.selected_types) == 1:
            return type_() == self.selected_types[0]

        return in_(type_(), self.selected_types)
