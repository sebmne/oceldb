from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Self

import duckdb

from oceldb.core.ocel import OCEL
from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context


@dataclass(frozen=True)
class BaseQuery(ABC):
    """
    Shared base class for root-level OCEL queries.

    Concrete subclasses such as ObjectQuery and EventQuery define:
        - which logical table they query
        - which alias they use
        - which id/type columns they expose
        - how selected types are interpreted
    """

    ocel: OCEL
    filters: tuple[Expr, ...] = field(default_factory=tuple)

    def filter(self, *exprs: Expr) -> Self:
        """
        Add one or more boolean filter expressions.
        """
        if not exprs:
            return self

        return self._with_filters(self.filters + tuple(exprs))

    def collect(self) -> duckdb.DuckDBPyRelation:
        """
        Execute the query and return matching rows as a DuckDB relation.
        """
        return self.ocel.sql(self.to_sql())

    def count(self) -> int:
        """
        Execute the query and return the number of matching rows.
        """
        sql = self.to_sql()
        return self.ocel.sql(f"SELECT COUNT(*) FROM ({sql}) q").fetchone()[0]  # type: ignore

    def ids(self) -> list[str]:
        """
        Execute the query and return the matching root ids.
        """
        sql = self.to_sql()
        return [
            row[0]
            for row in self.ocel.sql(
                f"SELECT {self._id_column()} FROM ({sql}) q"
            ).fetchall()
        ]

    def head(self, n: int = 5) -> duckdb.DuckDBPyRelation:
        """
        Return the first `n` matching rows as a DuckDB relation.
        """
        if n < 0:
            raise ValueError("n must be non-negative")

        sql = self.to_sql()
        return self.ocel.sql(f"SELECT * FROM ({sql}) q LIMIT {n}")

    def to_sql(self) -> str:
        """
        Compile this query into SQL returning matching root rows.
        """
        alias = self._root_alias()
        table = self._root_table()
        where_sql = self._where_sql()

        return f"""
            SELECT {alias}.*
            FROM {table} {alias}
            WHERE {where_sql}
        """

    @abstractmethod
    def _context(self) -> Context:
        raise NotImplementedError

    def _where_sql(self) -> str:
        parts: list[str] = []

        type_filter = self._type_filter_sql()
        if type_filter is not None:
            parts.append(type_filter)

        ctx = self._context()
        for expr in self.filters:
            parts.append(expr.to_sql(ctx))

        return " AND ".join(parts) if parts else "TRUE"

    @abstractmethod
    def _with_filters(self, filters: tuple[Expr, ...]) -> Self:
        """
        Return a copy of this query with the given filters.
        """
        raise NotImplementedError

    @abstractmethod
    def _root_alias(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _root_table_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _id_column(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _type_filter_sql(self) -> str | None:
        raise NotImplementedError

    def _root_table(self) -> str:
        return self._context().table(self._root_table_name())
