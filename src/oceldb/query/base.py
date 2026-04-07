from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Self
from uuid import uuid4

from oceldb.core.ocel import OCEL
from oceldb.expressions.base import Expr
from oceldb.expressions.context import Context, ScopeKind


@dataclass(frozen=True)
class BaseQuery(ABC):
    """
    Shared base class for lazy root-level OCEL queries.

    Concrete subclasses, such as ObjectQuery and EventQuery, define:
        - the root table and alias
        - the root scope kind
        - how selected types are interpreted
        - how a derived OCEL is materialized from the query result

    This class provides shared immutable query chaining and terminal execution
    methods.
    """

    ocel: OCEL
    filters: tuple[Expr, ...] = field(default_factory=tuple)

    def filter(self, *exprs: Expr) -> Self:
        """
        Return a new query with the given boolean filter expressions added.

        Args:
            *exprs: One or more boolean expressions.

        Returns:
            A new query of the same concrete type.
        """
        if not exprs:
            return self

        return self._clone_with_filters(self.filters + tuple(exprs))

    def count(self) -> int:
        """
        Execute the query and return the number of matching rows.
        """
        sql = self.to_sql()
        row = self.ocel.sql(f"SELECT COUNT(DISTINCT ocel_id) FROM ({sql}) q").fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def ids(self) -> list[str]:
        """
        Execute the query and return the matching root ids.
        """
        sql = self.to_sql()
        return [
            row[0]
            for row in self.ocel.sql(
                f"SELECT DISTINCT ocel_id FROM ({sql}) q"
            ).fetchall()
        ]

    def to_sql(self) -> str:
        """
        Compile this query into a SQL statement returning matching root rows.
        """
        alias = self._root_alias()
        table = self._root_table()
        where_sql = self._where_sql()

        return f"""
            SELECT {alias}.*
            FROM {table} {alias}
            WHERE {where_sql}
        """

    def to_ocel(self) -> OCEL:
        """
        Materialize this query result as a derived OCEL.

        The concrete subclass defines how the selected root rows induce the
        derived sublog.

        Returns:
            A new OCEL instance backed by a fresh DuckDB schema.
        """
        target_schema = f"ocel_{uuid4().hex[:8]}"
        con = self.ocel._con

        con.execute(f"CREATE SCHEMA {target_schema}")
        try:
            self._materialize_sublog(target_schema)
        except Exception:
            con.execute(f"DROP SCHEMA {target_schema} CASCADE")
            raise

        return OCEL(
            path=self.ocel.path,
            con=con,
            metadata=self.ocel.metadata,
            schema=target_schema,
            owns_connection=False,
        )

    def _context(self) -> Context:
        """
        Build the root compilation context for this query.
        """
        return Context(
            alias=self._root_alias(),
            schema=self.ocel.schema,
            kind=self._root_kind(),
        )

    def _where_sql(self) -> str:
        """
        Compile the WHERE clause for this query.
        """
        parts: list[str] = []

        type_filter = self._type_filter_sql()
        if type_filter is not None:
            parts.append(type_filter)

        ctx = self._context()
        for expr in self.filters:
            parts.append(expr.to_sql(ctx))

        return " AND ".join(parts) if parts else "TRUE"

    def _root_table(self) -> str:
        """
        Return the fully qualified root table of this query.
        """
        return self._context().table(self._root_table_name())

    @abstractmethod
    def _clone_with_filters(self, filters: tuple[Expr, ...]) -> Self:
        """
        Return a copy of this query with the given filters.
        """
        raise NotImplementedError

    @abstractmethod
    def _root_alias(self) -> str:
        """
        Return the SQL alias used for the root table of this query.
        """
        raise NotImplementedError

    @abstractmethod
    def _root_kind(self) -> ScopeKind:
        """
        Return the scope kind of the root rows of this query.
        """
        raise NotImplementedError

    @abstractmethod
    def _root_table_name(self) -> str:
        """
        Return the logical root table name of this query.
        """
        raise NotImplementedError

    @abstractmethod
    def _type_filter_sql(self) -> str | None:
        """
        Return the SQL fragment restricting the selected root types, if any.
        """
        raise NotImplementedError

    @abstractmethod
    def _materialize_sublog(self, target_schema: str) -> None:
        """
        Populate an already-created target schema with the logical OCEL tables.

        Args:
            target_schema: The fresh target schema that must be populated with
                the logical OCEL tables:
                - event
                - object
                - event_object
                - object_object
        """
        raise NotImplementedError

    def _materialize_common_sublog_views(self, target_schema: str) -> None:
        """
        Populate the common object-side views of a derived OCEL.

        Assumes `{target_schema}.event` already exists.
        """
        source_schema = self.ocel.schema
        con = self.ocel._con

        con.execute(f"""
            CREATE VIEW {target_schema}.event_object AS
            SELECT DISTINCT eo.*
            FROM {source_schema}.event_object eo
            JOIN {target_schema}.event e
              ON eo.ocel_event_id = e.ocel_id
        """)

        con.execute(f"""
            CREATE VIEW {target_schema}.object AS
            SELECT DISTINCT o.*
            FROM {source_schema}.object o
            JOIN {target_schema}.event_object eo
              ON o.ocel_id = eo.ocel_object_id
        """)

        con.execute(f"""
            CREATE VIEW {target_schema}.object_object AS
            SELECT DISTINCT oo.*
            FROM {source_schema}.object_object oo
            JOIN {target_schema}.object os
              ON oo.ocel_source_id = os.ocel_id
            JOIN {target_schema}.object ot
              ON oo.ocel_target_id = ot.ocel_id
        """)
