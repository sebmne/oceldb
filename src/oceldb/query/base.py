from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Self, Tuple
from uuid import uuid4

from oceldb.ast.base import BoolExpr
from oceldb.compiler.context import CompileContext, ScopeKind
from oceldb.compiler.render_expr import render_bool_expr
from oceldb.core.ocel import OCEL


@dataclass(frozen=True)
class BaseQuery(ABC):
    """
    Shared base class for lazy root-level OCEL queries.

    Concrete subclasses define:
        - the root alias
        - the root scope kind
        - the logical root table name
        - the root id column
        - optional root-type restrictions
        - how a derived OCEL sublog is materialized
    """

    ocel: OCEL
    filters: Tuple[BoolExpr, ...] = field(default_factory=tuple)

    root_alias: str = field(init=False)
    root_kind: ScopeKind = field(init=False)
    root_table_name: str = field(init=False)
    id_column: str = field(init=False)

    def filter(self, *exprs: BoolExpr) -> Self:
        """
        Return a new query with the given boolean filter expressions added.
        """
        if not exprs:
            return self
        return self._clone_with_filters(self.filters + tuple(exprs))

    def count(self) -> int:
        """
        Execute the query and return the number of matching root rows.
        """
        row = self.ocel.sql(f"SELECT COUNT(*) FROM ({self.to_sql()}) q").fetchone()
        if row is None:
            raise RuntimeError("COUNT query returned no rows")
        return int(row[0])

    def ids(self) -> list[str]:
        """
        Execute the query and return the distinct matching root ids.
        """
        rows = self.ocel.sql(
            f'SELECT DISTINCT "{self.id_column}" FROM ({self.to_sql()}) q'
        ).fetchall()
        return [row[0] for row in rows]

    def table(self):
        """
        Enter tabular analytical query mode for the current root scope.
        """
        from oceldb.query.table_query import TableQuery

        return TableQuery(
            ocel=self.ocel,
            root_table=self._root_table(),
            root_alias=self.root_alias,
            root_kind=self.root_kind,
            where_sql=self._where_sql(),
        )

    def to_sql(self) -> str:
        """
        Compile this root query into SQL.
        """
        where_sql = self._where_sql()

        return f"""
            SELECT {self.root_alias}.*
            FROM {self._root_table()} {self.root_alias}
            WHERE {where_sql}
        """

    def to_ocel(self) -> OCEL:
        """
        Materialize this query result as a derived OCEL.
        """
        target_schema = f"ocel_{uuid4().hex[:8]}"
        con = self.ocel._con

        con.execute(f"CREATE SCHEMA {target_schema}")
        try:
            self._materialize_sublog(target_schema)
            self._materialize_common_sublog_views(target_schema)
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

    def _root_table(self) -> str:
        """
        Return the fully-qualified root table.
        """
        return f"{self.ocel.schema}.{self.root_table_name}"

    def _context(self) -> CompileContext:
        """
        Build the root compilation context for this query.
        """
        return CompileContext(
            alias=self.root_alias,
            schema=self.ocel.schema,
            kind=self.root_kind,
        )

    def _where_sql(self) -> str:
        """
        Compile the WHERE clause for this root query.
        """
        ctx = self._context()
        predicates: list[str] = []

        type_filter = self._type_filter_expr()
        if type_filter is not None:
            predicates.append(render_bool_expr(type_filter, ctx))

        for expr in self.filters:
            predicates.append(render_bool_expr(expr, ctx))

        return " AND ".join(predicates) if predicates else "TRUE"

    def _materialize_common_sublog_views(self, target_schema: str) -> None:
        """
        Materialize the common OCEL relation/object closure views once the target
        event view already exists.
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

    @abstractmethod
    def _clone_with_filters(self, filters: Tuple[BoolExpr, ...]) -> Self:
        """
        Return a copy of this query with the given filters.
        """
        raise NotImplementedError

    @abstractmethod
    def _type_filter_expr(self) -> Optional[BoolExpr]:
        """
        Return the boolean expression restricting the root types, if any.
        """
        raise NotImplementedError

    @abstractmethod
    def _materialize_sublog(self, target_schema: str) -> None:
        """
        Materialize a derived OCEL schema for this query.
        """
        raise NotImplementedError
