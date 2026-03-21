"""Core Ocel class for reading, querying, and exporting OCEL 2.0 event logs."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from oceldb._util import query_type_map, sql_path
from oceldb.expr import Domain, Expr


@dataclass
class Summary:
    """Lightweight summary of an OCEL 2.0 log.

    All fields are computed via scalar DuckDB queries — no event or
    object data is materialized into Python memory.
    """

    num_events: int
    num_objects: int
    num_event_types: int
    num_object_types: int
    event_types: list[str]
    object_types: list[str]
    num_e2o_relations: int
    num_o2o_relations: int


class ViewBuilder:
    """Accumulates filter expressions and creates a filtered :class:`Ocel`.

    Obtain an instance via :meth:`Ocel.view`, chain ``.filter()`` calls,
    then call ``.create()``::

        filtered = (
            ocel.view()
            .filter(event.type == "Create Order")
            .filter(event.time > "2022-01-01")
            .create()
        )

    Each ``.filter()`` call adds a SQL WHERE condition.  Filters referencing
    event attributes and object attributes are classified automatically —
    a single expression must not mix both domains.
    """

    def __init__(self, ocel: Ocel) -> None:
        self._ocel = ocel
        self._filters: list[Expr] = []

    def filter(self, expr: Expr) -> ViewBuilder:
        """Append a filter expression.  Returns ``self`` for chaining."""
        self._filters.append(expr)
        return self

    def create(self) -> Ocel:
        """Materialize the accumulated filters into a new view-backed :class:`Ocel`.

        The returned instance shares the DuckDB connection with its parent
        and is backed by lazy views — no data is copied until you call
        :meth:`Ocel.to_sqlite` or :meth:`Ocel.to_pm4py`.
        """
        from oceldb.writer import create_views

        event_filters, object_filters = self._classify_by_domain()
        target = create_views(
            self._ocel._con,
            self._ocel._schema_prefix,
            event_filters,
            object_filters,
        )
        return Ocel(
            con=self._ocel._con,
            path=self._ocel._path,
            schema_prefix=target,
            owns_connection=False,
        )

    def _classify_by_domain(self) -> tuple[list[Expr], list[Expr]]:
        """Split filters into event-domain and object-domain lists.

        Raises :class:`ValueError` if a single expression references
        columns from both domains.
        """
        event_filters: list[Expr] = []
        object_filters: list[Expr] = []

        for f in self._filters:
            cols = f.columns()
            domains = {c.domain for c in cols}

            if not domains:
                raise ValueError(
                    "Filter expression must reference at least one "
                    "event or object column."
                )

            if len(domains) > 1:
                raise ValueError(
                    "A single filter expression cannot mix event and object columns. "
                    "Use separate .filter() calls."
                )

            domain = next(iter(domains))
            if domain == Domain.EVENT:
                event_filters.append(f)
            else:
                object_filters.append(f)

        return event_filters, object_filters


class Ocel:
    """An OCEL 2.0 event log backed by a DuckDB connection.

    Instances are either *file-backed* (created via :meth:`read`, owns the
    connection) or *view-backed* (created via :meth:`ViewBuilder.create`,
    shares the connection with the parent).

    Use as a context manager to ensure cleanup::

        with Ocel.read("log.sqlite") as ocel:
            ...
    """

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        path: Path | None,
        schema_prefix: str,
        owns_connection: bool = True,
    ) -> None:
        self._con = con
        self._path = path
        self._schema_prefix = schema_prefix
        self._owns_connection = owns_connection

    @classmethod
    def read(cls, path: str | Path) -> Ocel:
        """Open an OCEL 2.0 SQLite file for reading.

        The file is attached read-only to a new in-memory DuckDB connection.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"OCEL file not found: {path}")

        con = duckdb.connect()
        con.execute(f"ATTACH '{sql_path(path)}' AS ocel_db (TYPE SQLITE, READ_ONLY)")
        con.execute("USE ocel_db")

        return cls(con=con, path=path, schema_prefix="ocel_db.main")

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute an arbitrary SQL query against this log's tables."""
        self._con.execute(f"USE {self._schema_prefix}")
        return self._con.sql(query)

    def view(self) -> ViewBuilder:
        """Start building a filtered view of this log.

        Returns a :class:`ViewBuilder` that accepts chained ``.filter()``
        calls.  Call ``.create()`` on the builder to obtain a new, filtered
        :class:`Ocel` instance backed by lazy DuckDB views.
        """
        return ViewBuilder(self)

    # -- Schema inspection ----------------------------------------------------

    def event_types(self) -> duckdb.DuckDBPyRelation:
        """Distinct event types in this log."""
        return self.sql("SELECT DISTINCT ocel_type FROM event ORDER BY ocel_type")

    def object_types(self) -> duckdb.DuckDBPyRelation:
        """Distinct object types in this log."""
        return self.sql("SELECT DISTINCT ocel_type FROM object ORDER BY ocel_type")

    def events(self) -> duckdb.DuckDBPyRelation:
        """All events (``ocel_id``, ``ocel_type``)."""
        return self.sql("SELECT * FROM event")

    def objects(self) -> duckdb.DuckDBPyRelation:
        """All objects (``ocel_id``, ``ocel_type``)."""
        return self.sql("SELECT * FROM object")

    def event_objects(self) -> duckdb.DuckDBPyRelation:
        """All event-to-object relationships."""
        return self.sql("SELECT * FROM event_object")

    def object_objects(self) -> duckdb.DuckDBPyRelation:
        """All object-to-object relationships."""
        return self.sql("SELECT * FROM object_object")

    def attributes(self, entity: Domain, ocel_type: str) -> list[str]:
        """Return per-type attribute names for an event or object type.

        Args:
            entity: :attr:`Domain.EVENT` or :attr:`Domain.OBJECT`.
            ocel_type: The OCEL type name (e.g. ``"Create Order"``).

        Returns:
            Column names excluding ``ocel_id`` and ``ocel_time``.
        """
        type_map = query_type_map(self._con, self._schema_prefix, entity.value)
        table = type_map.get(ocel_type)
        if table is None:
            raise ValueError(
                f"Unknown {entity.value} type: {ocel_type!r}. Known types: {list(type_map)}"
            )
        self._con.execute(f"USE {self._schema_prefix}")
        cols = self._con.sql(f'DESCRIBE "{table}"').fetchall()
        return [row[0] for row in cols if row[0] not in ("ocel_id", "ocel_time")]

    def summary(self) -> Summary:
        """Compute a lightweight summary of this log.

        All counts are evaluated lazily via DuckDB — no data is loaded
        into Python beyond the scalar results.
        """

        def count(table: str) -> int:
            return self.sql(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        def distinct_types(table: str) -> list[str]:
            rows = self.sql(
                f"SELECT DISTINCT ocel_type FROM {table} ORDER BY ocel_type"
            ).fetchall()
            return [r[0] for r in rows]

        et = distinct_types("event")
        ot = distinct_types("object")
        return Summary(
            num_events=count("event"),
            num_objects=count("object"),
            num_event_types=len(et),
            num_object_types=len(ot),
            event_types=et,
            object_types=ot,
            num_e2o_relations=count("event_object"),
            num_o2o_relations=count("object_object"),
        )

    # -- Export ----------------------------------------------------------------

    def to_sqlite(self, path: str | Path) -> None:
        """Materialize this log (including any active filters) to a SQLite file.

        The output conforms to the OCEL 2.0 SQLite standard and can be read
        by pm4py or any other OCEL 2.0 consumer.
        """
        path = Path(path)
        if path.exists():
            path.unlink()

        self._con.execute(f"USE {self._schema_prefix}")
        self._con.execute(f"ATTACH '{sql_path(path)}' AS _ocel_export (TYPE SQLITE)")
        try:
            for table in [
                "event",
                "object",
                "event_object",
                "object_object",
                "event_map_type",
                "object_map_type",
            ]:
                self._con.execute(
                    f"CREATE TABLE _ocel_export.{table} AS SELECT * FROM {table}"
                )
            for entity in ("event", "object"):
                for table in query_type_map(
                    self._con, self._schema_prefix, entity
                ).values():
                    self._con.execute(
                        f'CREATE TABLE _ocel_export."{table}" AS '
                        f'SELECT * FROM "{table}"'
                    )
        finally:
            self._con.execute("DETACH _ocel_export")

    def to_pm4py(self) -> Any:
        """Export this log as a ``pm4py.OCEL`` object.

        Requires the optional ``pm4py`` dependency::

            pip install oceldb[pm4py]

        File-backed instances read directly from disk.  View-backed instances
        materialize to a temporary SQLite file first.
        """
        try:
            import pm4py
        except ImportError:
            raise ImportError(
                "pm4py is required for to_pm4py(). "
                "Install it with: pip install oceldb[pm4py]"
            ) from None

        if self._owns_connection and self._path is not None:
            return pm4py.read.read_ocel2_sqlite(str(self._path))
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            self.to_sqlite(tmp_path)
            return pm4py.read.read_ocel2_sqlite(str(tmp_path))
        finally:
            tmp_path.unlink()

    def close(self) -> None:
        """Release resources.

        View-backed instances drop their DuckDB schema.  File-backed
        instances close the underlying connection.
        """
        if not self._owns_connection:
            self._con.execute(f"DROP SCHEMA IF EXISTS {self._schema_prefix} CASCADE")
        else:
            self._con.close()

    def __enter__(self) -> Ocel:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Ocel('{self._path}')"
