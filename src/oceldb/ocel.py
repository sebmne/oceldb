"""Core Ocel class — the main entry point for oceldb."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import duckdb

from oceldb.types import Domain, Summary
from oceldb.utils import escape_string
from oceldb.view import ViewBuilder


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
        """Initialize an Ocel instance.

        This is an internal constructor. Use :meth:`read` to open a file,
        or :meth:`view` / :meth:`ViewBuilder.create` to create filtered views.

        Args:
            con: An open DuckDB connection with the OCEL schema attached.
            path: Path to the source SQLite file, or ``None`` for in-memory views.
            schema_prefix: Fully qualified schema prefix (e.g. ``"ocel_db.main"``
                or ``"memory.oceldb_view_abc12345"``).
            owns_connection: If ``True``, :meth:`close` shuts down the connection.
                If ``False``, it only drops the view schema.
        """
        self._con = con
        self._path = path
        self._schema_prefix = schema_prefix
        self._owns_connection = owns_connection

    @classmethod
    def read(cls, path: str | Path) -> Ocel:
        """Open an OCEL 2.0 SQLite file for reading.

        The file is attached read-only to a fresh in-memory DuckDB connection.
        Use as a context manager to ensure the connection is cleaned up::

            with Ocel.read("order_log.sqlite") as ocel:
                print(ocel.summary())

        Args:
            path: Path to an OCEL 2.0 SQLite file.

        Returns:
            A file-backed :class:`Ocel` instance that owns its connection.

        Raises:
            FileNotFoundError: If *path* does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"OCEL file not found: {path}")

        con = duckdb.connect()
        con.execute(
            f"ATTACH '{escape_string(str(path))}' AS ocel_db (TYPE SQLITE, READ_ONLY)"
        )
        con.execute("USE ocel_db")

        return cls(con=con, path=path, schema_prefix="ocel_db.main")

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query against this log's tables.

        The connection's active schema is set to this instance's schema before
        execution, so unqualified table names (``event``, ``object``, etc.)
        resolve correctly even for filtered views.

        Args:
            query: Any valid DuckDB SQL statement.

        Returns:
            A :class:`duckdb.DuckDBPyRelation` with the query results.
        """
        self._con.execute(f"USE {self._schema_prefix}")
        return self._con.sql(query)

    def view(self) -> ViewBuilder:
        """Start building a filtered view of this log.

        Returns a :class:`~oceldb.view.ViewBuilder`.  Chain ``.where()``
        calls to add conditions, then call ``.create()`` to materialize::

            filtered = ocel.view().where(event.type == "X").create()
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
        """Return the attribute column names for a specific event or object type.

        Args:
            entity: :attr:`Domain.EVENT` or :attr:`Domain.OBJECT`.
            ocel_type: The OCEL type name (e.g. ``"Create Order"``).

        Returns:
            Column names of the per-type table, including ``ocel_id``.

        Raises:
            ValueError: If *ocel_type* is not present in the log.
        """
        rows = self.sql(
            f"SELECT ocel_type, ocel_type_map FROM {entity.value}_map_type"
        ).fetchall()
        type_map = {row[0]: f"{entity.value}_{row[1]}" for row in rows}
        table = type_map.get(ocel_type)
        if table is None:
            raise ValueError(
                f"Unknown {entity.value} type: {ocel_type!r}. "
                f"Known types: {list(type_map)}"
            )
        cols = self.sql(f'DESCRIBE "{table}"').fetchall()
        return [row[0] for row in cols]

    def summary(self) -> Summary:
        """Compute a lightweight summary of this log."""
        et = [r[0] for r in self.event_types().fetchall()]
        ot = [r[0] for r in self.object_types().fetchall()]

        return Summary(
            num_events=self._count("event"),
            num_objects=self._count("object"),
            num_event_types=len(et),
            num_object_types=len(ot),
            event_types=et,
            object_types=ot,
            num_e2o_relations=self._count("event_object"),
            num_o2o_relations=self._count("object_object"),
        )

    def _count(self, table: str) -> int:
        count = self.sql(f"SELECT COUNT(*) FROM {table}").fetchone()
        if count is None:
            raise ValueError(f"Unknown table: {table!r}.")
        return count[0]

    # -- Export ----------------------------------------------------------------

    def to_sqlite(self, path: str | Path) -> None:
        """Export this log to a new OCEL 2.0 SQLite file.

        Writes all tables (core, relationship, map, and per-type) visible
        through the current schema — so filtered views export only the
        surviving subset.

        Args:
            path: Destination file path. Any existing file is overwritten.
        """
        path = Path(path)
        if path.exists():
            path.unlink()

        self.sql(f"ATTACH '{escape_string(str(path))}' AS _ocel_export (TYPE SQLITE)")
        try:
            for table in [
                "event",
                "object",
                "event_object",
                "object_object",
                "event_map_type",
                "object_map_type",
            ]:
                self.sql(f"CREATE TABLE _ocel_export.{table} AS SELECT * FROM {table}")

            for entity in [Domain.EVENT.value, Domain.OBJECT.value]:
                tables = self.sql(
                    f"SELECT DISTINCT ocel_type_map FROM {entity}_map_type"
                ).fetchall()
                for (table,) in tables:
                    self.sql(
                        f'CREATE TABLE _ocel_export."{entity}_{table}" AS '
                        f'SELECT * FROM "{entity}_{table}"'
                    )
        finally:
            self.sql("DETACH _ocel_export")

    def to_pm4py(self) -> Any:
        """Export this log as a ``pm4py.OCEL`` object.

        For file-backed instances, reads the original file directly. For
        view-backed instances, materializes to a temporary SQLite file first.

        Returns:
            A ``pm4py.OCEL`` object.

        Raises:
            ImportError: If ``pm4py`` is not installed. Install it with
                ``pip install oceldb[pm4py]``.
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
        """Release resources held by this instance.

        For file-backed instances (``owns_connection=True``), closes the
        DuckDB connection. For view-backed instances, drops the in-memory
        schema that holds the filtered views.
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
