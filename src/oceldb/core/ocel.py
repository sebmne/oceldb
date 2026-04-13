"""Public dataset handle for on-disk oceldb datasets."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from tempfile import TemporaryDirectory
from types import TracebackType
from typing import TYPE_CHECKING

import duckdb

from oceldb.core.manifest import LogicalTableName, OCELManifest, QuerySourceKind

if TYPE_CHECKING:
    from oceldb.inspect.inspector import OCELInspector
    from oceldb.query.root import OCELQueryRoot


class OCEL:
    """
    Stable handle for a single on-disk OCEL dataset.

    The class owns the DuckDB connection used to query the dataset and exposes
    the two stable high-level interfaces of the library:

    - `query` for lazy analysis and sublog construction
    - `inspect` for higher-level descriptive summaries

    The canonical oceldb storage format is a directory with these files:

    - `manifest.json`
    - `event.parquet`
    - `object.parquet`
    - `object_change.parquet`
    - `event_object.parquet`
    - `object_object.parquet`

    These files map to the same five logical tables exposed through the DSL and
    the raw SQL escape hatch. In particular, object identity and object history
    are stored separately:

    - `object` contains one row per object identity
    - `object_change` contains temporal object state updates

    Notes:
        `OCEL` intentionally stays thin. It does not materialize the log into
        Python objects and should be treated as a resource-owning handle around
        an on-disk dataset.
    """

    def __init__(
        self,
        path: Path,
        con: duckdb.DuckDBPyConnection,
        manifest: OCELManifest,
        *,
        tempdir: TemporaryDirectory[str] | None = None,
    ) -> None:
        """
        Initialize an `OCEL` handle around an already opened dataset.

        Library users should normally call `OCEL.read(...)` instead of
        instantiating `OCEL` directly.

        Args:
            path: Directory path that identifies the dataset for the user.
            con: Open DuckDB connection that serves the dataset tables.
            manifest: Loaded storage manifest for the dataset.
            tempdir: Temporary directory to clean up when the handle closes.
        """
        self._path = path
        self._con = con
        self._manifest = manifest
        self._tempdir = tempdir
        self._closed = False
        self._table_refs: dict[LogicalTableName, str] = {
            table_name: table_name
            for table_name in manifest.tables
        }

    @property
    def path(self) -> Path:
        """
        Return the directory path associated with this dataset handle.

        For persisted datasets this is the directory passed to `OCEL.read(...)`.
        For materialized temporary sublogs this points at the generated working
        directory that backs the handle.
        """
        return self._path

    @property
    def manifest(self) -> OCELManifest:
        """
        Return the immutable manifest describing the dataset schema and provenance.
        """
        return self._manifest

    @cached_property
    def inspect(self) -> OCELInspector:
        """
        Access the inspection helpers layered on top of the core DSL.

        The returned inspector is cached per handle.
        """
        from oceldb.inspect.inspector import OCELInspector

        return OCELInspector(self)

    @cached_property
    def query(self) -> OCELQueryRoot:
        """
        Return the root object for the lazy oceldb query DSL.

        The returned query root is cached per handle.
        """
        from oceldb.query.root import OCELQueryRoot

        return OCELQueryRoot(self)

    @staticmethod
    def read(path: str | Path) -> "OCEL":
        """
        Open an OCEL dataset from its canonical directory layout.

        Args:
            path: Directory containing the canonical oceldb files:

                - `manifest.json`: storage metadata and logical table schemas
                - `event.parquet`: event rows with core event columns and
                  custom event attributes
                - `object.parquet`: object identities with one row per object
                - `object_change.parquet`: temporal object history rows with
                  object state attributes
                - `event_object.parquet`: event-to-object relations
                - `object_object.parquet`: object-to-object relations

        Returns:
            An open `OCEL` handle.
        """
        from oceldb.io.read import read_ocel

        return read_ocel(path)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Run ad-hoc SQL against the underlying DuckDB connection.

        The canonical oceldb logical tables are registered in the connection as
        unqualified table names:

        - `event`
        - `object`
        - `object_change`
        - `event_object`
        - `object_object`

        Table semantics:

        - `event` contains one row per event. Core columns are
          `ocel_id`, `ocel_type`, and `ocel_time`. Any custom event attributes
          discovered during conversion are materialized as additional typed
          columns.
        - `object` contains one row per object identity. Core columns are
          `ocel_id` and `ocel_type`. This table does not contain temporal object
          state attributes.
        - `object_change` contains the temporal object history. Core columns are
          `ocel_id`, `ocel_type`, `ocel_time`, and `ocel_changed_field`.
          Custom object attributes are stored here as typed columns and may be
          null on rows where the attribute was not updated.
        - `event_object` contains event-to-object incidence edges with
          `ocel_event_id` and `ocel_object_id`.
        - `object_object` contains object-to-object relation edges with
          `ocel_source_id` and `ocel_target_id`.

        Practical implications:

        - Query `object` when you need identities or object counts.
        - Query `object_change` when you need raw object history.
        - Prefer the DSL entrypoint `ocel.query.object_states(...)` when you
          need reconstructed object state such as the latest state or a state
          as of a timestamp.

        Use `ocel.manifest.table(name).columns` to inspect the exact available
        columns for each logical table, including custom event columns and
        custom object-history columns.

        This is an advanced escape hatch. Prefer the DSL for library-facing
        features so analysis logic remains backend-controlled.

        Args:
            query: SQL string executed against the internal DuckDB connection.

        Returns:
            The resulting DuckDB relation.
        """
        return self._con.sql(query)

    def write(
        self,
        target: str | Path,
        *,
        overwrite: bool = False,
    ) -> Path:
        """
        Persist this dataset to a canonical oceldb directory.

        Args:
            target: Destination directory.
            overwrite: Replace an existing destination directory.

        Returns:
            The resolved target directory path.
        """
        from oceldb.io.write import write_ocel

        return write_ocel(self, target, overwrite=overwrite)

    def _available_columns(self, table_name: QuerySourceKind | str) -> dict[str, str]:
        if table_name in {"object_state", "object_state_at_event"}:
            return {
                **self._manifest.table("object").columns,
                "ocel_time": "TIMESTAMP",
                **self._manifest.table("object_change").custom_columns,
            }
        if table_name == "event":
            return self._manifest.table("event").columns
        if table_name == "object":
            return self._manifest.table("object").columns
        if table_name == "object_change":
            return self._manifest.table("object_change").columns
        if table_name == "event_object":
            return self._manifest.table("event_object").columns
        if table_name == "object_object":
            return self._manifest.table("object_object").columns
        raise TypeError(f"Unsupported table name: {table_name!r}")

    def _table_sql(self, table_name: LogicalTableName) -> str:
        alias = self._table_refs[table_name]
        escaped = alias.replace('"', '""')
        return f'"{escaped}"'

    def close(self) -> None:
        """
        Close the underlying DuckDB connection and release temporary resources.

        The method is idempotent and may be called multiple times safely.
        """
        if self._closed:
            return

        try:
            self._con.close()
        finally:
            self._closed = True
            if self._tempdir is not None:
                self._tempdir.cleanup()
                self._tempdir = None

    def __enter__(self) -> "OCEL":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc, traceback
        self.close()

    def __repr__(self) -> str:
        return f"OCEL(path='{self._path.name}')"
