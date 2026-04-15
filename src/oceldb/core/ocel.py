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
    from oceldb.query.root import OCELQueryRoot


class OCEL:
    """
    Stable handle for a single on-disk OCEL dataset.

    The class owns the DuckDB connection used to query the dataset and exposes
    the stable query interface of the library:

    - `query` for lazy analysis and sublog construction

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

    `manifest.json` is the canonical storage contract. It always identifies the
    five logical tables, but only the wide attribute-bearing tables carry
    attribute metadata:

    - `tables.event.custom_columns` and `tables.event.type_attributes`
      describe event attributes
    - `tables.object_change.custom_columns` and
      `tables.object_change.type_attributes` describe object-history
      attributes
    - `tables.object`, `tables.event_object`, and `tables.object_object` do
      not carry custom attribute metadata in the manifest because those tables
      do not have logical type-specific attributes in oceldb's representation

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

                - `manifest.json`: storage metadata and logical table schemas.
                  Only `event` and `object_change` carry custom attribute
                  metadata in the manifest.
                - `event.parquet`: event rows with core event columns and
                  custom event attributes
                - `object.parquet`: object identities with one row per object
                - `object_change.parquet`: temporal object history rows with
                  object state attributes
                - `event_object.parquet`: event-to-object relations with
                  `ocel_event_id`, `ocel_object_id`, and `ocel_qualifier`
                - `object_object.parquet`: object-to-object relations with
                  `ocel_source_id`, `ocel_target_id`, and `ocel_qualifier`

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
          `ocel_event_id`, `ocel_object_id`, and `ocel_qualifier`.
        - `object_object` contains object-to-object relation edges with
          `ocel_source_id`, `ocel_target_id`, and `ocel_qualifier`.

        Manifest interpretation:

        - `ocel.manifest.table("event").custom_columns` and
          `type_attributes` describe the event table's wide custom columns.
        - `ocel.manifest.table("object_change").custom_columns` and
          `type_attributes` describe the object-history table's wide custom
          columns.
        - `object`, `event_object`, and `object_object` expose only their core
          columns. In the manifest these tables intentionally do not declare
          custom columns or type-attribute ownership.

        Practical implications:

        - Query `object` when you need identities or object counts.
        - Query `object_change` when you need raw object history.
        - Prefer the DSL entrypoint `ocel.query.object_states(...)` when you
          need reconstructed object state such as the latest state or a state
          as of a timestamp.

        Use `ocel.manifest.table(name).columns` to inspect the exact available
        columns for each logical table. For wide event and object-history
        tables, `ocel.manifest.table(name).type_attributes` records which
        custom attributes belong to which OCEL type.

        This is an advanced escape hatch. Prefer the DSL for library-facing
        features so analysis logic remains backend-controlled. For descriptive
        summaries and mined process models, use the pure functions in
        `oceldb.inspect` and `oceldb.discovery`.

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


def ocel_connection(ocel: OCEL) -> duckdb.DuckDBPyConnection:
    """Return the live DuckDB connection owned by an OCEL handle."""
    return ocel._con  # pyright: ignore[reportPrivateUsage]


def ocel_available_columns(
    ocel: OCEL,
    table_name: QuerySourceKind | str,
    *,
    selected_types: tuple[str, ...] | None = None,
) -> dict[str, str]:
    def _custom_columns(name: LogicalTableName) -> dict[str, str]:
        schema = ocel.manifest.table(name)
        if selected_types is None:
            return dict(schema.custom_columns)
        return schema.custom_columns_for_types(selected_types)

    if table_name in {"object_state", "object_state_at_event"}:
        return {
            **ocel.manifest.table("object").columns,
            "ocel_time": "TIMESTAMP",
            **_custom_columns("object_change"),
        }
    if table_name == "event":
        return {
            **dict(ocel.manifest.table("event").core_columns),
            **_custom_columns("event"),
        }
    if table_name == "object":
        return ocel.manifest.table("object").columns
    if table_name == "event_occurrence":
        return {
            "ocel_event_id": "VARCHAR",
            "ocel_event_type": "VARCHAR",
            "ocel_event_time": "TIMESTAMP",
            "ocel_object_id": "VARCHAR",
            "ocel_object_type": "VARCHAR",
        }
    if table_name == "object_change":
        return {
            **dict(ocel.manifest.table("object_change").core_columns),
            **_custom_columns("object_change"),
        }
    if table_name == "event_object":
        return ocel.manifest.table("event_object").columns
    if table_name == "object_object":
        return ocel.manifest.table("object_object").columns
    raise TypeError(f"Unsupported table name: {table_name!r}")


def logical_table_sql(table_name: LogicalTableName) -> str:
    escaped = table_name.replace('"', '""')
    return f'"{escaped}"'
