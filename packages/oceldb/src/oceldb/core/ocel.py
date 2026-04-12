"""Stable dataset handle for parquet-backed OCEL logs."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from oceldb.core.metadata import LogicalTableName, OCELManifest, OCELMetadata


class OCEL:
    """
    Stable handle for an OCEL dataset stored in the oceldb format.

    `OCEL` is intentionally thin. It owns the dataset lifecycle, storage
    metadata, and the top-level entrypoints into querying and export.
    """

    def __init__(
        self,
        path: Path,
        con: duckdb.DuckDBPyConnection,
        manifest: OCELManifest,
        schema: str,
        *,
        data_path: Path | None = None,
        owns_connection: bool = True,
        tempdir: TemporaryDirectory[str] | None = None,
    ) -> None:
        self._path = path
        self._data_path = path if data_path is None else data_path
        self._con = con
        self._manifest = manifest
        self._schema = schema
        self._owns_connection = owns_connection
        self._tempdir = tempdir

    @property
    def path(self) -> Path:
        """The original directory or archive path used to open the dataset."""
        return self._path

    @property
    def metadata(self) -> OCELMetadata:
        """Stable metadata about the dataset."""
        return self._manifest.metadata

    @property
    def manifest(self) -> OCELManifest:
        """Advanced storage manifest used internally by the query engine."""
        return self._manifest

    @property
    def schema(self) -> str:
        """The DuckDB schema backing this OCEL instance."""
        return self._schema

    @property
    def inspect(self):
        """
        Access the inspection helpers layered on top of the core DSL.
        """
        from oceldb.inspect.inspector import OCELInspector

        return OCELInspector(self)

    def query(self):
        """
        Return the unified lazy query root for this dataset.
        """
        from oceldb.query.root import OCELQueryRoot

        return OCELQueryRoot(self)

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Run ad-hoc SQL against the underlying DuckDB connection.

        This is an advanced escape hatch. Prefer the DSL for library-facing
        features so analysis logic remains backend-controlled.
        """
        return self._con.sql(query)

    def write(
        self,
        target: str | Path,
        *,
        overwrite: bool = False,
        packaged: bool = False,
    ) -> Path:
        """
        Persist this dataset to the oceldb format.

        Args:
            target: Destination directory or archive path.
            overwrite: Replace an existing destination.
            packaged: When true, write a single-file archive.
        """
        from oceldb.io.write import write_ocel

        return write_ocel(
            self,
            target,
            overwrite=overwrite,
            packaged=packaged,
        )

    def available_columns(self, table_name: LogicalTableName) -> dict[str, str]:
        return self._manifest.table(table_name).columns

    def close(self) -> None:
        """Close the DuckDB connection and any extracted archive resources."""
        try:
            if self._owns_connection:
                self._con.close()
            else:
                self._con.execute(f'DROP SCHEMA "{self._schema}" CASCADE')
        finally:
            if self._tempdir is not None:
                self._tempdir.cleanup()
                self._tempdir = None

    def __enter__(self) -> "OCEL":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"OCEL(path='{self._path.name}', "
            f"packaging='{self.metadata.packaging}', "
            f"schema='{self._schema}')"
        )
