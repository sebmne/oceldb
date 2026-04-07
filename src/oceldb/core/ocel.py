"""Core OCEL class — pure data representation of an OCEL 2.0 log."""

from __future__ import annotations

from pathlib import Path

import duckdb

from oceldb.core.metadata import OCELMetadata


class OCEL:
    """
    Pure data representation of an OCEL 2.0 log.

    Each OCEL instance is bound to a DuckDB schema containing the fixed logical
    OCEL tables:

        - event
        - object
        - event_object
        - object_object

    Do not instantiate this class directly. Use `oceldb.io.read_ocel()` instead.
    """

    def __init__(
        self,
        path: Path,
        con: duckdb.DuckDBPyConnection,
        metadata: OCELMetadata,
        schema: str,
        owns_connection: bool = True,
    ) -> None:
        self._path = path
        self._con = con
        self._meta = metadata
        self._schema = schema
        self._owns_connection = owns_connection

    # ----- Public Properties -----

    @property
    def path(self) -> Path:
        """The absolute path to the underlying `.oceldb` directory."""
        return self._path

    @property
    def metadata(self) -> OCELMetadata:
        """Provenance information regarding the log's creation."""
        return self._meta

    @property
    def schema(self) -> str:
        """The DuckDB schema backing this OCEL instance."""
        return self._schema

    # ----- Inspection -----

    @property
    def inspect(self):
        """
        Access the OCEL inspection and descriptive summary interface.
        """
        from oceldb.inspect.inspector import OCELInspector

        return OCELInspector(self)

    @property
    def visualize(self):
        from oceldb.visualize.api import OCELVisualizer

        return OCELVisualizer(self)

    # ----- Safe Data Access API -----

    def objects(self, *types: str):
        """
        Start a lazy object-centric query.

        Examples:
            ocel.objects()
            ocel.objects("Order")
            ocel.objects("Order", "Invoice")
        """
        from oceldb.query.object_query import ObjectQuery

        return ObjectQuery(
            ocel=self,
            object_types=tuple(types),
        )

    def events(self, *types: str):
        """
        Start a lazy event-centric query.

        Examples:
            ocel.events()
            ocel.events("Create Order")
            ocel.events("Create Order", "Cancel Order")
        """
        from oceldb.query.event_query import EventQuery

        return EventQuery(
            ocel=self,
            event_types=tuple(types),
        )

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Run an ad-hoc SQL query against the underlying DuckDB connection.

        Example:
            >>> ocel.sql(f"SELECT * FROM {ocel.schema}.event LIMIT 10").df()

        Args:
            query: A valid DuckDB SQL string.

        Returns:
            A DuckDB relation.
        """
        return self._con.sql(query)

    # ----- Lifecycle Methods -----

    def close(self) -> None:
        """Safely close the DuckDB connection."""
        if self._owns_connection:
            self._con.close()
        else:
            self._con.execute(f"DROP SCHEMA {self._schema} CASCADE")

    def __enter__(self) -> OCEL:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"OCEL('{self._path.name}', schema='{self._schema}')"
