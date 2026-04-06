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
        - event_type
        - object_type

    Do not instantiate this class directly. Use `oceldb.io.read_ocel()` instead.
    """

    def __init__(
        self,
        path: Path,
        con: duckdb.DuckDBPyConnection,
        metadata: OCELMetadata,
        schema: str,
    ) -> None:
        self._path = path
        self._con = con
        self._meta = metadata
        self._schema = schema

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

    # ----- Lazy Inspection Methods -----

    @property
    def event_types(self) -> list[str]:
        """
        Lazy inspection of event types declared in the log.

        This reads from the dedicated `event_type` table, not from distinct
        types present in `event`, so declared-but-empty types are preserved.
        """
        query = f"SELECT DISTINCT ocel_type FROM {self.schema}.event_type"
        return sorted(r[0] for r in self.sql(query).fetchall())

    @property
    def object_types(self) -> list[str]:
        """
        Lazy inspection of object types declared in the log.

        This reads from the dedicated `object_type` table, not from distinct
        types present in `object`, so declared-but-empty types are preserved.
        """
        query = f"SELECT DISTINCT ocel_type FROM {self.schema}.object_type"
        return sorted(r[0] for r in self.sql(query).fetchall())

    def get_event_attributes(self, event_type: str) -> list[str]:
        """
        Dynamically extract the attributes currently attached to a specific event type.

        Args:
            event_type: The exact string name of the event type.

        Returns:
            A sorted list of custom attribute keys.
        """
        escaped = event_type.replace("'", "''")
        query = f"""
            SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
            FROM {self.schema}.event
            WHERE ocel_type = '{escaped}' AND attributes IS NOT NULL
        """
        return sorted(r[0] for r in self.sql(query).fetchall())

    def get_object_attributes(self, object_type: str) -> list[str]:
        """
        Dynamically extract the attributes currently attached to a specific object type.

        Args:
            object_type: The exact string name of the object type.

        Returns:
            A sorted list of custom attribute keys.
        """
        escaped = object_type.replace("'", "''")
        query = f"""
            SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
            FROM {self.schema}.object
            WHERE ocel_type = '{escaped}' AND attributes IS NOT NULL
        """
        return sorted(r[0] for r in self.sql(query).fetchall())

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
        self._con.close()

    def __enter__(self) -> OCEL:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"OCEL('{self._path.name}', schema='{self._schema}')"
