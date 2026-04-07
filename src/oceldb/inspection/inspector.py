from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

import duckdb

if TYPE_CHECKING:
    from oceldb.core.ocel import OCEL


@dataclass(frozen=True)
class OCELOverview:
    """
    High-level overview statistics of an OCEL.
    """

    event_count: int
    object_count: int
    object_record_count: int
    event_type_count: int
    object_type_count: int
    event_object_count: int
    object_object_count: int


@dataclass(frozen=True)
class EventObjectStats:
    """
    Structural statistics for the event-object relation.
    """

    avg_objects_per_event: Optional[float]
    min_objects_per_event: Optional[int]
    max_objects_per_event: Optional[int]
    avg_events_per_object: Optional[float]
    min_events_per_object: Optional[int]
    max_events_per_object: Optional[int]


class OCELInspector:
    """
    Inspection and descriptive summary interface for an OCEL.

    This object exposes:
        - type discovery
        - attribute discovery
        - overview statistics
        - per-type counts
        - structural relation statistics

    The inspector uses the OCEL public API and query DSL where possible, and
    falls back to ad-hoc SQL only for tasks that are inherently schema- or
    relation-inspection oriented.
    """

    def __init__(self, ocel: OCEL) -> None:
        self._ocel = ocel

    # -------------------------------------------------------------------------
    # Type inspection
    # -------------------------------------------------------------------------

    def event_types(self) -> list[str]:
        """
        Return the sorted list of event types present in the OCEL.
        """
        rows = self._ocel.sql(f"""
            SELECT DISTINCT ocel_type
            FROM {self._ocel.schema}.event
            ORDER BY ocel_type
        """).fetchall()
        return [row[0] for row in rows]

    def object_types(self) -> list[str]:
        """
        Return the sorted list of object types present in the OCEL.

        Uses the physical object history table and returns the distinct types
        represented there.
        """
        rows = self._ocel.sql(f"""
            SELECT DISTINCT ocel_type
            FROM {self._ocel.schema}.object
            ORDER BY ocel_type
        """).fetchall()
        return [row[0] for row in rows]

    def types(self) -> dict[str, list[str]]:
        """
        Return both event and object types.

        Returns:
            A dictionary with keys:
                - "event"
                - "object"
        """
        return {
            "event": self.event_types(),
            "object": self.object_types(),
        }

    # -------------------------------------------------------------------------
    # Attribute inspection
    # -------------------------------------------------------------------------

    def event_attributes(self, event_type: str) -> list[str]:
        """
        Return the sorted list of custom attributes used by a given event type.

        Args:
            event_type: Exact event type name.

        Returns:
            A sorted list of custom attribute keys extracted from the JSON
            `attributes` column.
        """
        escaped = event_type.replace("'", "''")
        rows = self._ocel.sql(f"""
            SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
            FROM {self._ocel.schema}.event
            WHERE ocel_type = '{escaped}'
              AND attributes IS NOT NULL
            ORDER BY 1
        """).fetchall()
        return [row[0] for row in rows]

    def object_attributes(self, object_type: str) -> list[str]:
        """
        Return the sorted list of custom attributes used by a given object type.

        Args:
            object_type: Exact object type name.

        Returns:
            A sorted list of custom attribute keys extracted from the JSON
            `attributes` column.
        """
        escaped = object_type.replace("'", "''")
        rows = self._ocel.sql(f"""
            SELECT DISTINCT UNNEST(json_keys(attributes::JSON))
            FROM {self._ocel.schema}.object
            WHERE ocel_type = '{escaped}'
              AND attributes IS NOT NULL
            ORDER BY 1
        """).fetchall()
        return [row[0] for row in rows]

    def attributes(self) -> dict[str, dict[str, list[str]]]:
        """
        Return all discovered custom attributes grouped by type.

        Returns:
            A nested dictionary of the form:
                {
                    "event": {<event_type>: [<attr>, ...], ...},
                    "object": {<object_type>: [<attr>, ...], ...},
                }
        """
        return {
            "event": {
                event_type: self.event_attributes(event_type)
                for event_type in self.event_types()
            },
            "object": {
                object_type: self.object_attributes(object_type)
                for object_type in self.object_types()
            },
        }

    # -------------------------------------------------------------------------
    # Overview
    # -------------------------------------------------------------------------

    def overview(self) -> OCELOverview:
        """
        Return a high-level overview of the OCEL.

        Semantics:
            - `event_count` counts event rows, which are also logical events
            - `object_count` counts distinct logical objects via the query API
            - `object_record_count` counts physical history rows in `object`
        """
        event_count = self._ocel.events().count()
        object_count = self._ocel.objects().count()

        object_record_count = self._scalar(f"""
            SELECT COUNT(*)
            FROM {self._ocel.schema}.object
        """)

        event_type_count = len(self.event_types())
        object_type_count = len(self.object_types())

        event_object_count = self._scalar(f"""
            SELECT COUNT(*)
            FROM {self._ocel.schema}.event_object
        """)

        object_object_count = self._scalar(f"""
            SELECT COUNT(*)
            FROM {self._ocel.schema}.object_object
        """)

        return OCELOverview(
            event_count=event_count,
            object_count=object_count,
            object_record_count=object_record_count,
            event_type_count=event_type_count,
            object_type_count=object_type_count,
            event_object_count=event_object_count,
            object_object_count=object_object_count,
        )

    # -------------------------------------------------------------------------
    # Per-type counts
    # -------------------------------------------------------------------------

    def event_type_counts(self) -> dict[str, int]:
        """
        Return the number of events per event type.

        Uses the event query DSL for clarity and backend independence.
        """
        return {
            event_type: self._ocel.events(event_type).count()
            for event_type in self.event_types()
        }

    def object_type_counts(self) -> dict[str, int]:
        """
        Return the number of logical objects per object type.

        Uses the object query DSL, so counts are based on distinct `ocel_id`
        semantics rather than raw object-history rows.
        """
        return {
            object_type: self._ocel.objects(object_type).count()
            for object_type in self.object_types()
        }

    # -------------------------------------------------------------------------
    # Relation summaries
    # -------------------------------------------------------------------------

    def event_object_stats(self) -> EventObjectStats:
        """
        Return structural statistics for the event-object relation.

        Statistics include:
            - average / min / max number of objects per event
            - average / min / max number of events per logical object
        """
        row = self._ocel.sql(f"""
            WITH per_event AS (
                SELECT
                    ocel_event_id,
                    COUNT(*) AS object_count
                FROM {self._ocel.schema}.event_object
                GROUP BY ocel_event_id
            ),
            per_object AS (
                SELECT
                    ocel_object_id,
                    COUNT(*) AS event_count
                FROM {self._ocel.schema}.event_object
                GROUP BY ocel_object_id
            )
            SELECT
                (SELECT AVG(object_count)::DOUBLE FROM per_event) AS avg_objects_per_event,
                (SELECT MIN(object_count) FROM per_event) AS min_objects_per_event,
                (SELECT MAX(object_count) FROM per_event) AS max_objects_per_event,
                (SELECT AVG(event_count)::DOUBLE FROM per_object) AS avg_events_per_object,
                (SELECT MIN(event_count) FROM per_object) AS min_events_per_object,
                (SELECT MAX(event_count) FROM per_object) AS max_events_per_object
        """).fetchone()

        if row is None:
            raise RuntimeError("event_object_stats query returned no rows")

        return EventObjectStats(
            avg_objects_per_event=row[0],
            min_objects_per_event=row[1],
            max_objects_per_event=row[2],
            avg_events_per_object=row[3],
            min_events_per_object=row[4],
            max_events_per_object=row[5],
        )

    def objects_per_event_type(self) -> duckdb.DuckDBPyRelation:
        """
        Return the number of involved objects per event type.

        The result contains one row per event type with:
            - number of events
            - average number of linked objects per event
            - minimum number of linked objects per event
            - maximum number of linked objects per event
        """
        return self._ocel.sql(f"""
            WITH per_event AS (
                SELECT
                    e.ocel_type,
                    e.ocel_id,
                    COUNT(eo.ocel_object_id) AS object_count
                FROM {self._ocel.schema}.event e
                LEFT JOIN {self._ocel.schema}.event_object eo
                  ON e.ocel_id = eo.ocel_event_id
                GROUP BY e.ocel_type, e.ocel_id
            )
            SELECT
                ocel_type,
                COUNT(*) AS event_count,
                AVG(object_count)::DOUBLE AS avg_objects_per_event,
                MIN(object_count) AS min_objects_per_event,
                MAX(object_count) AS max_objects_per_event
            FROM per_event
            GROUP BY ocel_type
            ORDER BY event_count DESC, ocel_type
        """)

    def events_per_object_type(self) -> duckdb.DuckDBPyRelation:
        """
        Return the number of linked events per object type.

        The result contains one row per object type with:
            - number of distinct logical objects
            - average number of linked events per logical object
            - minimum number of linked events per logical object
            - maximum number of linked events per logical object

        Uses distinct logical objects, not raw object-history row counts.
        """
        return self._ocel.sql(f"""
            WITH typed_objects AS (
                SELECT DISTINCT
                    ocel_id,
                    ocel_type
                FROM {self._ocel.schema}.object
            ),
            per_object AS (
                SELECT
                    o.ocel_type,
                    o.ocel_id,
                    COUNT(eo.ocel_event_id) AS event_count
                FROM typed_objects o
                LEFT JOIN {self._ocel.schema}.event_object eo
                  ON o.ocel_id = eo.ocel_object_id
                GROUP BY o.ocel_type, o.ocel_id
            )
            SELECT
                ocel_type,
                COUNT(*) AS object_count,
                AVG(event_count)::DOUBLE AS avg_events_per_object,
                MIN(event_count) AS min_events_per_object,
                MAX(event_count) AS max_events_per_object
            FROM per_object
            GROUP BY ocel_type
            ORDER BY object_count DESC, ocel_type
        """)

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _scalar(self, query: str) -> int:
        """
        Execute a scalar integer SQL query against the OCEL.
        """
        row = self._ocel.sql(query).fetchone()
        if row is None:
            raise RuntimeError("Scalar inspection query returned no rows")
        return int(row[0])
