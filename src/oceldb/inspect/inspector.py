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


@dataclass(frozen=True)
class ObjectHistoryStats:
    """
    Statistics describing the history-preserving object table.
    """

    object_record_count: int
    logical_object_count: int
    avg_records_per_object: Optional[float]
    min_records_per_object: Optional[int]
    max_records_per_object: Optional[int]
    objects_with_history_count: int
    objects_with_changed_field_count: int
    object_records_with_time_count: int
    object_records_with_changed_field_count: int


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

    def _scalar(self, query: str) -> int:
        """
        Execute a scalar integer SQL query against the OCEL.
        """
        row = self._ocel.sql(query).fetchone()
        if row is None:
            raise RuntimeError("Scalar inspection query returned no rows")
        return int(row[0])

    def object_history_stats(self) -> ObjectHistoryStats:
        """
        Return statistics describing the physical object-history representation.

        Semantics:
            - `object_record_count` counts physical rows in `object`
            - `logical_object_count` counts distinct `ocel_id`
            - records-per-object stats describe how many history/state rows each
            logical object has
            - `objects_with_history_count` counts logical objects with more than one
            physical record
            - `objects_with_changed_field_count` counts logical objects for which at
            least one record has a non-null `ocel_changed_field`
            - `object_records_with_time_count` counts physical rows with non-null
            `ocel_time`
            - `object_records_with_changed_field_count` counts physical rows with
            non-null `ocel_changed_field`
        """
        row = self._ocel.sql(f"""
            WITH per_object AS (
                SELECT
                    ocel_id,
                    COUNT(*) AS record_count,
                    MAX(CASE WHEN ocel_changed_field IS NOT NULL THEN 1 ELSE 0 END) AS has_changed_field
                FROM {self._ocel.schema}.object
                GROUP BY ocel_id
            )
            SELECT
                (SELECT COUNT(*) FROM {self._ocel.schema}.object) AS object_record_count,
                (SELECT COUNT(DISTINCT ocel_id) FROM {self._ocel.schema}.object) AS logical_object_count,
                (SELECT AVG(record_count)::DOUBLE FROM per_object) AS avg_records_per_object,
                (SELECT MIN(record_count) FROM per_object) AS min_records_per_object,
                (SELECT MAX(record_count) FROM per_object) AS max_records_per_object,
                (SELECT COUNT(*) FROM per_object WHERE record_count > 1) AS objects_with_history_count,
                (SELECT COUNT(*) FROM per_object WHERE has_changed_field = 1) AS objects_with_changed_field_count,
                (SELECT COUNT(*) FROM {self._ocel.schema}.object WHERE ocel_time IS NOT NULL) AS object_records_with_time_count,
                (SELECT COUNT(*) FROM {self._ocel.schema}.object WHERE ocel_changed_field IS NOT NULL) AS object_records_with_changed_field_count
        """).fetchone()

        if row is None:
            raise RuntimeError("object_history_stats query returned no rows")

        return ObjectHistoryStats(
            object_record_count=row[0],
            logical_object_count=row[1],
            avg_records_per_object=row[2],
            min_records_per_object=row[3],
            max_records_per_object=row[4],
            objects_with_history_count=row[5],
            objects_with_changed_field_count=row[6],
            object_records_with_time_count=row[7],
            object_records_with_changed_field_count=row[8],
        )

    def activity_object_type_matrix(self) -> duckdb.DuckDBPyRelation:
        """
        Return the event type × object type link-count matrix.

        Each row describes how many event-object links connect an event type to an
        object type.

        Columns:
            - event_type
            - object_type
            - link_count

        Semantics:
            - counts raw rows in `event_object`
            - if one event is linked to multiple objects of the same object type,
                all such links are counted
            - object-history rows are collapsed to distinct logical objects by
                deriving object types from distinct `(ocel_id, ocel_type)` pairs
        """
        return self._ocel.sql(f"""
            WITH typed_objects AS (
                SELECT DISTINCT
                    ocel_id,
                    ocel_type
                FROM {self._ocel.schema}.object
            )
            SELECT
                e.ocel_type AS event_type,
                o.ocel_type AS object_type,
                COUNT(*) AS link_count
            FROM {self._ocel.schema}.event_object eo
            JOIN {self._ocel.schema}.event e
                ON eo.ocel_event_id = e.ocel_id
            JOIN typed_objects o
                ON eo.ocel_object_id = o.ocel_id
            GROUP BY e.ocel_type, o.ocel_type
            ORDER BY event_type, object_type
        """)

    def activity_object_type_event_matrix(self) -> duckdb.DuckDBPyRelation:
        """
        Return the event type × object type event-incidence matrix.

        Each row describes how many distinct events of a given event type involve at
        least one object of a given object type.

        Columns:
            - event_type
            - object_type
            - event_count

        Semantics:
            - counts distinct events, not raw event-object links
            - if one event involves multiple objects of the same object type, the
                event is counted only once for that `(event_type, object_type)` pair
            - object-history rows are collapsed to distinct logical objects by
                deriving object types from distinct `(ocel_id, ocel_type)` pairs
        """
        return self._ocel.sql(f"""
            WITH typed_objects AS (
                SELECT DISTINCT
                    ocel_id,
                    ocel_type
                FROM {self._ocel.schema}.object
            ),
            event_type_object_type_pairs AS (
                SELECT DISTINCT
                    e.ocel_id,
                    e.ocel_type AS event_type,
                    o.ocel_type AS object_type
                FROM {self._ocel.schema}.event_object eo
                JOIN {self._ocel.schema}.event e
                    ON eo.ocel_event_id = e.ocel_id
                JOIN typed_objects o
                    ON eo.ocel_object_id = o.ocel_id
            )
            SELECT
                event_type,
                object_type,
                COUNT(*) AS event_count
            FROM event_type_object_type_pairs
            GROUP BY event_type, object_type
            ORDER BY event_type, object_type
        """)

    def activity_object_type_matrix_pivot(self) -> duckdb.DuckDBPyRelation:
        """
        Return the event type × object type link-count matrix in pivoted form.

        Rows are event types and columns are object types.
        Cell values are raw event-object link counts.

        Because object types are dynamic, the SQL is generated from the currently
        observed object types.
        """
        object_types = self.object_types()

        if not object_types:
            return self._ocel.sql("""
                SELECT CAST(NULL AS VARCHAR) AS event_type
                WHERE FALSE
            """)

        typed_objects_cte = f"""
            WITH typed_objects AS (
                SELECT DISTINCT
                    ocel_id,
                    ocel_type
                FROM {self._ocel.schema}.object
            ),
            base AS (
                SELECT
                    e.ocel_type AS event_type,
                    o.ocel_type AS object_type,
                    COUNT(*) AS link_count
                FROM {self._ocel.schema}.event_object eo
                JOIN {self._ocel.schema}.event e
                ON eo.ocel_event_id = e.ocel_id
                JOIN typed_objects o
                ON eo.ocel_object_id = o.ocel_id
                GROUP BY e.ocel_type, o.ocel_type
            )
        """

        columns = []
        for object_type in object_types:
            escaped = object_type.replace("'", "''")
            safe_alias = object_type.replace('"', '""')
            columns.append(
                f"""COALESCE(SUM(CASE WHEN object_type = '{escaped}' THEN link_count END), 0) AS "{safe_alias}" """
            )

        sql = f"""
            {typed_objects_cte}
            SELECT
                event_type,
                {", ".join(columns)}
            FROM base
            GROUP BY event_type
            ORDER BY event_type
        """

        return self._ocel.sql(sql)
