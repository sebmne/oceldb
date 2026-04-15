"""Bridge helpers for converting oceldb datasets to PM4Py OCEL objects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from oceldb.core.ocel import OCEL

if TYPE_CHECKING:
    import pm4py


def to_pm4py(ocel: OCEL) -> "pm4py.objects.ocel.obj.OCEL":
    """
    Convert an `oceldb.OCEL` handle into a PM4Py `OCEL` object.

    This bridge is optional and requires the `oceldb[pm4py]` extra.
    The resulting PM4Py object uses PM4Py's default OCEL column names.
    """
    try:
        import pandas as pd
        import pm4py
    except ImportError as exc:
        raise ImportError(
            "PM4Py interoperability requires optional dependencies. "
            "Install them with `pip install oceldb[pm4py]`."
        ) from exc

    return pm4py.objects.ocel.obj.OCEL(
            events=_query_dataframe(
                ocel,
                """
            SELECT
                e."ocel_id" AS "ocel:eid",
                e."ocel_type" AS "ocel:activity",
                e."ocel_time" AS "ocel:timestamp",
                e.*
            EXCLUDE ("ocel_id", "ocel_type", "ocel_time")
            FROM "event" e
            ORDER BY e."ocel_time", e."ocel_id"
            """,
                pandas_module=pd,
            ),
            objects=_query_dataframe(
                ocel,
                """
            WITH object_history AS (
                SELECT
                    c.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY c."ocel_id"
                        ORDER BY
                            CASE WHEN c."ocel_changed_field" IS NULL THEN 0 ELSE 1 END,
                            c."ocel_time"
                    ) AS "_oceldb_initial_rank"
                FROM "object_change" c
            )
            SELECT
                o."ocel_id" AS "ocel:oid",
                o."ocel_type" AS "ocel:type",
                h.*
            EXCLUDE ("ocel_id", "ocel_type", "ocel_time", "ocel_changed_field", "_oceldb_initial_rank")
            FROM "object" o
            LEFT JOIN object_history h
              ON h."ocel_id" = o."ocel_id"
             AND h."_oceldb_initial_rank" = 1
            ORDER BY o."ocel_id"
            """,
                pandas_module=pd,
            ),
            relations=_query_dataframe(
                ocel,
                """
            SELECT
                e."ocel_id" AS "ocel:eid",
                e."ocel_type" AS "ocel:activity",
                e."ocel_time" AS "ocel:timestamp",
                o."ocel_id" AS "ocel:oid",
                o."ocel_type" AS "ocel:type",
                eo."ocel_qualifier" AS "ocel:qualifier"
            FROM "event_object" eo
            JOIN "event" e
              ON eo."ocel_event_id" = e."ocel_id"
            JOIN "object" o
              ON eo."ocel_object_id" = o."ocel_id"
            ORDER BY e."ocel_time", e."ocel_id", o."ocel_id"
            """,
                pandas_module=pd,
            ),
            o2o=_query_dataframe(
                ocel,
                """
            SELECT
                oo."ocel_source_id" AS "ocel:oid",
                oo."ocel_target_id" AS "ocel:oid_2",
                oo."ocel_qualifier" AS "ocel:qualifier"
            FROM "object_object" oo
            ORDER BY oo."ocel_source_id", oo."ocel_target_id"
            """,
                pandas_module=pd,
            ),
            object_changes=_query_dataframe(
                ocel,
                """
            WITH object_history AS (
                SELECT
                    c.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY c."ocel_id"
                        ORDER BY
                            CASE WHEN c."ocel_changed_field" IS NULL THEN 0 ELSE 1 END,
                            c."ocel_time"
                    ) AS "_oceldb_initial_rank"
                FROM "object_change" c
            )
            SELECT
                c."ocel_id" AS "ocel:oid",
                c."ocel_type" AS "ocel:type",
                c."ocel_time" AS "ocel:timestamp",
                c."ocel_changed_field" AS "ocel:field",
                c.*
            EXCLUDE ("ocel_id", "ocel_type", "ocel_time", "ocel_changed_field", "_oceldb_initial_rank")
            FROM object_history c
            WHERE c."_oceldb_initial_rank" > 1
            ORDER BY c."ocel_id", c."ocel_time"
            """,
                pandas_module=pd,
            ),
            globals={
                "oceldb:path": str(ocel.path),
                "oceldb:source": ocel.manifest.source,
                "oceldb:storage_version": ocel.manifest.storage_version,
            },
    )


def _query_dataframe(
    ocel: OCEL,
    sql: str,
    *,
    pandas_module: Any,
) -> Any:
    relation = ocel.sql(sql)
    return pandas_module.DataFrame(relation.fetchall(), columns=relation.columns)
