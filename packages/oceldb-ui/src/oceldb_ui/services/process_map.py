from __future__ import annotations

from oceldb import OCEL

ALL_TYPES = "__all__"


def _object_filter(schema: str, object_type: str) -> str:
    """Return a WHERE clause fragment filtering by object type, or empty for all."""
    if object_type == ALL_TYPES:
        return ""
    return f"AND o.ocel_type = '{object_type}'"


def compute_dfg(ocel: OCEL, object_type: str) -> dict:
    schema = ocel.schema
    type_filter = _object_filter(schema, object_type)

    # Activity frequencies
    activity_sql = f"""
        SELECT e.ocel_type AS activity, COUNT(*) AS frequency
        FROM {schema}.event_object eo
        JOIN {schema}.event e ON eo.ocel_event_id = e.ocel_id
        JOIN {schema}.object o ON eo.ocel_object_id = o.ocel_id
        WHERE 1=1 {type_filter}
        GROUP BY e.ocel_type
        ORDER BY frequency DESC
    """
    act_rel = ocel.sql(activity_sql)
    activities = {row[0]: row[1] for row in act_rel.fetchall()}

    # Directly-follows edges
    dfg_sql = f"""
        WITH object_events AS (
            SELECT
                eo.ocel_object_id,
                e.ocel_type AS activity,
                e.ocel_time
            FROM {schema}.event_object eo
            JOIN {schema}.event e ON eo.ocel_event_id = e.ocel_id
            JOIN {schema}.object o ON eo.ocel_object_id = o.ocel_id
            WHERE 1=1 {type_filter}
        ),
        with_next AS (
            SELECT
                activity,
                LEAD(activity) OVER (
                    PARTITION BY ocel_object_id ORDER BY ocel_time
                ) AS next_activity
            FROM object_events
        )
        SELECT activity AS source, next_activity AS target, COUNT(*) AS frequency
        FROM with_next
        WHERE next_activity IS NOT NULL
        GROUP BY activity, next_activity
        ORDER BY frequency DESC
    """
    edge_rel = ocel.sql(dfg_sql)
    edges = [
        {"source": row[0], "target": row[1], "frequency": row[2]}
        for row in edge_rel.fetchall()
    ]

    # Start / end activities
    start_end_sql = f"""
        WITH object_events AS (
            SELECT
                eo.ocel_object_id,
                e.ocel_type AS activity,
                e.ocel_time,
                ROW_NUMBER() OVER (
                    PARTITION BY eo.ocel_object_id ORDER BY e.ocel_time ASC
                ) AS rn_asc,
                ROW_NUMBER() OVER (
                    PARTITION BY eo.ocel_object_id ORDER BY e.ocel_time DESC
                ) AS rn_desc
            FROM {schema}.event_object eo
            JOIN {schema}.event e ON eo.ocel_event_id = e.ocel_id
            JOIN {schema}.object o ON eo.ocel_object_id = o.ocel_id
            WHERE 1=1 {type_filter}
        )
        SELECT
            activity,
            SUM(CASE WHEN rn_asc = 1 THEN 1 ELSE 0 END) AS start_count,
            SUM(CASE WHEN rn_desc = 1 THEN 1 ELSE 0 END) AS end_count
        FROM object_events
        WHERE rn_asc = 1 OR rn_desc = 1
        GROUP BY activity
    """
    se_rel = ocel.sql(start_end_sql)
    start_counts: dict[str, int] = {}
    end_counts: dict[str, int] = {}
    for row in se_rel.fetchall():
        if row[1] > 0:
            start_counts[row[0]] = row[1]
        if row[2] > 0:
            end_counts[row[0]] = row[2]

    nodes = [
        {
            "id": name,
            "label": name,
            "frequency": freq,
            "is_start": name in start_counts,
            "is_end": name in end_counts,
            "start_count": start_counts.get(name, 0),
            "end_count": end_counts.get(name, 0),
        }
        for name, freq in activities.items()
    ]

    return {
        "nodes": nodes,
        "edges": edges,
    }


def compute_variants(ocel: OCEL, object_type: str) -> list[dict]:
    schema = ocel.schema
    type_filter = _object_filter(schema, object_type)

    variants_sql = f"""
        WITH object_events AS (
            SELECT
                eo.ocel_object_id,
                e.ocel_type AS activity,
                e.ocel_time
            FROM {schema}.event_object eo
            JOIN {schema}.event e ON eo.ocel_event_id = e.ocel_id
            JOIN {schema}.object o ON eo.ocel_object_id = o.ocel_id
            WHERE 1=1 {type_filter}
        ),
        ordered AS (
            SELECT
                ocel_object_id,
                LIST(activity ORDER BY ocel_time) AS variant
            FROM object_events
            GROUP BY ocel_object_id
        )
        SELECT variant, COUNT(*) AS frequency
        FROM ordered
        GROUP BY variant
        ORDER BY frequency DESC
    """
    rel = ocel.sql(variants_sql)
    rows = rel.fetchall()

    total = sum(r[1] for r in rows)

    variants = []
    for i, row in enumerate(rows):
        activities = list(row[0]) if row[0] else []
        freq = row[1]
        variants.append({
            "id": i,
            "activities": activities,
            "frequency": freq,
            "percentage": round(freq / total * 100, 1) if total > 0 else 0,
        })

    return variants
