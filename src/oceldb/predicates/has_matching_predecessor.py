from oceldb.expr import Predicate, Table, col
from oceldb.ocel import OCEL


def has_matching_predecessor(
    ocel: OCEL,
    preceding_event_type: str,
    object_type: str,
) -> Predicate:
    """Match events with an earlier event over the same object set."""
    t = object_type.replace("'", "''")
    p = preceding_event_type.replace("'", "''")
    sql = f"""
        WITH
        eo_typed AS (
            SELECT ocel_event_id, ocel_object_id
            FROM event_object
            WHERE ocel_object_type = '{t}'
        ),
        sizes AS (
            SELECT ocel_event_id, COUNT(*) AS n
            FROM eo_typed
            GROUP BY ocel_event_id
        ),
        matched AS (
            SELECT DISTINCT curr.ocel_event_id AS ocel_id
            FROM eo_typed curr
            JOIN events        curr_e  ON curr_e.ocel_id      = curr.ocel_event_id
            JOIN eo_typed      prev    ON prev.ocel_object_id = curr.ocel_object_id
            JOIN events        prev_e  ON prev_e.ocel_id      = prev.ocel_event_id
                                      AND prev_e.ocel_type    = '{p}'
                                      AND prev_e.ocel_time    < curr_e.ocel_time
            JOIN sizes         sz_curr ON sz_curr.ocel_event_id = curr.ocel_event_id
            JOIN sizes         sz_prev ON sz_prev.ocel_event_id = prev.ocel_event_id
                                      AND sz_prev.n            = sz_curr.n
            GROUP BY curr.ocel_event_id, prev.ocel_event_id, sz_curr.n
            HAVING COUNT(*) = sz_curr.n
        )
        SELECT ocel_id FROM matched
    """
    matched_ids = Table(ocel.con.sql(sql))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
    return col("ocel_id").isin(matched_ids["ocel_id"])
