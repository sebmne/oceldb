from uuid import uuid4

from oceldb.core.ocel import OCEL
from oceldb.views.query.view_query import ViewQuery


def materialize_view_query(query: ViewQuery) -> OCEL:
    target_schema = f"ocel_{uuid4().hex[:8]}"
    con = query.ocel._con

    con.execute(f"CREATE SCHEMA {target_schema}")
    try:
        _materialize_root(query, target_schema)
    except Exception:
        con.execute(f"DROP SCHEMA {target_schema} CASCADE")
        raise

    return OCEL(
        path=query.ocel.path,
        con=con,
        metadata=query.ocel.metadata,
        schema=target_schema,
        owns_connection=False,
    )


def _materialize_root(query: ViewQuery, target_schema: str) -> None:
    if query.root_kind == "event":
        _materialize_event_view(query, target_schema)
    elif query.root_kind == "object":
        _materialize_object_view(query, target_schema)
    else:
        raise TypeError(f"Unsupported root kind: {query.root_kind!r}")


def _materialize_event_view(query: ViewQuery, target_schema: str) -> None:
    source_schema = query.ocel.schema
    con = query.ocel._con

    con.execute(f"""
        CREATE VIEW {target_schema}._root AS
        SELECT DISTINCT ocel_id
        FROM ({query.to_sql()}) q
    """)

    con.execute(f"""
        CREATE VIEW {target_schema}.event AS
        SELECT DISTINCT e.*
        FROM {source_schema}.event e
        JOIN {target_schema}._root r
          ON e.ocel_id = r.ocel_id
    """)

    _materialize_common_views(query, target_schema)


def _materialize_object_view(query: ViewQuery, target_schema: str) -> None:
    source_schema = query.ocel.schema
    con = query.ocel._con

    con.execute(f"""
        CREATE VIEW {target_schema}._root AS
        SELECT DISTINCT ocel_id
        FROM ({query.to_sql()}) q
    """)

    con.execute(f"""
        CREATE VIEW {target_schema}.event AS
        SELECT DISTINCT e.*
        FROM {source_schema}.event e
        JOIN {source_schema}.event_object eo
          ON e.ocel_id = eo.ocel_event_id
        JOIN {target_schema}._root r
          ON eo.ocel_object_id = r.ocel_id
    """)

    _materialize_common_views(query, target_schema)


def _materialize_common_views(query: ViewQuery, target_schema: str) -> None:
    source_schema = query.ocel.schema
    con = query.ocel._con

    con.execute(f"""
        CREATE VIEW {target_schema}.event_object AS
        SELECT DISTINCT eo.*
        FROM {source_schema}.event_object eo
        JOIN {target_schema}.event e
          ON eo.ocel_event_id = e.ocel_id
    """)

    con.execute(f"""
        CREATE VIEW {target_schema}.object AS
        SELECT DISTINCT o.*
        FROM {source_schema}.object o
        JOIN {target_schema}.event_object eo
          ON o.ocel_id = eo.ocel_object_id
    """)

    con.execute(f"""
        CREATE VIEW {target_schema}.object_object AS
        SELECT DISTINCT oo.*
        FROM {source_schema}.object_object oo
        JOIN {target_schema}.object os
          ON oo.ocel_source_id = os.ocel_id
        JOIN {target_schema}.object ot
          ON oo.ocel_target_id = ot.ocel_id
    """)
