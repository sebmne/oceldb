from oceldb.sql.render_expr import render_bool_expr

from oceldb.sql.context import CompileContext
from oceldb.sublog.query.view_query import ViewQuery


def render_view_query(query: ViewQuery) -> str:
    return f"""
        SELECT {query.root_alias}.*
        FROM {query.ocel.schema}.{query.root_table_name} {query.root_alias}
        WHERE {render_view_where_clause(query)}
    """


def render_view_where_clause(query: ViewQuery) -> str:
    ctx = CompileContext(
        alias=query.root_alias,
        schema=query.ocel.schema,
        kind=query.root_kind,
    )

    predicates: list[str] = []

    type_filter = query.type_filter_expr()
    if type_filter is not None:
        predicates.append(render_bool_expr(type_filter, ctx))

    for expr in query.filters:
        predicates.append(render_bool_expr(expr, ctx))

    return " AND ".join(predicates) if predicates else "TRUE"
