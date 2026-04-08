from dataclasses import dataclass

from oceldb.core.ocel import OCEL
from oceldb.tables.query.table_query import AnalysisTableKind


@dataclass(frozen=True)
class RenderedSource:
    """
    SQL source fragment used by the analysis compiler.
    """

    from_sql: str
    alias: str
    kind: AnalysisTableKind


def render_analysis_source(
    ocel: OCEL,
    table_kind: AnalysisTableKind,
) -> RenderedSource:
    schema = ocel.schema

    match table_kind:
        case "event":
            return RenderedSource(
                from_sql=f"{schema}.event ev",
                alias="ev",
                kind="event",
            )

        case "object":
            return RenderedSource(
                from_sql=f"{schema}.object ob",
                alias="ob",
                kind="object",
            )

        case "event_object":
            return RenderedSource(
                from_sql=f"{schema}.event_object eo",
                alias="eo",
                kind="event_object",
            )

        case "object_object":
            return RenderedSource(
                from_sql=f"{schema}.object_object oo",
                alias="oo",
                kind="object_object",
            )

        case _:
            raise TypeError(f"Unsupported analysis table kind: {table_kind!r}")
