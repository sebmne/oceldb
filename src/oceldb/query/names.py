from __future__ import annotations

from oceldb.ast.aggregation import AvgAgg, CountAgg, CountDistinctAgg, MaxAgg, MinAgg, SumAgg
from oceldb.ast.base import AliasExpr, CastExpr, Expr
from oceldb.ast.field import ColumnExpr
from oceldb.ast.relation import RelationCountExpr


def output_name(expr: Expr) -> str | None:
    match expr:
        case AliasExpr(name=name):
            return name
        case ColumnExpr(name=name):
            return name
        case CastExpr(expr=inner):
            return output_name(inner)
        case CountAgg():
            return "count"
        case CountDistinctAgg(expr=inner):
            inner_name = output_name(inner)
            return "count_distinct" if inner_name is None else f"count_distinct_{inner_name}"
        case MinAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"min_{inner_name}"
        case MaxAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"max_{inner_name}"
        case SumAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"sum_{inner_name}"
        case AvgAgg(expr=inner):
            inner_name = output_name(inner)
            return None if inner_name is None else f"avg_{inner_name}"
        case RelationCountExpr(spec=spec):
            return f"{spec.kind}_count"
        case _:
            return None
