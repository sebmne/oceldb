from __future__ import annotations

from datetime import date, datetime

from oceldb.ast.base import AggregateExpr, AliasExpr, Expr
from oceldb.query.mixins import (
    ExecutionMixin,
    GroupByMixin,
    HavingMixin,
    LimitMixin,
    MaterializeMixin,
    SelectMixin,
    SortMixin,
    UniqueMixin,
    WhereMixin,
    WithColumnsMixin,
    coerce_aggregate_exprs,
    coerce_aggregate_named_exprs,
)
from oceldb.query.plan import GroupPlan, ObjectStateSource, QueryPlan, root_source


class RowsQuery(
    GroupByMixin,
    WithColumnsMixin,
    SelectMixin,
    LimitMixin,
    UniqueMixin,
    SortMixin,
    WhereMixin,
    ExecutionMixin,
):
    pass


class EventRows(MaterializeMixin, RowsQuery):
    pass


class ObjectRows(MaterializeMixin, RowsQuery):
    pass


class ObjectChangeRows(RowsQuery):
    pass


class ObjectStateRows(MaterializeMixin, RowsQuery):
    pass


class EventObjectRows(RowsQuery):
    pass


class ObjectObjectRows(RowsQuery):
    pass


class SelectedRows(
    LimitMixin,
    UniqueMixin,
    SortMixin,
    WhereMixin,
    ExecutionMixin,
):
    pass


class AggregatedRows(
    SelectMixin,
    LimitMixin,
    SortMixin,
    HavingMixin,
    ExecutionMixin,
):
    pass


class GroupedRows:
    def __init__(self, plan: QueryPlan, groupings: tuple[Expr, ...]) -> None:
        self._plan = plan
        self._groupings = groupings

    def agg(
        self,
        *exprs: AggregateExpr | AliasExpr,
        **named_exprs: AggregateExpr,
    ) -> AggregatedRows:
        aggregations = coerce_aggregate_exprs(
            exprs, context="agg(...)"
        ) + coerce_aggregate_named_exprs(
            (),
            named_exprs,
            context="agg(...)",
        )
        if not aggregations:
            raise ValueError("agg(...) requires at least one expression")
        return AggregatedRows(
            QueryPlan(
                ocel=self._plan.ocel,
                node=GroupPlan(self._plan.node, self._groupings, aggregations),
            )
        )


class ObjectStateSeed:
    def __init__(self, plan: QueryPlan) -> None:
        self._plan = plan

    def latest(self) -> ObjectStateRows:
        source = root_source(self._plan.node)
        if not isinstance(source, ObjectStateSource):
            raise TypeError("latest() is only valid for object_states(...) roots")
        return ObjectStateRows(
            self._plan.with_root_source(
                ObjectStateSource(
                    selected_types=source.selected_types,
                    mode="latest",
                )
            )
        )

    def as_of(self, value: date | datetime | str) -> ObjectStateRows:
        source = root_source(self._plan.node)
        if not isinstance(source, ObjectStateSource):
            raise TypeError("as_of(...) is only valid for object_states(...) roots")
        return ObjectStateRows(
            self._plan.with_root_source(
                ObjectStateSource(
                    selected_types=source.selected_types,
                    mode="as_of",
                    as_of=value,
                )
            )
        )
