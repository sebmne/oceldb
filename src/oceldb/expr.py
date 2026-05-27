"""Typed wrappers around ibis expressions."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, cast

import ibis
import ibis.expr.types as ir
import pandas as pd
import pyarrow as pa

JoinKind = Literal["inner", "left", "right", "outer", "asof", "semi", "anti"]


def _unwrap(x: object) -> Any:
    """Return the underlying ibis expression for a Column, else the value as-is."""
    if isinstance(x, Column):
        return x.raw()
    if isinstance(x, Table):
        return x.raw()
    return x


class Column:
    """Typed wrapper around an ibis column / deferred / scalar expression."""

    __slots__ = ("_expr",)

    def __init__(self, expr: Any) -> None:
        self._expr: Any = expr

    def raw(self) -> Any:
        return self._expr

    def name(self, alias: str) -> "Column":
        return Column(self._expr.name(alias))

    def __eq__(self, other: object) -> "Predicate":  # type: ignore[override]
        return Predicate(self._expr == _unwrap(other))

    def __ne__(self, other: object) -> "Predicate":  # type: ignore[override]
        return Predicate(self._expr != _unwrap(other))

    def __lt__(self, other: object) -> "Predicate":
        return Predicate(self._expr < _unwrap(other))

    def __le__(self, other: object) -> "Predicate":
        return Predicate(self._expr <= _unwrap(other))

    def __gt__(self, other: object) -> "Predicate":
        return Predicate(self._expr > _unwrap(other))

    def __ge__(self, other: object) -> "Predicate":
        return Predicate(self._expr >= _unwrap(other))

    def __hash__(self) -> int:
        return id(self)

    def isin(self, values: "Column | list[object]") -> "Predicate":
        return Predicate(self._expr.isin(_unwrap(values)))

    def notin(self, values: "Column | list[object]") -> "Predicate":
        return Predicate(self._expr.notin(_unwrap(values)))

    def isnull(self) -> "Predicate":
        return Predicate(self._expr.isnull())

    def notnull(self) -> "Predicate":
        return Predicate(self._expr.notnull())

    def count(self) -> "Column":
        return Column(self._expr.count())

    def sum(self) -> "Column":
        return Column(self._expr.sum())

    def mean(self) -> "Column":
        return Column(self._expr.mean())

    def min(self) -> "Column":
        return Column(self._expr.min())

    def max(self) -> "Column":
        return Column(self._expr.max())

    def nunique(self) -> "Column":
        return Column(self._expr.nunique())

    def lag(self, offset: int = 1, default: object = None) -> "Column":
        return Column(self._expr.lag(offset, _unwrap(default)))

    def lead(self, offset: int = 1, default: object = None) -> "Column":
        return Column(self._expr.lead(offset, _unwrap(default)))

    def over(
        self,
        *,
        group_by: "str | Column | Sequence[str | Column] | None" = None,
        order_by: "str | Column | Sequence[str | Column] | None" = None,
    ) -> "Column":
        def norm(
            x: "str | Column | Sequence[str | Column] | None",
        ) -> Any:
            if x is None:
                return None
            if isinstance(x, (str, Column)):
                return _unwrap(x)
            return [_unwrap(i) for i in x]

        return Column(self._expr.over(group_by=norm(group_by), order_by=norm(order_by)))

    def execute(self) -> Any:
        return self._expr.execute()


class Predicate(Column):
    """A :class:`Column` resolving to boolean; accepted by :meth:`Table.filter`."""

    __slots__ = ()

    def __invert__(self) -> "Predicate":
        return Predicate(~self._expr)

    def __and__(self, other: "Predicate") -> "Predicate":
        return Predicate(self._expr & other.raw())

    def __or__(self, other: "Predicate") -> "Predicate":
        return Predicate(self._expr | other.raw())

    def __rand__(self, other: "Predicate") -> "Predicate":
        return Predicate(other.raw() & self._expr)

    def __ror__(self, other: "Predicate") -> "Predicate":
        return Predicate(other.raw() | self._expr)

    def __hash__(self) -> int:
        return id(self)


class GroupedTable:
    """Result of :meth:`Table.group_by`. Only supports :meth:`aggregate`."""

    __slots__ = ("_expr",)

    def __init__(self, expr: Any) -> None:
        self._expr: Any = expr

    def aggregate(self, **aggs: "Column | Any") -> "Table":
        kwargs = {k: _unwrap(v) for k, v in aggs.items()}
        return Table(self._expr.aggregate(**kwargs))


class Table:
    """Typed wrapper around an ibis :class:`~ibis.expr.types.Table`."""

    __slots__ = ("_expr",)

    def __init__(self, expr: ir.Table) -> None:
        self._expr: ir.Table = expr

    def raw(self) -> ir.Table:
        return self._expr

    def __getitem__(self, name: str) -> Column:
        return Column(self._expr[name])  # pyright: ignore[reportUnknownArgumentType]

    def __getattr__(self, name: str) -> Column:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return Column(self._expr[name])  # pyright: ignore[reportUnknownArgumentType]
        except Exception as exc:
            raise AttributeError(name) from exc

    @property
    def columns(self) -> list[str]:
        return list(self._expr.columns)

    def count(self) -> Column:
        return Column(self._expr.count())

    def filter(self, *predicates: "Predicate | Any") -> "Table":
        exprs = [_unwrap(p) for p in predicates]
        return Table(self._expr.filter(*exprs))  # pyright: ignore[reportUnknownArgumentType]

    def select(self, *cols: "str | Column", **aliased: "Column | Any") -> "Table":
        args = [_unwrap(c) for c in cols]
        kwargs = {k: _unwrap(v) for k, v in aliased.items()}
        return Table(self._expr.select(*args, **kwargs))  # pyright: ignore[reportUnknownArgumentType]

    def mutate(self, **cols: "Column | Any") -> "Table":
        kwargs = {k: _unwrap(v) for k, v in cols.items()}
        return Table(self._expr.mutate(**kwargs))  # pyright: ignore[reportUnknownArgumentType]

    def drop(self, *cols: str) -> "Table":
        return Table(self._expr.drop(*cols))

    def rename(self, **mapping: str) -> "Table":
        return Table(self._expr.rename(**mapping))

    def distinct(self) -> "Table":
        return Table(self._expr.distinct())

    def limit(self, n: int) -> "Table":
        return Table(self._expr.limit(n))

    def order_by(self, *cols: "str | Column") -> "Table":
        args = [_unwrap(c) for c in cols]
        return Table(self._expr.order_by(*args))  # pyright: ignore[reportUnknownArgumentType]

    def group_by(self, *cols: "str | Column") -> GroupedTable:
        args = [_unwrap(c) for c in cols]
        return GroupedTable(self._expr.group_by(*args))

    def join(
        self,
        other: "Table",
        predicates: "str | Sequence[str] | Predicate | Sequence[Predicate] | Any",
        *,
        how: JoinKind = "inner",
    ) -> "Table":
        preds: Any
        if isinstance(predicates, (str, Column)):
            preds = _unwrap(predicates)
        elif isinstance(predicates, Sequence):
            preds = [_unwrap(p) for p in cast(Sequence[Any], predicates)]
        else:
            preds = predicates
        return Table(self._expr.join(other.raw(), preds, how=how))

    def execute(self) -> pd.DataFrame:
        return cast(pd.DataFrame, self._expr.execute())

    def to_pyarrow(self) -> pa.Table:
        return self._expr.to_pyarrow()


def col(name: str) -> Column:
    """Return a deferred column reference."""
    return Column(ibis._[name])


def row_number() -> Column:
    return Column(ibis.row_number())


def desc(c: "str | Column") -> Column:
    return Column(ibis.desc(_unwrap(c)))


def asc(c: "str | Column") -> Column:
    return Column(ibis.asc(_unwrap(c)))


def union(*tables: Table) -> Table:
    raws = [t.raw() for t in tables]
    return Table(ibis.union(*raws))


__all__ = [
    "Column",
    "GroupedTable",
    "JoinKind",
    "Predicate",
    "Table",
    "asc",
    "col",
    "desc",
    "row_number",
    "union",
]
