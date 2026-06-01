"""Converter contracts and the Arrow batch adapter."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import uuid4

import pyarrow as pa

if TYPE_CHECKING:
    import duckdb


_DUCKDB_TO_ARROW: dict[str, pa.DataType] = {
    "VARCHAR": pa.string(),
    "TIMESTAMP": pa.timestamp("us"),
    "INTEGER": pa.int64(),
    "DOUBLE": pa.float64(),
    "BOOLEAN": pa.bool_(),
}

_OCEL_TO_DUCKDB = {
    "string": "VARCHAR",
    "time": "TIMESTAMP",
    "integer": "INTEGER",
    "float": "DOUBLE",
    "boolean": "BOOLEAN",
}


def duckdb_to_arrow(duckdb_type: str) -> pa.DataType:
    """Translate a canonical DuckDB type string to a pyarrow type."""
    try:
        return _DUCKDB_TO_ARROW[duckdb_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported attribute type {duckdb_type!r}.") from exc


def ocel_to_duckdb(declared: str) -> str:
    try:
        return _OCEL_TO_DUCKDB[declared.lower()]
    except KeyError as exc:
        allowed = ", ".join(sorted(_OCEL_TO_DUCKDB))
        raise ValueError(
            f"Unsupported OCEL attribute type {declared!r}; expected one of {allowed}."
        ) from exc


def relation_if_nonempty(
    con: "duckdb.DuckDBPyConnection", sql: str
) -> "duckdb.DuckDBPyRelation | None":
    row = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()
    if not row or int(row[0]) == 0:
        return None
    return con.sql(sql)


@dataclass(frozen=True)
class Canonical:
    """Lazy, canonical DuckDB relations returned by a :class:`Source`."""

    event_types: Mapping[str, Mapping[str, str]]
    object_types: Mapping[str, Mapping[str, str]]
    events_for: Callable[[str], "duckdb.DuckDBPyRelation"]
    objects_for: Callable[[str], "duckdb.DuckDBPyRelation"]
    object_changes_for: Callable[[str], "duckdb.DuckDBPyRelation"]
    event_object: "duckdb.DuckDBPyRelation"
    object_object: "duckdb.DuckDBPyRelation | None"


@runtime_checkable
class Source(Protocol):
    """A parser that exposes canonical relations through DuckDB."""

    def attach(self, con: "duckdb.DuckDBPyConnection") -> Canonical: ...


def release_source(source: Source, con: "duckdb.DuckDBPyConnection") -> None:
    release = getattr(source, "release", None)
    if callable(release):
        release(con)


@contextmanager
def temporary_view(
    con: "duckdb.DuckDBPyConnection", relation: "duckdb.DuckDBPyRelation"
) -> Iterator[str]:
    name = f"_oceldb_relation_{uuid4().hex}"
    relation.create_view(name)
    try:
        yield name
    finally:
        con.execute(f'DROP VIEW IF EXISTS "{name}"')


class ArrowSource(ABC):
    """Adapter for sources that yield Arrow record batches."""

    @abstractmethod
    def event_types(self) -> Mapping[str, Mapping[str, str]]:
        """Return ``type -> {attr_name: duckdb_type}`` for events."""

    @abstractmethod
    def object_types(self) -> Mapping[str, Mapping[str, str]]:
        """Return ``type -> {attr_name: duckdb_type}`` for objects."""

    @abstractmethod
    def events_batches(self, type_name: str) -> Iterator[pa.RecordBatch]:
        """Yield event batches for *type_name*."""

    @abstractmethod
    def objects_batches(self, type_name: str) -> Iterator[pa.RecordBatch]:
        """Yield object identity batches for *type_name*."""

    @abstractmethod
    def object_changes_batches(self, type_name: str) -> Iterator[pa.RecordBatch]:
        """Yield object-change batches for *type_name*."""

    @abstractmethod
    def event_object_batches(self) -> Iterator[pa.RecordBatch]:
        """Yield E2O batches with denormalized type columns."""

    def object_object_batches(self) -> Iterator[pa.RecordBatch] | None:
        """Yield O2O batches, or ``None``."""
        return None

    def events_schema(self, type_name: str, attrs: Mapping[str, str]) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ocel_id", pa.string()),
                pa.field("ocel_time", pa.timestamp("us")),
                *(pa.field(n, duckdb_to_arrow(t)) for n, t in attrs.items()),
            ]
        )

    def objects_schema(self, type_name: str) -> pa.Schema:
        return pa.schema([pa.field("ocel_id", pa.string())])

    def object_changes_schema(
        self, type_name: str, attrs: Mapping[str, str]
    ) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ocel_id", pa.string()),
                pa.field("ocel_time", pa.timestamp("us")),
                pa.field("ocel_changed_field", pa.string()),
                *(pa.field(n, duckdb_to_arrow(t)) for n, t in attrs.items()),
            ]
        )

    def event_object_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ocel_event_id", pa.string()),
                pa.field("ocel_event_type", pa.string()),
                pa.field("ocel_object_id", pa.string()),
                pa.field("ocel_object_type", pa.string()),
                pa.field("ocel_qualifier", pa.string()),
            ]
        )

    def object_object_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("ocel_source_id", pa.string()),
                pa.field("ocel_source_type", pa.string()),
                pa.field("ocel_target_id", pa.string()),
                pa.field("ocel_target_type", pa.string()),
                pa.field("ocel_qualifier", pa.string()),
            ]
        )

    def attach(self, con: "duckdb.DuckDBPyConnection") -> Canonical:
        event_types = self.event_types()
        object_types = self.object_types()
        registered_names: list[str] = []

        def events_for(t: str) -> "duckdb.DuckDBPyRelation":
            schema = self.events_schema(t, event_types[t])
            return _arrow_to_relation(
                con, self.events_batches(t), schema, registered_names
            )

        def objects_for(t: str) -> "duckdb.DuckDBPyRelation":
            schema = self.objects_schema(t)
            return _arrow_to_relation(
                con, self.objects_batches(t), schema, registered_names
            )

        def object_changes_for(t: str) -> "duckdb.DuckDBPyRelation":
            schema = self.object_changes_schema(t, object_types[t])
            return _arrow_to_relation(
                con, self.object_changes_batches(t), schema, registered_names
            )

        event_object = _arrow_to_relation(
            con,
            self.event_object_batches(),
            self.event_object_schema(),
            registered_names,
        )

        o2o_batches = self.object_object_batches()
        object_object = (
            _arrow_to_relation(
                con, o2o_batches, self.object_object_schema(), registered_names
            )
            if o2o_batches is not None
            else None
        )
        self._oceldb_registered_names = registered_names

        return Canonical(
            event_types=event_types,
            object_types=object_types,
            events_for=events_for,
            objects_for=objects_for,
            object_changes_for=object_changes_for,
            event_object=event_object,
            object_object=object_object,
        )

    def release(self, con: "duckdb.DuckDBPyConnection") -> None:
        names: list[str] = getattr(self, "_oceldb_registered_names", [])
        for name in names:
            con.unregister(name)
        names.clear()


def _arrow_to_relation(
    con: "duckdb.DuckDBPyConnection",
    batches: Iterator[pa.RecordBatch],
    schema: pa.Schema,
    registered_names: list[str],
) -> "duckdb.DuckDBPyRelation":
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    name = f"_oceldb_arrow_{uuid4().hex}"
    con.register(name, reader)
    registered_names.append(name)
    return con.table(name)
