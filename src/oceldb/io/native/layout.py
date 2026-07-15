"""Single source of truth for the physical native Parquet layout."""

from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
import urllib.parse
from collections.abc import Mapping

import polars as pl

from oceldb import schema as s
from oceldb.io.errors import OCELIOError
from oceldb.schema import OCELSchema, TypeAttributes
from oceldb.schema._layout import (
    CHANGE_CORE,
    E2O_COLUMNS,
    EVENT_CORE,
    OBJECT_CORE,
    O2O_COLUMNS,
)

COMPRESSION = "zstd"
DATA_FILENAME = "data.parquet"
UNSORTED_DATA_FILENAME = "data.unsorted.parquet"
PARTITION_PREFIX = f"{s.OCEL_TYPE}="


class NativeStorageError(OCELIOError):
    """A native dataset does not match its declared physical layout."""


@dataclass(frozen=True)
class NativeTable:
    """Physical contract for one native OCEL table."""

    key: str
    manifest_name: str
    path: str
    columns: tuple[str, ...]
    base_schema: Mapping[str, pl.DataType]
    sort_by: tuple[str, ...]
    partitioned: bool = False
    stores_attributes: bool = False
    optional: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "base_schema", MappingProxyType(dict(self.base_schema))
        )

    def root(self, base: str | Path) -> Path:
        return Path(base) / self.path

    def file(
        self,
        base: str | Path,
        *,
        type_name: str | None = None,
        unsorted: bool = False,
    ) -> Path:
        """Return the physical file for this table and optional type partition."""
        root = self.root(base)
        if self.partitioned:
            if type_name is None:
                raise ValueError(f"{self.key} requires a type partition.")
            filename = UNSORTED_DATA_FILENAME if unsorted else DATA_FILENAME
            return root / partition_name(type_name) / filename
        if type_name is not None:
            raise ValueError(f"{self.key} is not partitioned by type.")
        if not unsorted:
            return root
        return root.with_name(f"{root.stem}.unsorted{root.suffix}")

    def schema(
        self, attributes: TypeAttributes | None = None
    ) -> dict[str, pl.DataType]:
        result = dict(self.base_schema)
        if self.stores_attributes and attributes:
            result.update(
                {
                    name: attr_type.polars_dtype()
                    for name, attr_type in attributes.items()
                }
            )
        return result

    def manifest_entry(self) -> dict[str, object]:
        return {
            "path": self.path,
            "partitionedBy": [s.OCEL_TYPE] if self.partitioned else [],
        }


EVENTS = NativeTable(
    key="events",
    manifest_name="events",
    path="events",
    columns=EVENT_CORE,
    base_schema={
        s.OCEL_ID: pl.String(),
        s.OCEL_TIME: pl.Datetime("us", "UTC"),
    },
    sort_by=(s.OCEL_TIME,),
    partitioned=True,
    stores_attributes=True,
)

OBJECTS = NativeTable(
    key="objects",
    manifest_name="objects",
    path="objects",
    columns=OBJECT_CORE,
    base_schema={s.OCEL_ID: pl.String()},
    sort_by=(s.OCEL_ID,),
    partitioned=True,
)

OBJECT_CHANGES = NativeTable(
    key="object_changes",
    manifest_name="objectChanges",
    path="object_changes",
    columns=CHANGE_CORE,
    base_schema={
        s.OCEL_ID: pl.String(),
        s.OCEL_TIME: pl.Datetime("us", "UTC"),
        s.OCEL_CHANGED_FIELD: pl.String(),
    },
    sort_by=(s.OCEL_ID, s.OCEL_TIME),
    partitioned=True,
    stores_attributes=True,
)

EVENT_OBJECT = NativeTable(
    key="event_object",
    manifest_name="eventObject",
    path="event_object.parquet",
    columns=E2O_COLUMNS,
    base_schema={name: pl.String() for name in E2O_COLUMNS},
    sort_by=(s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID),
)

OBJECT_OBJECT = NativeTable(
    key="object_object",
    manifest_name="objectObject",
    path="object_object.parquet",
    columns=O2O_COLUMNS,
    base_schema={name: pl.String() for name in O2O_COLUMNS},
    sort_by=(s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID),
    optional=True,
)

TABLES = (EVENTS, OBJECTS, OBJECT_CHANGES, EVENT_OBJECT, OBJECT_OBJECT)


def manifest_layout() -> dict[str, dict[str, object]]:
    return {table.manifest_name: table.manifest_entry() for table in TABLES}


def encode_type_name(type_name: str) -> str:
    """Encode an OCEL type for a partition directory."""
    return urllib.parse.quote(type_name, safe="")


def decode_type_name(encoded: str) -> str:
    """Decode a type name from a partition directory."""
    return urllib.parse.unquote(encoded)


def partition_name(type_name: str) -> str:
    return f"{PARTITION_PREFIX}{encode_type_name(type_name)}"


def validate_native_layout(path: str | Path, schema: OCELSchema) -> None:
    """Validate physical tables and partitions against the native manifest."""
    base = Path(path)
    unfinished = next(base.rglob("*.unsorted.parquet"), None)
    if unfinished is not None:
        raise NativeStorageError(
            f"Native dataset contains unfinished staging file: {unfinished}."
        )

    _validate_partitioned(base, EVENTS, schema.event_types)
    _validate_partitioned(base, OBJECTS, schema.object_types)
    _validate_partitioned(base, OBJECT_CHANGES, schema.object_types)
    _validate_relation(base, EVENT_OBJECT)
    _validate_relation(base, OBJECT_OBJECT)


def _validate_partitioned(
    base: Path,
    table: NativeTable,
    declared_types: Mapping[str, TypeAttributes],
) -> None:
    root = table.root(base)
    if not root.is_dir():
        raise NativeStorageError(
            f"Native table {table.key!r} requires directory: {root}."
        )

    partitions: dict[str, Path] = {}
    for child in sorted(root.iterdir()):
        if child.name.startswith("."):
            continue
        if not child.is_dir() or not child.name.startswith(PARTITION_PREFIX):
            raise NativeStorageError(
                f"Malformed partition in native table {table.key!r}: {child}."
            )
        type_name = decode_type_name(child.name.removeprefix(PARTITION_PREFIX))
        if child.name != partition_name(type_name):
            raise NativeStorageError(
                f"Non-canonical type partition {child.name!r} in {table.key!r}."
            )
        if type_name in partitions:
            raise NativeStorageError(
                f"Duplicate type partition {type_name!r} in {table.key!r}."
            )
        partitions[type_name] = child

    expected = set(declared_types)
    observed = set(partitions)
    if missing := sorted(expected - observed):
        raise NativeStorageError(
            f"Native table {table.key!r} is missing type partitions: {missing}."
        )
    if unexpected := sorted(observed - expected):
        raise NativeStorageError(
            f"Native table {table.key!r} has undeclared type partitions: {unexpected}."
        )

    for type_name in sorted(expected):
        file = table.file(base, type_name=type_name)
        attributes = declared_types[type_name] if table.stores_attributes else None
        _validate_parquet(file, table, attributes, type_name=type_name)


def _validate_relation(base: Path, table: NativeTable) -> None:
    file = table.file(base)
    if not file.exists():
        if table.optional:
            return
        raise NativeStorageError(
            f"Native table {table.key!r} requires Parquet file: {file}."
        )
    _validate_parquet(file, table)


def _validate_parquet(
    file: Path,
    table: NativeTable,
    attributes: TypeAttributes | None = None,
    *,
    type_name: str | None = None,
) -> None:
    if not file.is_file():
        raise NativeStorageError(f"Native Parquet path is not a file: {file}.")
    try:
        actual = pl.read_parquet_schema(file)
    except Exception as exc:
        raise NativeStorageError(
            f"Cannot read native Parquet schema {file}: {exc}"
        ) from exc

    expected = table.schema(attributes)
    context = table.key if type_name is None else f"{table.key}[{type_name!r}]"
    if tuple(actual) != tuple(expected):
        raise NativeStorageError(
            f"Native table {context} has columns {list(actual)}; "
            f"expected {list(expected)}."
        )
    incompatible = {
        name: (actual[name], dtype)
        for name, dtype in expected.items()
        if actual[name] != dtype
    }
    if incompatible:
        details = ", ".join(
            f"{name}: {found!r} != {required!r}"
            for name, (found, required) in incompatible.items()
        )
        raise NativeStorageError(
            f"Native table {context} has incompatible datatypes: {details}."
        )
