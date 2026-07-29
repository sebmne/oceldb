"""Batched OCEL 2.0 SQLite-to-native conversion."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import polars as pl
from polars.exceptions import PolarsError

from oceldb import OCEL
from oceldb.core import schema as s
from oceldb.io._errors import OCELConversionError, conversion_error
from oceldb.io._schema import AttributeType, ExchangeSchema, TypeDeclarations
from oceldb.io._sink import TableSink

_REQUIRED_TABLES = {
    "event",
    "object",
    "event_map_type",
    "object_map_type",
    "event_object",
    "object_object",
}
_EVENT_CORE = {s.OCEL_ID, s.OCEL_TIME}
_OBJECT_CORE = {s.OCEL_ID, s.OCEL_TIME, s.OCEL_CHANGED_FIELD}


@dataclass(frozen=True)
class TypeMapping:
    """One standard type name to physical table-suffix mapping."""

    name: str
    suffix: str


def convert(
    source: Path,
    target: Path,
    staging: Path,
    overwrite: bool,
    validate: bool,
    batch_size: int,
) -> OCEL:
    """Convert a relational OCEL using read-only batched SQLite cursors."""
    connection = _connect(source)
    try:
        tables = _table_names(connection)
        missing = sorted(_REQUIRED_TABLES - tables)
        if missing:
            raise conversion_error(
                "SQLite schema",
                f"missing required tables: {missing}",
            )
        event_mappings = _mappings(connection, "event_map_type")
        object_mappings = _mappings(connection, "object_map_type")
        _validate_mapping_coverage(connection, "event", event_mappings)
        _validate_mapping_coverage(connection, "object", object_mappings)
        event_types = _type_declarations(
            connection,
            event_mappings,
            prefix="event",
            core=_EVENT_CORE,
        )
        object_types = _type_declarations(
            connection,
            object_mappings,
            prefix="object",
            core=_OBJECT_CORE,
        )
        schema = ExchangeSchema.build(event_types, object_types)
        sink = TableSink(staging / "tables", schema, batch_size=batch_size)
        typed_event_rows = _convert_events(
            connection,
            sink,
            schema,
            event_mappings,
            batch_size,
            validate=validate,
        )
        _convert_objects(connection, sink, batch_size)
        _convert_changes(
            connection,
            sink,
            schema,
            object_mappings,
            batch_size,
        )
        _convert_relations(connection, sink, "event_object", "e2o", batch_size)
        _convert_relations(connection, sink, "object_object", "o2o", batch_size)
        if validate:
            master_event_rows = _count(connection, "event")
            if typed_event_rows != master_event_rows:
                raise conversion_error(
                    "SQLite events",
                    f"event contains {master_event_rows} identities but typed "
                    f"event tables contain {typed_event_rows} rows",
                )
        return sink.finish(
            target,
            overwrite=overwrite,
            validate=validate,
        )
    except OCELConversionError:
        raise
    except sqlite3.Error as exc:
        raise OCELConversionError(f"Invalid OCEL SQLite database: {exc}") from exc
    except PolarsError as exc:
        raise OCELConversionError(
            f"Invalid value in OCEL SQLite database: {exc}"
        ) from exc
    finally:
        connection.close()


def _connect(source: Path) -> sqlite3.Connection:
    uri = source.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    return connection


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }


def _mappings(
    connection: sqlite3.Connection,
    table: str,
) -> list[TypeMapping]:
    _require_typed_columns(
        connection,
        table,
        {
            s.OCEL_TYPE: AttributeType.STRING,
            "ocel_type_map": AttributeType.STRING,
        },
    )
    result: list[TypeMapping] = []
    names: set[str] = set()
    suffixes: set[str] = set()
    query = (
        f"SELECT {_quote(s.OCEL_TYPE)}, {_quote('ocel_type_map')} "
        f"FROM {_quote(table)} ORDER BY {_quote(s.OCEL_TYPE)}"
    )
    for index, row in enumerate(connection.execute(query)):
        context = f"{table}[{index}]"
        name = _sqlite_string(row[0], f"{context}.ocel_type")
        suffix = _sqlite_string(row[1], f"{context}.ocel_type_map")
        if name in names:
            raise conversion_error(context, f"duplicate type {name!r}")
        if suffix in suffixes:
            raise conversion_error(context, f"duplicate table suffix {suffix!r}")
        names.add(name)
        suffixes.add(suffix)
        result.append(TypeMapping(name, suffix))
    return result


def _validate_mapping_coverage(
    connection: sqlite3.Connection,
    master: str,
    mappings: list[TypeMapping],
) -> None:
    _require_typed_columns(
        connection,
        master,
        {
            s.OCEL_ID: AttributeType.STRING,
            s.OCEL_TYPE: AttributeType.STRING,
        },
    )
    values = {
        _sqlite_string(row[0], f"{master}.ocel_type")
        for row in connection.execute(
            f"SELECT DISTINCT {_quote(s.OCEL_TYPE)} FROM {_quote(master)}"
        )
    }
    mapped = {mapping.name for mapping in mappings}
    missing = sorted(values - mapped)
    if missing:
        raise conversion_error(
            f"SQLite {master}",
            f"types missing from {master}_map_type: {missing}",
        )


def _type_declarations(
    connection: sqlite3.Connection,
    mappings: list[TypeMapping],
    *,
    prefix: str,
    core: set[str],
) -> TypeDeclarations:
    result: TypeDeclarations = {}
    tables = _table_names(connection)
    for mapping in mappings:
        table = f"{prefix}_{mapping.suffix}"
        if table not in tables:
            raise conversion_error(
                f"SQLite {prefix}_map_type",
                f"mapped table {table!r} does not exist",
            )
        columns = _columns(connection, table)
        required = _EVENT_CORE if prefix == "event" else _OBJECT_CORE
        missing = sorted(required - {name for name, _ in columns})
        if missing:
            raise conversion_error(
                f"SQLite table {table!r}",
                f"missing required columns: {missing}",
            )
        expected_core = {
            s.OCEL_ID: AttributeType.STRING,
            s.OCEL_TIME: AttributeType.TIME,
        }
        if prefix == "object":
            expected_core[s.OCEL_CHANGED_FIELD] = AttributeType.STRING
        _require_typed_columns(connection, table, expected_core)
        result[mapping.name] = {
            name: _attribute_type(sqlite_type, context=f"{table}.{name}")
            for name, sqlite_type in columns
            if name not in core
        }
    return result


def _convert_events(
    connection: sqlite3.Connection,
    sink: TableSink,
    schema: ExchangeSchema,
    mappings: list[TypeMapping],
    batch_size: int,
    *,
    validate: bool,
) -> int:
    total = 0
    for mapping in mappings:
        table = f"event_{mapping.suffix}"
        declarations = schema.event_types[mapping.name]
        if validate:
            invalid = _invalid_typed_events(
                connection,
                table,
                mapping.name,
            )
            if invalid:
                raise conversion_error(
                    f"SQLite table {table!r}",
                    f"contains {invalid} event(s) absent from event or assigned "
                    "to another type",
                )
        overrides = _read_overrides(
            declarations,
            core={s.OCEL_ID: pl.String(), s.OCEL_TIME: pl.String()},
        )
        for batch_index, batch in enumerate(
            _frames(connection, f"SELECT * FROM {_quote(table)}", batch_size, overrides)
        ):
            _require_frame_strings(
                batch,
                (s.OCEL_ID,),
                context=f"{table} batch {batch_index}",
            )
            normalized = batch.select(
                pl.col(s.OCEL_ID),
                _timestamp_expr(s.OCEL_TIME),
                pl.lit(mapping.name, dtype=pl.String).alias(s.OCEL_TYPE),
                *(
                    _attribute_expr(
                        name,
                        declared,
                        schema.event_attributes[name],
                    )
                    for name, declared in declarations.items()
                ),
            )
            sink.add_frame("events", normalized)
            total += batch.height
    return total


def _convert_objects(
    connection: sqlite3.Connection,
    sink: TableSink,
    batch_size: int,
) -> None:
    query = f"SELECT {_quote(s.OCEL_ID)}, {_quote(s.OCEL_TYPE)} FROM {_quote('object')}"
    overrides = {s.OCEL_ID: pl.String(), s.OCEL_TYPE: pl.String()}
    for batch_index, batch in enumerate(
        _frames(connection, query, batch_size, overrides)
    ):
        _require_frame_strings(
            batch,
            (s.OCEL_ID, s.OCEL_TYPE),
            context=f"object batch {batch_index}",
        )
        sink.add_frame("objects", batch)


def _convert_changes(
    connection: sqlite3.Connection,
    sink: TableSink,
    schema: ExchangeSchema,
    mappings: list[TypeMapping],
    batch_size: int,
) -> None:
    for mapping in mappings:
        table = f"object_{mapping.suffix}"
        declarations = schema.object_types[mapping.name]
        overrides = _read_overrides(
            declarations,
            core={
                s.OCEL_ID: pl.String(),
                s.OCEL_TIME: pl.String(),
                s.OCEL_CHANGED_FIELD: pl.String(),
            },
        )
        for batch_index, batch in enumerate(
            _frames(connection, f"SELECT * FROM {_quote(table)}", batch_size, overrides)
        ):
            context = f"{table} batch {batch_index}"
            _require_frame_strings(batch, (s.OCEL_ID,), context=context)
            _validate_changed_fields(batch, declarations, context=context)
            normalized = batch.select(
                pl.col(s.OCEL_ID),
                _timestamp_expr(s.OCEL_TIME),
                pl.col(s.OCEL_CHANGED_FIELD),
                pl.col(s.OCEL_CHANGED_FIELD).is_null().alias(s.OCEL_IS_INITIAL),
                pl.lit(mapping.name, dtype=pl.String).alias(s.OCEL_TYPE),
                *(
                    _attribute_expr(
                        name,
                        declared,
                        schema.object_attributes[name],
                    )
                    for name, declared in declarations.items()
                ),
            )
            sink.add_frame("object_changes", normalized)


def _convert_relations(
    connection: sqlite3.Connection,
    sink: TableSink,
    source_table: str,
    target_table: str,
    batch_size: int,
) -> None:
    if target_table == "e2o":
        columns = tuple(s.E2O_SCHEMA)
        query = f"""
            SELECT relation.{_quote(s.OCEL_EVENT_ID)}
                       AS {_quote(s.OCEL_EVENT_ID)},
                   event.{_quote(s.OCEL_TYPE)}
                       AS {_quote(s.OCEL_EVENT_TYPE)},
                   relation.{_quote(s.OCEL_OBJECT_ID)}
                       AS {_quote(s.OCEL_OBJECT_ID)},
                   object.{_quote(s.OCEL_TYPE)}
                       AS {_quote(s.OCEL_OBJECT_TYPE)},
                   relation.{_quote(s.OCEL_QUALIFIER)}
                       AS {_quote(s.OCEL_QUALIFIER)}
            FROM {_quote(source_table)} AS relation
            LEFT JOIN {_quote("event")} AS event
              ON relation.{_quote(s.OCEL_EVENT_ID)} = event.{_quote(s.OCEL_ID)}
            LEFT JOIN {_quote("object")} AS object
              ON relation.{_quote(s.OCEL_OBJECT_ID)} = object.{_quote(s.OCEL_ID)}
        """
        required = {s.OCEL_EVENT_ID, s.OCEL_OBJECT_ID, s.OCEL_QUALIFIER}
    else:
        columns = tuple(s.O2O_SCHEMA)
        query = f"""
            SELECT relation.{_quote(s.OCEL_SOURCE_ID)}
                       AS {_quote(s.OCEL_SOURCE_ID)},
                   source.{_quote(s.OCEL_TYPE)}
                       AS {_quote(s.OCEL_SOURCE_TYPE)},
                   relation.{_quote(s.OCEL_TARGET_ID)}
                       AS {_quote(s.OCEL_TARGET_ID)},
                   target.{_quote(s.OCEL_TYPE)}
                       AS {_quote(s.OCEL_TARGET_TYPE)},
                   relation.{_quote(s.OCEL_QUALIFIER)}
                       AS {_quote(s.OCEL_QUALIFIER)}
            FROM {_quote(source_table)} AS relation
            LEFT JOIN {_quote("object")} AS source
              ON relation.{_quote(s.OCEL_SOURCE_ID)} = source.{_quote(s.OCEL_ID)}
            LEFT JOIN {_quote("object")} AS target
              ON relation.{_quote(s.OCEL_TARGET_ID)} = target.{_quote(s.OCEL_ID)}
        """
        required = {s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER}
    _require_typed_columns(
        connection,
        source_table,
        {column: AttributeType.STRING for column in required},
    )
    _validate_relation_identifiers(connection, source_table, required)
    _validate_relation_endpoints(connection, source_table, target_table)
    overrides = {column: pl.String() for column in columns}
    if target_table == "e2o":
        orderings = (
            (
                "e2o",
                (s.OCEL_EVENT_ID, s.OCEL_OBJECT_ID, s.OCEL_QUALIFIER),
            ),
            (
                "e2o_by_object",
                (s.OCEL_OBJECT_ID, s.OCEL_EVENT_ID, s.OCEL_QUALIFIER),
            ),
        )
    else:
        orderings = (
            (
                "o2o",
                (s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER),
            ),
        )
    for output_table, ordering in orderings:
        ordered_query = (
            query + " ORDER BY " + ", ".join(_quote(column) for column in ordering)
        )
        _stage_relation_query(
            connection,
            sink,
            source_table=source_table,
            output_table=output_table,
            query=ordered_query,
            columns=columns,
            required_strings=tuple(
                column for column in columns if column != s.OCEL_QUALIFIER
            ),
            overrides=overrides,
            batch_size=batch_size,
        )


def _stage_relation_query(
    connection: sqlite3.Connection,
    sink: TableSink,
    *,
    source_table: str,
    output_table: str,
    query: str,
    columns: tuple[str, ...],
    required_strings: tuple[str, ...],
    overrides: Mapping[str, pl.DataType],
    batch_size: int,
) -> None:
    for batch_index, batch in enumerate(
        _frames(connection, query, batch_size, overrides)
    ):
        _require_frame_strings(
            batch,
            required_strings,
            context=f"{source_table} batch {batch_index}",
        )
        sink.add_sorted_frame(output_table, batch)


def _validate_relation_identifiers(
    connection: sqlite3.Connection,
    table: str,
    columns: set[str],
) -> None:
    for column in sorted(columns - {s.OCEL_QUALIFIER}):
        row = connection.execute(
            f"SELECT {_quote(column)} FROM {_quote(table)} "
            f"WHERE typeof({_quote(column)}) != 'text' "
            f"OR {_quote(column)} = '' LIMIT 1"
        ).fetchone()
        if row is None:
            continue
        value = "NULL" if row[0] is None else repr(row[0])
        raise conversion_error(
            table,
            f"{column} must contain non-empty TEXT; found {value}",
        )


def _validate_relation_endpoints(
    connection: sqlite3.Connection,
    table: str,
    target_table: str,
) -> None:
    if target_table == "e2o":
        checks = (
            (
                s.OCEL_EVENT_ID,
                "event",
                s.OCEL_OBJECT_ID,
                "event",
            ),
            (
                s.OCEL_OBJECT_ID,
                "object",
                s.OCEL_EVENT_ID,
                "object",
            ),
        )
    else:
        checks = (
            (
                s.OCEL_SOURCE_ID,
                "object",
                s.OCEL_TARGET_ID,
                "source object",
            ),
            (
                s.OCEL_TARGET_ID,
                "object",
                s.OCEL_SOURCE_ID,
                "target object",
            ),
        )
    for reference, master, companion, endpoint in checks:
        join = (
            f"FROM {_quote(table)} AS relation "
            f"LEFT JOIN {_quote(master)} AS master "
            f"ON relation.{_quote(reference)} = master.{_quote(s.OCEL_ID)} "
            f"WHERE master.{_quote(s.OCEL_ID)} IS NULL"
        )
        counts = connection.execute(
            f"SELECT COUNT(*), "
            f"COUNT(DISTINCT relation.{_quote(reference)}) {join}"
        ).fetchone()
        assert counts is not None
        row_count = int(counts[0])
        if not row_count:
            continue
        first = connection.execute(
            f"SELECT relation.{_quote(reference)}, "
            f"relation.{_quote(companion)} {join} "
            f"ORDER BY relation.{_quote(reference)}, "
            f"relation.{_quote(companion)} LIMIT 1"
        ).fetchone()
        assert first is not None
        distinct = int(counts[1])
        rows = "row" if row_count == 1 else "rows"
        ids = "id" if distinct == 1 else "ids"
        verb = "references" if row_count == 1 else "reference"
        raise conversion_error(
            table,
            f"{row_count} relation {rows} {verb} {distinct} unknown "
            f"{endpoint} {ids}; first is {first[0]!r} "
            f"({companion}={first[1]!r})",
        )


def _frames(
    connection: sqlite3.Connection,
    query: str,
    batch_size: int,
    schema_overrides: Mapping[str, pl.DataType],
) -> Iterator[pl.DataFrame]:
    """Read a SQLite query into bounded native Polars batches."""
    result = pl.read_database(
        query,
        connection,
        iter_batches=True,
        batch_size=batch_size,
        schema_overrides=dict(schema_overrides),
        infer_schema_length=None,
    )
    return result


def _read_overrides(
    declarations: Mapping[str, AttributeType],
    *,
    core: Mapping[str, pl.DataType],
) -> dict[str, pl.DataType]:
    result = dict(core)
    for name, declared in declarations.items():
        result[name] = pl.String() if declared is AttributeType.TIME else declared.dtype
    return result


def _timestamp_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .str.to_datetime(
            time_unit="us",
            time_zone="UTC",
            strict=True,
        )
        .alias(column)
    )


def _attribute_expr(
    name: str,
    declared: AttributeType,
    target: pl.DataType,
) -> pl.Expr:
    if target == pl.Datetime("us", "UTC"):
        return _timestamp_expr(name)
    return pl.col(name).cast(target, strict=True).alias(name)


def _require_frame_strings(
    frame: pl.DataFrame,
    columns: tuple[str, ...],
    *,
    context: str,
) -> None:
    counts = frame.select(
        (
            pl.col(column).is_null()
            | (pl.col(column).str.len_chars() == 0)
        )
        .sum()
        .alias(column)
        for column in columns
    ).row(0, named=True)
    invalid = [
        f"{column} ({count} invalid {'value' if count == 1 else 'values'})"
        for column, count in counts.items()
        if count
    ]
    if invalid:
        raise conversion_error(
            context,
            "expected non-empty TEXT; invalid columns: " + ", ".join(invalid),
        )


def _validate_changed_fields(
    frame: pl.DataFrame,
    declarations: Mapping[str, AttributeType],
    *,
    context: str,
) -> None:
    changed = pl.col(s.OCEL_CHANGED_FIELD)
    names = list(declarations)
    if frame.select((changed.is_not_null() & ~changed.is_in(names)).any()).item():
        raise conversion_error(
            context,
            "ocel_changed_field names an undeclared attribute",
        )


def _invalid_typed_events(
    connection: sqlite3.Connection,
    typed_table: str,
    type_name: str,
) -> int:
    row = connection.execute(
        f"SELECT COUNT(*) FROM {_quote(typed_table)} AS typed "
        f"LEFT JOIN {_quote('event')} AS master "
        f"ON typed.{_quote(s.OCEL_ID)} = master.{_quote(s.OCEL_ID)} "
        f"WHERE master.{_quote(s.OCEL_ID)} IS NULL "
        f"OR master.{_quote(s.OCEL_TYPE)} != ?",
        (type_name,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _count(connection: sqlite3.Connection, table: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()
    assert row is not None
    return int(row[0])


def _require_columns(
    connection: sqlite3.Connection,
    table: str,
    required: set[str],
) -> None:
    names = {name for name, _ in _columns(connection, table)}
    missing = sorted(required - names)
    if missing:
        raise conversion_error(
            f"SQLite table {table!r}",
            f"missing required columns: {missing}",
        )


def _require_typed_columns(
    connection: sqlite3.Connection,
    table: str,
    required: dict[str, AttributeType],
) -> None:
    _require_columns(connection, table, set(required))
    columns = dict(_columns(connection, table))
    incompatible = [
        f"{name} ({columns[name]!r}, expected {expected.value})"
        for name, expected in required.items()
        if _attribute_type(columns[name], context=f"{table}.{name}") is not expected
    ]
    if incompatible:
        raise conversion_error(
            f"SQLite table {table!r}",
            f"has incompatible core columns: {', '.join(incompatible)}",
        )


def _columns(
    connection: sqlite3.Connection,
    table: str,
) -> list[tuple[str, str]]:
    return [
        (str(row[1]), str(row[2]))
        for row in connection.execute(f"PRAGMA table_info({_quote(table)})")
    ]


def _attribute_type(sqlite_type: str, *, context: str) -> AttributeType:
    normalized = sqlite_type.strip().upper()
    if any(name in normalized for name in ("TIMESTAMP", "DATETIME", "DATE")):
        return AttributeType.TIME
    if "BOOL" in normalized:
        return AttributeType.BOOLEAN
    if "INT" in normalized:
        return AttributeType.INTEGER
    if any(
        name in normalized for name in ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL")
    ):
        return AttributeType.FLOAT
    if any(name in normalized for name in ("TEXT", "CHAR", "CLOB", "VARCHAR")):
        return AttributeType.STRING
    raise conversion_error(
        context,
        f"unsupported declared SQLite type {sqlite_type!r}",
    )


def _sqlite_string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value:
        raise conversion_error(context, "must be a non-empty TEXT value")
    return value


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
