# OCEL 2.0 exchange conversion

`oceldb.io` converts the three OCEL 2.0 exchange formats—SQLite, JSON, and
XML—directly into the native type-partitioned Parquet format.

```python
from oceldb.io import convert_ocel

ocel = convert_ocel("orders.jsonocel", "orders.oceldb")
```

The source format is inferred from `.json`, `.jsonocel`, `.xml`, `.xmlocel`,
`.sqlite`, `.sqlite3`, or `.db`. Unknown extensions are detected from the JSON,
XML, or SQLite file signature. Use `format=` when detection should be explicit.

```python
ocel = convert_ocel(
    "download.data",
    "orders.oceldb",
    format="json",
    overwrite=True,
)
```

Format-specific entry points provide the same options:

```python
from oceldb.io import convert_json, convert_sqlite, convert_xml

json_log = convert_json("log.jsonocel", "json.oceldb")
xml_log = convert_xml("log.xmlocel", "xml.oceldb")
sqlite_log = convert_sqlite("log.sqlite", "sqlite.oceldb")
```

All conversion functions return an opened, lazy `OCEL`.

## Memory and execution model

Converters never build the complete OCEL as Python objects:

- JSON is parsed incrementally with `ijson`.
- XML uses secure incremental parsing and rejects entity declarations.
- SQLite is opened read-only and consumed as vectorized Polars batches.
- Normalized rows are flushed to temporary Parquet parts.
- JSON and XML relation endpoint types are resolved through a temporary
  disk-backed index rather than an in-memory object dictionary.
- Parser parts are compacted into bounded, sorted native shards.
- Event- and object-oriented E2O representations are built during conversion.
- The completed native snapshot is physically reopened before atomic
  installation.

The default `batch_size=10_000` is intentionally conservative. It controls
parser memory, not the final Parquet fragment count. Increasing it raises peak
memory roughly with row width and does not necessarily improve throughput.
Conversion uses temporary disk space next to the destination and removes it
after success or failure.

## Validation

`validate=True` is the default. It checks the complete native logical contract
before installation, including unique identifiers, endpoint references,
denormalized types, and object-change invariants.

```python
checked = convert_ocel("log.sqlite", "log.native", validate=True)
```

For a trusted source and maximum throughput, `validate=False` skips the final
whole-log logical pass. Structural parsing, declared attribute types, required
exchange fields, native schemas, and physical snapshot validation still
remain enforced.

Malformed exchange content raises `OCELConversionError`. Existing targets are
never partially modified. `overwrite=True` only replaces another physically
valid native snapshot and refuses unrelated files, directories, and final
symlinks.

## Type reconciliation

OCEL exchange schemas declare attributes per event or object type. Native
scans require a shared attribute name to use one dtype across partitions.
Converters therefore reconcile declarations as follows:

- identical declarations retain their native dtype;
- a shared integer/float attribute becomes `Float64`; and
- all other mixed declarations use their canonical string representation.

This rule is deterministic and prevents one type partition from changing how
another partition is read.
