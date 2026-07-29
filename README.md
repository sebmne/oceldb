# oceldb

Fast, memory-conscious access to large [OCEL 2.0](https://www.ocel-standard.org/)
event logs.

oceldb exposes an object-centric event log as five lazy
[Polars](https://pola.rs/) tables and stores it as an immutable,
type-partitioned Parquet snapshot. Queries load only the columns, type
partitions, and relation ranges they need.

The project focuses on three properties:

- predictable memory use for logs that do not fit comfortably in memory;
- a small, typed Python API built around standard Polars lazy frames; and
- a portable native format that remains readable without oceldb.

> oceldb is currently pre-1.0. The public `OCEL` API and native format version
> 2 are the stability boundaries; exchange conversion and operations remain
> areas of active optimization.

## Features

- Lazy access to events, objects, object changes, E2O, and O2O relations.
- Type and identifier filtering directly on every table accessor.
- Forward-filled object-state reconstruction.
- Immutable filtering, views, projection, and classical flattening.
- Direct OCEL 2.0 SQLite, JSON, and XML conversion.
- Type-partitioned Parquet with per-type attribute schemas.
- Event- and object-oriented E2O storage for efficient traversal in both
  directions.
- Transactional snapshot installation and guarded replacement.
- Optional DuckDB SQL over all five logical tables.
- Public type information, including a `py.typed` marker.

## Requirements and installation

oceldb requires Python 3.11 or newer.

```bash
pip install oceldb
```

With optional DuckDB SQL support:

```bash
pip install "oceldb[sql]"
```

Using uv:

```bash
uv add oceldb
```

## Quick start

Open an existing native snapshot. Opening reads the manifest and Parquet
metadata; it does not materialize the log.

```python
from oceldb import OCEL

ocel = OCEL.open("orders.oceldb")

counts = (
    ocel.events()
    .group_by("ocel_type")
    .len()
    .sort("len", descending=True)
    .collect()
)
```

Convert an OCEL 2.0 exchange file directly into native storage:

```python
from oceldb.io import convert_ocel

ocel = convert_ocel(
    "orders.sqlite",
    "orders.oceldb",
    validate=True,
)
```

Apply operations directly or compose them into a pipeline:

```python
import polars as pl

from oceldb.operations import (
    filter_events_by_type,
    filter_objects_by_attribute,
    project,
)

paid_orders = (
    ocel
    >> filter_events_by_type("Pay Order")
    >> filter_objects_by_attribute(
        pl.col("status") == "paid",
        object_types="order",
        when="any",
    )
    >> project("order-42")
)

paid_orders.write("paid-order-42.oceldb")
```

`OCEL` handles and operations are immutable. Evaluation begins only when a
Polars execution method such as `collect()` or `sink_parquet()` is called, or
when a snapshot is validated or written.

## Data model

An OCEL consists of five logical tables:

| Accessor | Contents |
| --- | --- |
| `events()` | Event identity, timestamp, type, and event attributes |
| `objects()` | Authoritative object identity and type |
| `object_changes()` | Initial object states and sparse attribute changes |
| `e2o()` | Qualified event-to-object relations |
| `o2o()` | Qualified object-to-object relations |

Object attributes live in `object_changes()`. Use `object_states()` when a
complete forward-filled state is required at every recorded change:

```python
states = ocel.object_states("order", ids="order-42")
```

Every accessor returns a `polars.LazyFrame`.

## Table access

Type filters are positional for typed entity tables:

```python
ocel.events()
ocel.events("Create Order")
ocel.events("Create Order", "Cancel Order", ids=["e1", "e2"])

ocel.objects()
ocel.objects("order", ids="order-42")

ocel.object_changes("order", ids="order-42")
ocel.object_states("order", ids="order-42")

ocel.event_types()
ocel.object_types()
```

Relation accessors accept type filters first, followed by endpoint and
qualifier filters:

```python
relations = ocel.e2o(
    event_types="Pay Order",
    object_types="order",
    event=["e1", "e2"],
    object="order-42",
    qualifier="order",
)

links = ocel.o2o(
    source_types="order",
    target_types="item",
    source="order-42",
    target=["item-1", "item-2"],
    qualifier="contains",
)
```

A selector accepts one string or an iterable of strings. `None` means
unrestricted. Required selectors reject empty input, while an empty optional
identifier iterable produces an empty result.

Native E2O access automatically chooses the event- or object-oriented physical
representation. This is transparent to callers and does not change the
logical relation schema.

## Operations

The operations package contains lazy, immutable filters and transformations:

```python
from oceldb.operations import (
    filter_events_by_object_count,
    filter_events_by_time,
    filter_objects_by_event_count,
    flatten,
    project,
    view,
)

recent = ocel >> filter_events_by_time(start="2025-01-01T00:00:00Z")

connected = recent >> filter_events_by_object_count(
    object_types="order",
    min_count=1,
)

frequent_objects = connected >> filter_objects_by_event_count(
    event_types=["Create Order", "Update Order"],
    min_count=2,
)

selected = frequent_objects >> view(
    event_types=["Create Order", "Update Order"],
    object_types=["order", "item"],
)

one_order = selected >> project("order-42")
```

Event filters retain the selected events and induce the connected objects.
Object filters apply the symmetric rule. Explicitly selected relationless
entities remain present.

Flatten one object type into a classical event log:

```python
classical = flatten(
    ocel,
    "order",
    case_attributes=["customer", "region"],
)
```

Object attributes are point-in-time state columns by default.
`case_attributes` explicitly promotes attributes from the initial object state
to `case:<name>` columns.

## Exchange conversion

The generic converter detects SQLite, JSON, or XML from the extension and, when
necessary, from the file signature:

```python
from oceldb.io import (
    convert_json,
    convert_ocel,
    convert_sqlite,
    convert_xml,
)

ocel = convert_ocel("source.data", "target.oceldb")
sqlite_ocel = convert_sqlite("source.sqlite", "sqlite.oceldb")
json_ocel = convert_json("source.jsonocel", "json.oceldb")
xml_ocel = convert_xml("source.xmlocel", "xml.oceldb")
```

Null timed object-attribute changes in SQLite, JSON, and XML are imported as
tombstones, clearing the previous value from that timestamp onward.

Converters write through bounded batches and install the result only after the
complete native snapshot can be reopened. JSON and XML are parsed
incrementally. SQLite ingestion uses vectorized Polars batches. JSON and XML
relation endpoints are resolved through a temporary disk-backed index rather
than a complete in-memory object map.

`validate=True` executes the complete logical OCEL contract before
installation. Set it to `False` only for trusted inputs when conversion
throughput is more important than whole-log validation.

See [docs/io.md](docs/io.md) for format behavior, type reconciliation, and
conversion errors.

## Native storage

Native snapshots are immutable directories:

```text
orders.oceldb/
  manifest.json
  events/
    ocel_type=Create%20Order/
      part-00000.parquet
  objects/
    ocel_type=order/
      part-00000.parquet
  object_changes/
    ocel_type=order/
      part-00000.parquet
  e2o/
    part-00000.parquet
  o2o/
    part-00000.parquet
  indexes/
    e2o_by_object/
      part-00000.parquet
```

Events, objects, and object changes are partitioned by URL-encoded
`ocel_type`. Individual type partitions store only attributes that carry a
value for that type. E2O and O2O use sorted, bounded Parquet shards. The
secondary E2O representation stores the same logical rows in object-oriented
order.

The format uses standard Parquet and can be queried directly:

```python
import polars as pl

events = pl.scan_parquet(
    "orders.oceldb/events/ocel_type=*/*.parquet",
    hive_partitioning=True,
)
e2o = pl.scan_parquet("orders.oceldb/e2o/*.parquet")
```

Writing is staged beside the destination and committed by directory rename.
`overwrite=True` replaces only another physically valid oceldb snapshot; it
does not replace unrelated paths or follow a final symlink. Copying an
unchanged native handle reuses its committed Parquet files without decoding
and re-encoding them.

The complete format version 2 contract is documented in
[docs/storage-format.md](docs/storage-format.md).

## Validation and errors

Opening validates the manifest, required directories, and physical Parquet
schemas. Row data remains lazy:

```python
ocel = OCEL.open("orders.oceldb")
```

Logical validation checks required values, identifier uniqueness, endpoint
references and types, and object-change invariants:

```python
ocel = OCEL.open("orders.oceldb", validate=True)
# or
ocel.validate()
```

The public error contract is:

| Error | Meaning |
| --- | --- |
| `FileNotFoundError` | A snapshot or required table is missing |
| `FileExistsError` | A target exists and `overwrite=False` |
| `OCELFormatError` | A native manifest, layout, or Parquet schema is malformed |
| `OCELStorageError` | A snapshot cannot be installed or replaced safely |
| `OCELValidationError` | The logical OCEL contract is violated |
| `OCELConversionError` | An exchange file cannot be converted |

All oceldb-specific errors inherit from `OCELDBError`.

## SQL

Install the `sql` extra to execute eager DuckDB queries:

```python
result = ocel.sql(
    """
    SELECT ocel_type, count(*) AS events
    FROM events
    GROUP BY ocel_type
    ORDER BY events DESC
    """
)
```

The method registers `events`, `objects`, `object_changes`, `e2o`, and `o2o`
as DuckDB views and returns a `polars.DataFrame`.

## Manual construction

`OCEL.from_frames()` accepts eager or lazy Polars frames, validates their
schemas immediately, and remains lazy unless `validate=True` is requested:

```python
from datetime import datetime, timezone

import polars as pl

from oceldb import OCEL

timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc)

ocel = OCEL.from_frames(
    events=pl.LazyFrame(
        {
            "ocel_id": ["e1"],
            "ocel_time": [timestamp],
            "ocel_type": ["Create Order"],
            "amount": [42.0],
        }
    ),
    objects=pl.LazyFrame(
        {
            "ocel_id": ["order-42"],
            "ocel_type": ["order"],
        }
    ),
    object_changes=pl.LazyFrame(
        {
            "ocel_id": ["order-42"],
            "ocel_time": [timestamp],
            "ocel_changed_field": pl.Series([None], dtype=pl.String),
            "ocel_is_initial": [True],
            "ocel_type": ["order"],
            "status": ["created"],
        }
    ),
    e2o=pl.LazyFrame(
        {
            "ocel_event_id": ["e1"],
            "ocel_event_type": ["Create Order"],
            "ocel_object_id": ["order-42"],
            "ocel_object_type": ["order"],
            "ocel_qualifier": ["order"],
        }
    ),
    validate=True,
)
```

`OCEL.empty()` returns all five canonical empty tables. `append()` creates a
new lazy handle and can append any combination of tables:

```python
ocel = OCEL.empty().append(
    events=event_batch,
    objects=object_batch,
    object_changes=change_batch,
    e2o=relation_batch,
    validate=True,
)
```

Append does not deduplicate rows. Event and object-change batches may add
attribute columns; object and relation batches must use their canonical
schemas.

## Type checking

oceldb ships a `py.typed` marker. The main public typing helpers are available
from the top-level package:

```python
from oceldb import FrameLike, OneOrMany, PathLikeStr, TimeLike
```

Accessors return concrete `polars.LazyFrame` values, operations preserve
`OCEL`, and `flatten()` returns a `polars.LazyFrame`.

## Performance

The following results were measured on the BPIC 2017 OCEL:

| Dataset property | Value |
| --- | ---: |
| SQLite source size | 716.2 MB |
| Events | 1,202,267 |
| Objects | 106,162 |
| Object changes | 106,162 |
| E2O relations | 2,404,534 |
| O2O relations | 74,504 |
| Event types | 26 |
| Object types | 4 |
| Native snapshot size | 50.2 MB |

The native snapshot is approximately 14.3 times smaller than the SQLite
source.

Operation benchmark:

| Workload | Median | Peak RSS |
| --- | ---: | ---: |
| Open snapshot | 5.7 ms | 58.9 MiB |
| Full logical validation | 407.6 ms | 204.0 MiB |
| Full event scan | 36.4 ms | 70.9 MiB |
| Typed event scan | 8.2 ms | 68.9 MiB |
| Typed E2O scan | 10.7 ms | 71.9 MiB |
| Typed O2O scan | 7.3 ms | 68.6 MiB |
| Object-state reconstruction | 9.1 ms | 75.8 MiB |
| Event-type induced sublog | 71.5 ms | 82.3 MiB |
| Event related-object count filter | 427.9 ms | 244.1 MiB |
| Object related-event count filter | 496.9 ms | 210.7 MiB |
| Typed view | 109.6 ms | 89.9 MiB |
| Single-object projection | 163.1 ms | 88.1 MiB |
| Classical flattening | 72.6 ms | 144.3 MiB |
| Unchanged native snapshot copy | 39.7 ms | 60.2 MiB |
| Filtered native write | 164.1 ms | 115.8 MiB |

SQLite-to-native conversion with complete validation took 16.20 seconds at
443.7 MiB peak RSS.

These are warm-cache measurements on macOS arm64 with CPython 3.13.12, Polars
1.41.2, one Polars worker, three measured rounds, and one warm-up. Each
operation ran in a fresh process, included `OCEL.open()`, and fully consumed
its result. Conversion used one measured round and `batch_size=10_000`.

The numbers are evidence for this dataset and environment, not universal
latency or memory guarantees. Relation density, attribute width, type
cardinality, storage hardware, cache state, and thread count materially affect
results.

### Competitive comparison

The same BPIC 2017 source was compared with
[PM4Py 2.7.22](https://github.com/process-intelligence-solutions/pm4py) and
[r4pm 0.5.5](https://pypi.org/project/r4pm/). Each cell below contains median
end-to-end time and maximum peak RSS:

| Workload | oceldb | PM4Py | r4pm |
| --- | ---: | ---: | ---: |
| Ingest and summarize | 16.183 s / 164.6 MiB | 19.129 s / 3.0 GiB | 10.686 s / 4.7 GiB |
| Repeated load and summarize | 0.196 s / 81.1 MiB | 18.329 s / 3.0 GiB | 9.537 s / 4.7 GiB |
| Event-type induced sublog | 0.117 s / 82.0 MiB | 18.362 s / 3.0 GiB | — |
| Object-type induced sublog | 0.114 s / 92.8 MiB | 18.547 s / 3.0 GiB | — |
| Timestamp-induced sublog | 0.531 s / 145.2 MiB | 20.116 s / 3.0 GiB | — |
| Object-count induced sublog | 0.531 s / 313.8 MiB | 29.639 s / 3.0 GiB | — |
| Flatten on `Offer` | 0.075 s / 136.4 MiB | 18.024 s / 3.0 GiB | 6.603 s / 4.0 GiB |

End-to-end time includes opening oceldb's native snapshot or importing the
exchange file for a competitor. With the input representation ready, the
operation medians were:

| Workload | oceldb | PM4Py | r4pm |
| --- | ---: | ---: | ---: |
| Event-type induced sublog | 111 ms | 651 ms | — |
| Object-type induced sublog | 108 ms | 703 ms | — |
| Timestamp-induced sublog | 526 ms | 2.274 s | — |
| Object-count induced sublog | 526 ms | 11.876 s | — |
| Flatten on `Offer` | 70 ms | 179 ms | 663 ms |

r4pm was the fastest eager SQLite importer in this measurement. oceldb's
ingestion additionally created durable native storage and used substantially
less peak memory. Its conversion cost was recovered after approximately one
repeated access relative to PM4Py and two relative to r4pm.

r4pm 0.5.5 does not expose equivalent induced-sublog filters through its
Python API, so the benchmark leaves those cells empty rather than implementing
substitutes. Every supported workload completed without an error or timeout
and passed the canonical structural-result gate.

These measurements used macOS arm64, CPython 3.13.12, Polars 1.41.2, pandas
3.0.5, one worker thread, three measured rounds, one warm-up, and a five-minute
per-sample timeout. Measured ingestion used `batch_size=10_000` and disabled
oceldb's additional full logical-validation pass because neither competitor
performs an equivalent step. PM4Py and r4pm produced eager in-memory
representations; oceldb produced a durable native snapshot.

Reproduce the operation benchmark:

```bash
uv run python benchmarks/benchmark.py run /path/to/native-ocel \
  --rounds 3 \
  --warmups 1 \
  --threads 1 \
  --output /tmp/oceldb-benchmark.json
```

Reproduce conversion measurements:

```bash
uv run python benchmarks/conversion.py /path/to/source.sqlite \
  --format sqlite \
  --batch-size 10000 \
  --validate \
  --rounds 1 \
  --output /tmp/oceldb-conversion.json
```

Run the optional, correctness-gated competitor comparison:

```bash
uv sync --group benchmark

uv run --group benchmark python benchmarks/competitor_comparison.py \
  /path/to/source.sqlite \
  --native /path/to/native-ocel \
  --rounds 3 \
  --warmups 1 \
  --threads 1 \
  --timeout 300 \
  --output /tmp/oceldb-competitors.json
```

This comparison reports operation and end-to-end times separately, records
peak RSS and failures, and rejects completed workload pairs whose canonical
result summaries differ. It is intentionally separate from the internal
regression suite. See the benchmark documentation for the execution-model and
ingestion caveats.

See [benchmarks/README.md](benchmarks/README.md) and
[docs/benchmarks.md](docs/benchmarks.md) for profiles, workload definitions,
and comparison rules.

## Development

```bash
uv sync
uv run ruff check .
uv run basedpyright
uv run mypy
```

The implementation uses the `src/` layout:

```text
src/oceldb/
  ocel.py          # public immutable handle
  core/            # native storage, validation, states, SQL bridge
  io/              # exchange-format conversion
  operations/      # lazy filters and transformations
```

## License

MIT
