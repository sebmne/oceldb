# oceldb

Polars-backed access to OCEL 2.0 event logs.

oceldb represents an object-centric event log as five lazy Polars tables:
events, objects, object changes, event-object relations, and object-object
relations. The native on-disk format is Parquet, split by event or object type
for compact storage and fast type-filtered scans. OCEL 2.0 SQLite exports are
imported explicitly into that native layout.

Core table accessors return `polars.LazyFrame`; execution normally begins when
you call `collect()`, `sink_parquet()`, or another Polars execution method.
Type-specific event/change accessors perform a small eager presence query to
remove attribute columns that are null for every selected row.

## Installation

```bash
pip install oceldb
# or
uv add oceldb
```

Requires Python 3.11+.

## Quick Start

```python
from oceldb.io import import_sqlite, open_ocel

import_sqlite("running-example.sqlite", "running-example")
ocel = open_ocel("running-example")

event_counts = (
    ocel.events()
    .group_by("ocel_type")
    .len()
    .sort("len", descending=True)
    .collect()
)

orders = ocel.objects("order").collect()

latest_order_states = (
    ocel.object_states("order")
    .sort("ocel_type", "ocel_id", "ocel_time")
    .unique(subset=["ocel_type", "ocel_id"], keep="last")
    .collect()
)
```

## Opening and Importing Logs

Use `open_ocel(...)` for an existing native dataset. Opening is lazy: it reads
the small manifest and constructs Parquet scans without loading row data.

```python
from oceldb.io import open_ocel

ocel = open_ocel("native-log")
```

Use `import_ocel(...)` to convert an OCEL 2.0 exchange file into reusable
native storage. The returned log is opened lazily from the new native dataset.
Under the default strict validation, JSON and XML records are parsed
incrementally into typed Parquet batches; SQLite is converted directly through
DuckDB. Permissive `warn` and `none` imports currently use the eager fallback.

```python
from oceldb.io import import_ocel

ocel = import_ocel("log.jsonocel", "native-log")
ocel = import_ocel("log.xmlocel", "native-log", overwrite=True)
ocel = import_ocel("log.sqlite", "native-log", overwrite=True)
```

Exchange readers validate declarations, values, timestamps, relationships, and
identifiers by default. Use `validation="warn"` to recover usable data while
emitting warnings, or `validation="none"` for trusted inputs.

`OCEL.open(...)` is the class-level equivalent of `open_ocel(...)`:

```python
from oceldb import OCEL

ocel = OCEL.open("converted-log")
```

SQLite is import-only because it cannot produce Polars lazy frames directly.
Import it into native storage, then open that dataset:

```python
from oceldb.io import import_sqlite

import_sqlite("source.sqlite", "converted-log", overwrite=True)
ocel = OCEL.open("converted-log")
```

`read_json`, `read_xml`, and `from_pm4py` build an **in-memory** `OCEL` from the
standard OCEL 2.0 exchange formats:

```python
from oceldb.io import from_pm4py, read_json, read_xml

ocel = read_json("log.jsonocel")
ocel = read_xml("log.xmlocel")

import pm4py
ocel = from_pm4py(pm4py.read_ocel2_xml("log.xmlocel"))  # OCEL 2.0 only
```

These parse the whole file into memory. For larger logs, use `import_ocel()` or
the format-specific importers to produce file-backed native frames.

## The OCEL API

`OCEL` is a lightweight handle around five lazy frames.

```python
ocel.events()                 # all events
ocel.events("Place Order")    # selected event types only

ocel.objects()                # all object identities
ocel.objects("order")         # selected object types only

ocel.object_changes("order")  # sparse object attribute changes
ocel.object_states("order")   # forward-filled object state history

ocel.event_object()           # event-to-object relations
ocel.object_object()          # object-to-object relations
```

When you pass type names to `events(...)`, `object_changes(...)`, or
`object_states(...)`, oceldb filters the rows and omits attribute columns that
are entirely null for the selected types. This keeps type-specific queries
smaller and easier to inspect.

## Filtering and Pipelines

Filtering operations live in `oceldb.filters`. They take an `OCEL`, return a
new `OCEL`, and prune the connected core of the log: removed events, removed
objects, and relations pointing to removed rows are dropped together.

Every filter supports two equivalent call styles:

```python
from oceldb.filters import filter_objects_by_type, filter_events_by_time

branches_and_users = filter_objects_by_type(ocel, "branches", "users")

recent_branches_and_users = (
    ocel
    >> filter_objects_by_type("branches", "users")
    >> filter_events_by_time(start="2024-01-01", end="2024-06-30")
)
```

The preferred usage is `>>` as it easily allows for extending a single filter
to a filter pipeline.

Two keyword arguments are consistent across the predicate filters:

- **scope** — `event_types=` on event filters and `object_types=` on object
  filters restricts *which* types the filter applies to. Rows of other types
  always pass through unchanged. `None` (the default) applies the filter to
  every type.
- **`mode`** — `"include"` (default) keeps the matching rows; `"exclude"` keeps
  the complement (within the scope).

```python
# Drop only the "pay order" events after a cutoff; every other event is kept
ocel >> filter_events_by_time(
    start="2024-01-01", event_types="pay order", mode="exclude"
)
```

`filter_events_by_type` / `filter_objects_by_type` take only `mode` (types are
their subject), and `sample_events` / `sample_objects` take neither.

Event filters:

```python
import polars as pl
from oceldb.filters import (
    filter_events_by_attribute,
    filter_events_by_id,
    filter_events_by_object_count,
    filter_events_by_time,
    filter_events_by_type,
)

paid = ocel >> filter_events_by_type("Pay Order")
without_cancellations = ocel >> filter_events_by_type("Cancel Order", mode="exclude")
selected_events = ocel >> filter_events_by_id("e-001", "e-042")
large_payments = ocel >> filter_events_by_attribute(
    pl.col("amount") >= 1000,
    event_types="Pay Order",
)
multi_object_events = ocel >> filter_events_by_object_count(
    min_count=2,
    object_types="item",
)
first_quarter = ocel >> filter_events_by_time(start="2024-01-01", end="2024-03-31")
```

Object filters:

```python
import polars as pl
from oceldb.filters import (
    filter_objects_by_attribute,
    filter_objects_by_event_count,
    filter_objects_by_id,
    filter_objects_by_o2o_count,
    filter_objects_by_type,
)

orders_and_items = ocel >> filter_objects_by_type("order", "item")
without_test_objects = ocel >> filter_objects_by_id("test-order-1", mode="exclude")
expensive_orders = ocel >> filter_objects_by_attribute(
    pl.col("price") > 100,
    object_types="order",
    when="sometimes",
)
frequent_objects = ocel >> filter_objects_by_event_count(
    min_count=3,
    event_types="Pay Order",
)
bundled_orders = ocel >> filter_objects_by_o2o_count(
    min_count=1,
    related_types="item",
    direction="out",
)
```

`filter_objects_by_attribute(..., when=...)` evaluates predicates on
`object_states()`. Use `"sometimes"` for at least one matching state,
`"always"` for every recorded state, or a timestamp string for the last known
state at or before that time.

Relation and sampling filters:

```python
from oceldb.filters import (
    filter_e2o_by_qualifier,
    filter_o2o_by_qualifier,
    sample_events,
    sample_objects,
)

# Keep only event-to-object relations with a given qualifier (prunes the core)
delivered = ocel >> filter_e2o_by_qualifier("receives")
# Trim object-to-object edges by qualifier (objects/events are left untouched)
containment = ocel >> filter_o2o_by_qualifier("contains")
# Random sub-logs for previews or quick experiments
preview = ocel >> sample_events(1000, seed=0)
tenth = ocel >> sample_objects(fraction=0.1, seed=0)
```

## Transformations

`oceldb.transformations` derives new tables or sub-logs from an `OCEL`. Like
filters, each can be called directly or as a `>>` pipe step.

`view` and `project` return a new `OCEL` (a sub-log with the connected core
pruned):

```python
from oceldb.transformations import view, project

# Restrict to selected event and/or object types
orders_view = ocel >> view(object_types=["order", "item"], event_types=["Pay Order"])

# Every event involving the given object(s), plus their co-participating objects
around_order = ocel >> project("order-42")
```

`flatten` projects the log onto one object type as a classical XES-style event
log (a `LazyFrame`) — the standard input for control-flow discovery:

```python
from oceldb.transformations import flatten, collect

log = ocel >> flatten("order") >> collect()
```

Its columns are:

- `case:concept:name` (object id / case), `concept:name` (activity),
  `time:timestamp` (event time).
- `case:<attribute>` — one per object attribute that is **static** (never takes
  more than one value for *any* object of the type). These are genuine case
  attributes, constant within each case.
- `<attribute>` — one per object attribute that **changes** for some object,
  holding its value *as of that event* (event-level, forward-filled).
- `<attribute>` — one per event payload attribute.
- `ocel_event_id`.

An object attribute and an event attribute sharing a name would be ambiguous in
a flattened row, so `flatten` raises `ValueError` in that case — rename one side
first (e.g. with `rename_types`, below).

`case_table` summarizes each object as a single row — lifecycle span, event
count, first/last activity, and last-known attribute values — a feature table
for machine learning and decision mining:

```python
from oceldb.transformations import case_table

features = ocel >> case_table(object_types="order") >> collect()
```

`collect()` is a terminal pipe step that materializes the preceding lazy frame,
so an entire pipeline reads as one expression.

## Log Operations

Beyond filtering, oceldb ships the operations you almost always need after
importing or before exporting a log.

Check and repair referential integrity (readers and manual construction do not
guarantee it):

```python
from oceldb.validation import validate, clean

report = validate(ocel)
if not report.is_valid:
    ocel = ocel >> clean()  # drop dangling relations, dedupe ids, sort events
```

Relabel type names consistently across every table:

```python
from oceldb.transformations import rename_types

ocel = ocel >> rename_types(
    events={"place order": "Place Order"},
    objects={"orders": "Order"},
)
```

Combine several logs, and inspect a log programmatically:

```python
from oceldb import OCEL

combined = OCEL.merge(ocel_a, ocel_b)   # union, de-duplicating shared ids

summary = ocel.describe()               # counts, per-type counts, time span
summary.events, summary.event_types, summary.start_time, summary.end_time
```

## Exporting

Use `export_ocel(...)` when the operation is specifically native-to-exchange.
It accepts either an `OCEL` or the path of a native dataset:

```python
from oceldb.io import export_ocel

export_ocel(ocel, "out.jsonocel")
export_ocel("native-log", "out.xmlocel")
export_ocel("native-log", "out.sqlite")
```

JSON, XML, and SQLite exports retain declared types, attribute schemas, object
changes, E2O/O2O qualifiers, and time-valued attributes. The format-specific
writers remain available:

```python
from oceldb.io import write_json, write_sqlite, write_xes, write_xml
from oceldb.transformations import flatten

ocel.write("my-log", overwrite=True)                # native Parquet directory
write_json(ocel, "out.jsonocel", overwrite=True)    # OCEL 2.0 JSON
write_xml(ocel, "out.xmlocel", overwrite=True)      # OCEL 2.0 XML
write_sqlite(ocel, "out.sqlite", overwrite=True)    # OCEL 2.0 SQLite
write_xes(ocel >> flatten("order"), "orders.xes")   # a flattened log to XES
```

`write_xes` takes the output of `flatten`; `case:*` columns become trace
attributes and the remaining columns become event attributes. Codec
capabilities are inspectable through `exchange_codecs()`; `XES_CAPABILITIES`
explicitly records that XES requires a case notion and is not a lossless OCEL
encoding.

OCEL 2.0 formats declare event and object attribute types independently from
their instances. Exchange readers preserve this information as `ocel.schema`.
Manually constructed logs can provide an `OCELSchema`; when it is omitted,
writers infer declarations from observed Polars columns.

## Manual Construction

You can build an `OCEL` from explicitly named Polars lazy frames with
`OCEL.from_frames()`. It trusts the supplied schema and does not validate
dangling relations or sort rows.

```python
from datetime import datetime

import polars as pl
from oceldb import OCEL

events = pl.DataFrame(
    {
        "ocel_id": ["e1"],
        "ocel_time": [datetime(2024, 1, 1)],
        "ocel_type": ["Place Order"],
        "amount": [42.0],
    }
).lazy()

objects = pl.DataFrame(
    {"ocel_id": ["o1"], "ocel_type": ["order"]}
).lazy()

object_changes = pl.DataFrame(
    {
        "ocel_id": ["o1"],
        "ocel_time": [datetime(1970, 1, 1)],
        "ocel_changed_field": [None],
        "status": ["created"],
        "ocel_type": ["order"],
    }
).lazy()

e2o = pl.DataFrame(
    {
        "ocel_event_id": ["e1"],
        "ocel_event_type": ["Place Order"],
        "ocel_object_id": ["o1"],
        "ocel_object_type": ["order"],
        "ocel_qualifier": [None],
    }
).lazy()

o2o = pl.DataFrame(
    schema={
        "ocel_source_id": pl.String,
        "ocel_source_type": pl.String,
        "ocel_target_id": pl.String,
        "ocel_target_type": pl.String,
        "ocel_qualifier": pl.String,
    }
).lazy()

ocel = OCEL.from_frames(
    events=events,
    objects=objects,
    object_changes=object_changes,
    event_object=e2o,
    object_object=o2o,
)
ocel.write("manual-log", overwrite=True)
```

## Native Storage Layout

```text
my-log/
  manifest.json
  events/
    ocel_type=Place%20Order/
      data.parquet
  objects/
    ocel_type=order/
      data.parquet
  object_changes/
    ocel_type=order/
      data.parquet
  event_object.parquet
  object_object.parquet
```

Type names are URL-encoded in directory names. The `ocel_type` column is
re-attached when reading, so per-type Parquet files only store ids, timestamps,
relation fields, and custom attributes.

## Development

The package root intentionally contains only the public `OCEL` implementation
and exports. Supporting code is grouped by responsibility:

```text
src/oceldb/
  ocel.py
  schema/          # column constants and declared OCEL type metadata
  core/            # frame, state, inspection, SQL, and pruning services
  io/
    native/        # manifested Parquet storage and batch writing
    exchange/      # lossless codec operations supported by each format
    integrations/  # optional third-party bridges such as PM4Py
    exports/       # derived/lossy formats such as XES
  validation/      # integrity reports and repairs
  filters/
  transformations/
  utils/
```

```bash
uv run ruff check .
uv run basedpyright
uv run pytest
```

MIT
