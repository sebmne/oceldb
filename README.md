# oceldb

Polars-backed access to OCEL 2.0 event logs.

oceldb represents an object-centric event log as five lazy Polars tables:
events, objects, object changes, event-object relations, and object-object
relations. The native on-disk format is Parquet, split by event or object type
for compact storage and fast type-filtered scans. OCEL 2.0 SQLite exports can
be converted to that layout or opened through a conversion cache.

All query methods return `polars.LazyFrame`. Nothing is materialized until you
call `collect()`, `sink_parquet()`, or another Polars execution method.

## Installation

```bash
pip install oceldb
# or
uv add oceldb
```

Requires Python 3.11+.

## Quick Start

```python
from oceldb.io import read_sqlite

ocel = read_sqlite("running-example.sqlite")

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

## Opening Logs

Use `OCEL.read(...)` for an existing native oceldb directory:

```python
from oceldb import OCEL

ocel = OCEL.read("converted-log")
```

Use `read_sqlite(...)` for an OCEL 2.0 SQLite export. The first call converts
the SQLite file to a native Parquet directory under the OS cache directory.
Repeated reads reuse the cache while the source path, file size, and modified
time stay unchanged.

```python
from oceldb.io import read_sqlite

ocel = read_sqlite("source.sqlite")
```

Use `convert_sqlite(...)` when you want to persist the converted directory
yourself:

```python
from oceldb.io import convert_sqlite

convert_sqlite("source.sqlite", "converted-log", overwrite=True)
ocel = OCEL.read("converted-log")
```

## The OCEL API

`OCEL` is a lightweight handle around five lazy frames. It does not own a
database connection and does not need to be used as a context manager.

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

## Manual Construction

You can build an `OCEL` directly from Polars lazy frames. The constructor trusts
the supplied schema and does not validate dangling relations or sort rows.

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

ocel = OCEL(events, objects, object_changes, o2o, e2o)
ocel.write("manual-log", overwrite=True)
```

## Native Storage Layout

```text
my-log/
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

```bash
uv run ruff check .
uv run basedpyright
uv run pytest
```

MIT
