# oceldb

Polars-backed access to OCEL 2.0 event logs.

oceldb represents an object-centric event log as five lazy Polars tables:
events, objects, object changes, event-object relations, and object-object
relations. The native on-disk format is type-partitioned Parquet plus a
minimal versioned manifest.

The core is intentionally small: the `OCEL` value (five `LazyFrame`
accessors, `open`, `write`, `sql`, and `>>` piping) and the native layout.
Everything else — filters and transformations today, exchange-format
conversion later — is built as layers around that core.

## Installation

```bash
pip install oceldb
# or
uv add oceldb
```

Requires Python 3.11+.

## Public API

- `oceldb`: the `OCEL` class.
- `oceldb.operations`: filters plus `flatten`, `project`, `rename_types`,
  and `view`.
- `oceldb.core.schema`: the reserved OCEL column-name constants.

Everything else under `oceldb.core` and `oceldb.operations` is
implementation detail and may change.

## Quick Start

```python
from oceldb import OCEL
from oceldb.operations import filter_events_by_time, view

ocel = OCEL.open("my-log")

event_counts = (
    ocel.events()
    .group_by("ocel_type")
    .len()
    .sort("len", descending=True)
    .collect()
)

recent_orders = (
    ocel
    >> view(object_types="order")
    >> filter_events_by_time(start="2024-01-01")
)
recent_orders.write("recent-orders", overwrite=True)
```

## The OCEL API

`OCEL` is a lightweight, immutable handle around five lazy frames. Accessors
return `polars.LazyFrame`; execution begins when you call `collect()`,
`sink_parquet()`, or another Polars execution method.

```python
ocel.events()                 # all events
ocel.events("Place Order")    # selected event types only

ocel.objects()                # all object identities
ocel.objects("order")         # selected object types only

ocel.object_changes("order")  # sparse object attribute changes

ocel.e2o()                    # event-to-object relations
ocel.o2o()                    # object-to-object relations

ocel.sql("SELECT count(*) FROM events")   # eager DuckDB SQL over the tables
```

`sql()` registers the five tables as DuckDB views named `events`, `objects`,
`object_changes`, `e2o`, and `o2o`, and returns an eager `polars.DataFrame`.

### Opening and writing native datasets

`OCEL.open(...)` validates the minimal versioned manifest and the required
physical tables before constructing lazy scans; row data is not loaded.
`write(...)` stages the complete directory next to the target and renames it
into place, so a failed write never leaves a half-written dataset.

```python
ocel = OCEL.open("my-log")
ocel.write("copy-of-my-log")
ocel.write("copy-of-my-log", overwrite=True)
```

The writer partitions each of the three typed tables by `ocel_type`, keeps
only the attribute columns that actually carry values per type, sorts each
file for row-group pruning, and compresses with ZSTD. See
[docs/storage-format.md](docs/storage-format.md) for the full layout
contract, including how to read a log without oceldb.

### Manual construction

The constructor takes five explicitly named lazy frames and trusts them; it
performs no integrity checks.

```python
from datetime import datetime, timezone

import polars as pl
from oceldb import OCEL

ocel = OCEL(
    events=pl.LazyFrame(
        {
            "ocel_id": ["e1"],
            "ocel_time": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "ocel_type": ["Place Order"],
            "amount": [42.0],
        }
    ),
    objects=pl.LazyFrame({"ocel_id": ["o1"], "ocel_type": ["order"]}),
    object_changes=pl.LazyFrame(
        {
            "ocel_id": ["o1"],
            "ocel_time": [datetime(2024, 1, 1, tzinfo=timezone.utc)],
            "ocel_changed_field": pl.Series([None], dtype=pl.String),
            "ocel_is_initial": [True],
            "status": ["created"],
            "ocel_type": ["order"],
        }
    ),
    e2o=pl.LazyFrame(
        {
            "ocel_event_id": ["e1"],
            "ocel_event_type": ["Place Order"],
            "ocel_object_id": ["o1"],
            "ocel_object_type": ["order"],
            "ocel_qualifier": ["order"],
        }
    ),
    o2o=pl.LazyFrame(
        schema={
            "ocel_source_id": pl.String,
            "ocel_source_type": pl.String,
            "ocel_target_id": pl.String,
            "ocel_target_type": pl.String,
            "ocel_qualifier": pl.String,
        }
    ),
)
ocel.write("manual-log", overwrite=True)
```

## Operations

Operations live in `oceldb.operations`. They take an `OCEL`, return a new
`OCEL` (or a derived `LazyFrame`), and never mutate their input. Every
operation supports two equivalent call styles; the preferred usage is `>>`,
which composes naturally into pipelines:

```python
from oceldb.operations import filter_objects_by_type, filter_events_by_time

branches_and_users = filter_objects_by_type(ocel, "branches", "users")

recent = (
    ocel
    >> filter_objects_by_type("branches", "users")
    >> filter_events_by_time(start="2024-01-01", end="2024-06-30")
)
```

Filters prune the connected core of the log: removed events, removed
objects, and relations pointing to removed rows are dropped together.

Two keyword arguments are consistent across the predicate filters:

- **scope** — `event_types=` on event filters and `object_types=` on object
  filters restricts *which* types the filter applies to. Rows of other types
  always pass through unchanged. `None` (the default) applies the filter to
  every type.
- **`mode`** — `"include"` (default) keeps the matching rows; `"exclude"`
  keeps the complement (within the scope).

A nullable predicate result is treated as no match: include mode drops null
results, exclude mode retains them. Invalid modes and bounds raise
immediately instead of failing later during Polars execution.

Event filters:

```python
import polars as pl
from oceldb.operations import (
    filter_events_by_attribute,
    filter_events_by_id,
    filter_events_by_object_count,
    filter_events_by_time,
    filter_events_by_type,
)

paid = ocel >> filter_events_by_type("Pay Order")
selected = ocel >> filter_events_by_id("e-001", "e-042")
large = ocel >> filter_events_by_attribute(
    pl.col("amount") >= 1000, event_types="Pay Order"
)
multi = ocel >> filter_events_by_object_count(min_count=2, object_types="item")
q1 = ocel >> filter_events_by_time(start="2024-01-01", end="2024-03-31")
```

Time bounds accept ISO 8601 strings, `date`, or `datetime` values; naive
values are interpreted as UTC. Count filters count distinct related rows,
include zero-count rows, and require at least one non-negative bound.

Object filters:

```python
from oceldb.operations import (
    filter_objects_by_attribute,
    filter_objects_by_event_count,
    filter_objects_by_id,
    filter_objects_by_o2o_count,
    filter_objects_by_type,
)

orders = ocel >> filter_objects_by_type("order", "item")
expensive = ocel >> filter_objects_by_attribute(
    pl.col("price") > 100, object_types="order", when="sometimes"
)
frequent = ocel >> filter_objects_by_event_count(min_count=3)
bundled = ocel >> filter_objects_by_o2o_count(
    min_count=1, related_types="item", direction="out"
)
```

`filter_objects_by_attribute(..., when=...)` evaluates the predicate on the
forward-filled object state history: `"sometimes"` requires at least one
matching state, `"always"` every recorded state, and a timestamp selects the
last known state at or before that instant. Objects explicitly selected by
an object filter remain in the result even when they have no event relation.

Relation and sampling filters:

```python
from oceldb.operations import (
    filter_e2o_by_qualifier,
    filter_o2o_by_qualifier,
    sample_events,
    sample_objects,
)

delivered = ocel >> filter_e2o_by_qualifier("receives")   # prunes the core
containment = ocel >> filter_o2o_by_qualifier("contains") # trims O2O edges only
preview = ocel >> sample_events(1000, seed=0)
tenth = ocel >> sample_objects(fraction=0.1, seed=0)
```

Transformations:

```python
from oceldb.operations import flatten, project, rename_types, view

# Restrict to selected event and/or object types (composes the type filters)
orders_view = ocel >> view(object_types=["order", "item"], event_types=["Pay Order"])

# Every event involving the given object(s), plus their co-participating objects
around_order = ocel >> project("order-42")

# Relabel type names consistently across every table
renamed = ocel >> rename_types(events={"place order": "Place Order"})

# Classical XES-style event log for one object type (a LazyFrame)
log = (ocel >> flatten("order")).collect()
```

`flatten` projects the log onto one object type as the case notion: static
object attributes become `case:<attribute>` columns, changing attributes are
forward-filled to their value as of each event, and event payload attributes
are carried along. Output-column collisions raise `ValueError` before the
result is constructed.

## Native Storage Layout

```text
my-log/
  manifest.json
  events/
    ocel_type=Place%20Order/data.parquet
  objects/
    ocel_type=order/data.parquet
  object_changes/
    ocel_type=order/data.parquet
  e2o.parquet
  o2o.parquet
```

Type names are URL-encoded in directory names. The `ocel_type` column is
re-attached when reading, so per-type Parquet files only store ids,
timestamps, and that type's attribute columns. The full contract is
documented in [docs/storage-format.md](docs/storage-format.md).

## Development

```text
src/oceldb/
  ocel.py          # the OCEL value: five accessors, open, write, sql, >>
  core/            # native layout (open/write), column constants, SQL bridge
  operations/      # filters, transformations, and their shared machinery
```

```bash
uv run pytest
uv run ruff check .
uv run basedpyright
```

MIT
