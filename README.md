# oceldb

**DuckDB-backed dataframe layer for OCEL 2.0.**

oceldb exposes object-centric event logs through a typed, lazy dataframe API
backed by DuckDB. Ibis is used internally, but queries are built through
oceldb's typed expression wrappers. Logs can be stored as typed Parquet files
or loaded into ephemeral in-memory storage. Nothing is materialised into
Python objects until you explicitly call `.execute()`.

## Installation

```bash
pip install oceldb
# or
uv add oceldb
```

Requires Python 3.12+.

## Quick start

```python
from oceldb import OCEL, col, desc
from oceldb.io import convert_ocel

# Convert an OCEL 2.0 JSON, XML, or SQLite file to the oceldb Parquet layout
convert_ocel("running-example.jsonocel", "running-example", overwrite=True)

ocel = OCEL.read("running-example")

# Event counts by type
event_counts = (
    ocel.events()
    .group_by("ocel_type")
    .aggregate(n=col("ocel_id").count())
    .order_by(desc("n"))
    .execute()
)

# Latest state of each order
latest_states = ocel.object_states("order").latest().execute()

# Filter objects using a predicate
from oceldb.predicates import participated_in
paid_orders = ocel.objects("order").filter(participated_in(ocel, "Pay Order"))
print(paid_orders.count().execute())
```

For applications that do not need persistent oceldb output, load the source
directly into temporary DuckDB storage:

```python
with OCEL.load("running-example.jsonocel") as ocel:
    latest_states = ocel.object_states("order").latest().execute()
```

## Core concepts

[OCEL 2.0](https://www.ocel-standard.org/) extends flat event logs with
multiple interrelated object types. An order-fulfilment process, for example,
involves orders, items, packages, and payments — each with their own lifecycle.
oceldb stores this structure natively.

### The `OCEL` class

`OCEL.read(path)` opens a persisted Parquet log directory. `OCEL.load(source)`
imports JSON, XML, SQLite, or a registered custom source into in-memory
DuckDB tables without writing an oceldb directory. Each returns an `OCEL`
instance backed by lazy `oceldb.Table` expressions.

```python
ocel = OCEL.read("my-log")
# or:
ocel = OCEL.load("source.jsonocel")

ocel.events()                   # all events: ocel_id, ocel_type, ocel_time, …attrs
ocel.events("Place Order")      # filtered to one type
ocel.events("A", "B")           # filtered to multiple types

ocel.objects()                  # all objects: ocel_id, ocel_type
ocel.objects("order")           # filtered to one type

ocel.object_changes("order")    # sparse history rows for object type
ocel.object_states("order")     # fill-forward state view (see below)
ocel.flatten("order")           # case-centric event log with orders as cases

ocel.event_object               # E2O bridge: event_id, event_type, object_id, object_type, qualifier
ocel.object_object              # O2O bridge: source_id, source_type, target_id, target_type, qualifier

ocel.manifest                   # parsed manifest.json (counts, types, time range)
```

### Object states

Attributes in OCEL 2.0 are stored as change rows. `object_states` reconstructs
point-in-time snapshots via fill-forward:

```python
from datetime import datetime

states = ocel.object_states("order")

states.history()          # all change rows in chronological order
states.latest()           # most recent state per object
states.as_of(datetime(2024, 6, 1))  # state at a specific timestamp
```

### Flattening

Flatten an OCEL to a traditional case-centric event log by choosing one object
type as the case notion:

```python
flat_orders = ocel.flatten("order").execute()
```

The flattened table follows the PM4Py/XES naming convention:

- `case:concept:name`: object id of the selected object type
- `concept:name`: event activity, from `ocel_type`
- `time:timestamp`: event timestamp, from `ocel_time`
- `ocel_event_id`: original OCEL event id
- event attributes are preserved as additional columns

### Typed expressions

All accessors return `oceldb.Table` expressions. Use the wrappers exported by
`oceldb` for expression construction; do not compose queries with Ibis
objects directly. The wrappers keep the supported expression surface typed
despite Ibis' dynamic typing internals.

```python
from oceldb import col, desc, row_number

# Latest object states with a specific attribute value
heavy = ocel.object_states("Container").latest().filter(
    col("Weight") > 500
)

# Event timeline per flattened case with lag/lead
flat = ocel.flatten("order")
timeline = flat.mutate(
    seq=row_number().over(
        group_by="case:concept:name", order_by=["time:timestamp", "ocel_event_id"]
    ),
    previous=col("concept:name").lag().over(
        group_by="case:concept:name", order_by=["time:timestamp", "ocel_event_id"]
    ),
    next=col("concept:name").lead().over(
        group_by="case:concept:name", order_by=["time:timestamp", "ocel_event_id"]
    ),
).order_by("case:concept:name", "time:timestamp").execute()

# Aggregation and ordering
counts = (
    ocel.events()
    .group_by("ocel_type")
    .aggregate(n=col("ocel_id").count())
    .order_by(desc("n"))
    .execute()
)
```

The typed surface includes `Table.filter`, `select`, `mutate`, `drop`,
`rename`, `distinct`, `limit`, `order_by`, `group_by`, `join`, `execute`,
and `to_pyarrow`; column comparisons and aggregations; and the `col`, `asc`,
`desc`, `row_number`, and `union` helpers.

## Predicates

`oceldb.predicates` provides free-function predicates for OCEL-specific filter
expressions. All return deferred boolean expressions for use with
`oceldb.Table.filter()`.

### Object predicates

```python
from oceldb.predicates import (
    participated_in,     # object participated in ≥1 event of a given type
    event_count,         # count of events of a given type per object
    cooccurrence_count,  # count of co-occurring objects of a given type
    o2o_count,           # count of O2O-linked objects of a given type
    o2o_reachable,       # reachability via O2O relations
)

# Orders that were paid
paid = ocel.objects("order").filter(participated_in(ocel, "Pay Order"))

# Orders with more than 3 items co-occurring
busy = ocel.objects("order").filter(cooccurrence_count(ocel, "item") >= 3)

# Objects with exactly two linked children
ocel.objects("order").filter(o2o_count(ocel, "item") == 2)

# Objects linked (directly or transitively) to a Transport Document
ocel.objects("Container").filter(o2o_reachable(ocel, "Transport Document"))
```

### Event predicates

```python
from oceldb.predicates import (
    involves,                  # event involves ≥1 object of a given type
    has_matching_predecessor,  # batch-synchronisation check
)

# Events that involve at least one item
ocel.events().filter(involves(ocel, "item"))

# Sync events that have a matching preceding Group event
matched = ocel.events("Sync").filter(
    has_matching_predecessor(ocel, "Group", "member")
)
```

### Count predicates

`event_count`, `cooccurrence_count`, `o2o_count` return a `CountPredicate`
object that supports threshold comparisons:

```python
event_count(ocel, "Pay Order") >= 1   # participated at least once
event_count(ocel, "Pay Order") == 1   # participated exactly once
event_count(ocel, "Pay Order") == 0   # never participated
```

## Inspection

`oceldb.inspect` provides structural summaries read directly from the manifest
(no SQL):

```python
from oceldb.inspect import overview, event_types, object_types

print(overview(ocel))
# Events:        35,413  (14 types)
# Objects:       13,910  (7 types)
# E2O relations: 74,334
# O2O relations: 15,953
# Time range:    2023-05-22 11:54:42  →  2024-08-22 12:18:41

for et in event_types(ocel):      # sorted by count descending
    print(et.name, et.count, et.attributes)

for ot in object_types(ocel):     # sorted by count descending
    print(ot.name, ot.object_count, ot.attributes)
```

## Import

Convert an OCEL 2.0 exchange format to the oceldb Parquet layout:

```python
from oceldb.io import convert_ocel

convert_ocel("source.jsonocel", "target/", overwrite=True)   # JSON
convert_ocel("source.xml",      "target/", overwrite=True)   # XML
convert_ocel("source.sqlite",   "target/", overwrite=True)   # SQLite
```

The format is inferred from common OCEL file extensions (`.json`, `.jsonocel`,
`.xml`, `.xmlocel`, `.db`, `.sqlite`, `.sqlite3`). Pass `format="json"` /
`"xml"` / `"sqlite"` when the filename does not expose the format.

Custom sources can be registered through the shared converter registry:

```python
from oceldb import OCEL
from oceldb.io import ConverterSpec, register

register(ConverterSpec(format="myformat", source_factory=MySource))
convert_ocel(source, target, format="myformat")
# or:
ocel = OCEL.load(source, format="myformat")
```

## Storage layout

```
my-log/
  manifest.json                           # schema, provenance, totals
  events/
    ocel_type=<url-encoded-type>/
      data.parquet                        # ocel_id, ocel_time, …attrs; sorted by ocel_time
  objects/
    ocel_type=<url-encoded-type>/
      data.parquet                        # ocel_id
  object_changes/
    ocel_type=<url-encoded-type>/
      data.parquet                        # ocel_id, ocel_time, ocel_changed_field, …attrs
  event_object.parquet                    # E2O bridge with denormalised type columns
  object_object.parquet                   # O2O bridge with denormalised type columns (if present)
```

Type names with spaces or special characters are URL-encoded in directory names
(e.g. `ocel_type=Place%20Order`). DuckDB uses Hive partition pruning to skip
files when filtering by type.

## Type checking

Ibis' expression types are dynamic and produce pervasive errors in strict
pyright/basedpyright projects. For that reason, Ibis is an implementation
detail: public query methods return `oceldb.Table`, and supported query
operations are exposed through the wrappers in `oceldb.expr`.

```python
from oceldb import OCEL, col, desc

with OCEL.load("source.jsonocel") as ocel:
    result = (
        ocel.events("Place Order")
        .filter(col("amount") > 100)
        .order_by(desc("ocel_time"))
        .execute()
    )
```

Keep expressions within this typed API. Importing Ibis expressions directly or
operating on the underlying raw backend bypasses the wrapper layer and brings
back the typing problems it is intended to isolate.

## License

MIT
