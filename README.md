# oceldb

**DuckDB-backed OCEL 2.0 storage and lazy query DSL.**

oceldb keeps object-centric event logs on disk as typed Parquet files and exposes
a fluent, dataframe-like DSL for filtering, aggregation, sequence analysis, and
sublog extraction -- all compiled to SQL and executed lazily by DuckDB.

## Features

- **On-disk by default** -- logs stay in Parquet; nothing is materialized into Python objects until you ask.
- **Lazy query DSL** -- Polars-like API that compiles to SQL. Filter, group, window, and aggregate without writing SQL.
- **OCEL-aware semantics** -- first-class support for object states, event occurrences, relation predicates, and sublog extraction.
- **Process discovery** -- built-in OC-DFG (Object-Centric Directly-Follows Graph) mining.
- **Inspection helpers** -- one-call summaries of types, attributes, time ranges, and relation statistics.
- **Raw SQL escape hatch** -- drop into DuckDB SQL whenever you need full control.
- **Minimal dependencies** -- only [DuckDB](https://duckdb.org).

## Installation

```bash
pip install oceldb
```

or with [uv](https://docs.astral.sh/uv/):

```bash
uv add oceldb
```

Requires Python 3.12+.

Optional PM4Py interoperability:

```bash
pip install "oceldb[pm4py]"
```

## Quick start

```python
from oceldb import OCEL, col, count, has_event
from oceldb.io import convert_sqlite

# Convert an OCEL 2.0 SQLite database to the oceldb Parquet format
convert_sqlite("running-example.sqlite", "running-example", overwrite=True)

with OCEL.read("running-example") as ocel:
    # Find all orders that were paid
    paid_orders = (
        ocel.query
        .objects("order")
        .where(has_event("Pay Order").exists())
    )
    print(paid_orders.count())  # 3

    # Event counts by type
    event_counts = (
        ocel.query
        .events()
        .group_by("ocel_type")
        .agg(count().alias("n"))
        .sort("n", descending=True)
    )
    print(event_counts.collect())

    # Latest state of open orders
    open_orders = (
        ocel.query
        .object_states("order")
        .latest()
        .where(col("status") == "open")
    )
    print(open_orders.count())
```

## Core concepts

[OCEL 2.0](https://www.ocel-standard.org/) (Object-Centric Event Logs) extends
traditional flat event logs with multiple interrelated object types. An order
fulfillment process, for example, involves orders, items, packages, and
payments -- each with their own lifecycle. oceldb embraces this structure
natively.

### Query roots

`ocel.query` is the stable entry point. Each root selects a different analytical
grain:

| Root | Row grain | Use case |
|------|-----------|----------|
| `events(...)` | One row per event | Event-level analysis |
| `objects(...)` | One row per object identity | Object selection, relation predicates |
| `object_states(...).latest()` | Latest reconstructed state per object | Current attribute snapshots |
| `object_states(...).as_of(t)` | State at timestamp *t* | Point-in-time analysis |
| `object_changes(...)` | One row per raw attribute update | Change history |
| `event_occurrences(...)` | One row per event-object incidence | Sequence and process analysis |
| `event_objects()` | Raw event-to-object relations | Low-level joins |
| `object_objects()` | Raw object-to-object relations | Low-level joins |

The key distinction:

- **`objects(...)`** -- identity-oriented, no temporal state
- **`object_changes(...)`** -- sparse history rows exactly as stored
- **`object_states(...)`** -- fill-forward reconstructed snapshots for analysis

### Fluent query API

Queries are built by chaining methods and execute only on a terminal call:

```python
result = (
    ocel.query
    .events("Place Order", "Pay Order")
    .where(col("amount") > 100)
    .with_columns(month=col("ocel_time").dt.month())
    .group_by("ocel_type", "month")
    .agg(count().alias("n"), avg(col("amount")).alias("avg_amount"))
    .sort("month")
    .collect()                  # execute and return DuckDB relation
)
```

**Chainable methods:** `where`, `with_columns`, `select`, `group_by`, `agg`,
`having`, `sort`, `unique`, `limit`

**Terminal methods:** `collect`, `count`, `exists`, `scalar`, `to_sql`, `ids`,
`to_ocel`, `write`


## Optional PM4Py bridge

If you want to hand an `oceldb` dataset to PM4Py, use the optional interop
module:

```python
from oceldb import OCEL
from oceldb.interop import to_pm4py

with OCEL.read("running-example") as ocel:
    pm4py_ocel = to_pm4py(ocel)
```

This keeps PM4Py out of the core dependency set while still allowing
interoperability when you need it.


### Column references

Use bare strings for simple references and `col(...)` for expressions:

```python
# Bare strings in positional arguments
.select("ocel_id", "status")
.group_by("ocel_type")
.sort("ocel_time")

# col() for expressions
.where(col("status") == "open")
.with_columns(upper_name=col("name").str.upper())
```

### Relation predicates

Filter objects or events based on their relationships:

```python
from oceldb import has_event, has_object, cooccurs_with, linked

# Orders that have a "Pay Order" event
ocel.query.objects("order").where(has_event("Pay Order").exists())

# Orders co-occurring with at least 3 items
ocel.query.objects("order").where(cooccurs_with("item").count() >= 3)

# Events involving a "customer" object
ocel.query.events().where(has_object("customer").exists())

# Objects linked to another object type
ocel.query.objects("order").where(linked("package").exists())
```

### Sequence analysis

`event_occurrences(...)` combined with window functions enables process analysis:

```python
from oceldb import col, row_number

timeline = (
    ocel.query
    .event_occurrences("order")
    .with_columns(
        seq=row_number().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
        previous=col("ocel_event_type").lag().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
        next=col("ocel_event_type").lead().over(
            partition_by="ocel_object_id",
            order_by=("ocel_event_time", "ocel_event_id"),
        ),
    )
    .select("ocel_object_id", "seq", "previous", "ocel_event_type", "next")
    .sort("ocel_object_id", "seq")
)
```

### Sublog extraction

Identity-preserving query roots (`events`, `objects`, `object_states`) can
materialize filtered sublogs:

```python
# Write a sublog containing only paid orders and their related events
(
    ocel.query
    .objects("order")
    .where(has_event("Pay Order").exists())
    .write("paid-orders-sublog", overwrite=True)
)
```

## Inspection

Direct structural facts about a log, without derived analytics:

```python
from oceldb.inspect import (
    overview, event_types, object_types,
    event_attributes, object_attributes,
    table_counts, time_range,
)

with OCEL.read("my-log") as ocel:
    info = overview(ocel)
    print(info.event_count, info.object_count)

    print(event_types(ocel))          # ["Place Order", "Pay Order", ...]
    print(object_types(ocel))         # ["order", "item", "package", ...]
    print(event_attributes(ocel))     # {"Place Order": {"amount": "DOUBLE", ...}}
    print(table_counts(ocel))         # TableCounts(event=120, object=45, ...)
    print(time_range(ocel))           # TimeRange(min=..., max=...)
```

## Discovery

Derived analytical artifacts mined from the log:

```python
from oceldb.discovery import ocdfg

with OCEL.read("my-log") as ocel:
    dfg = ocdfg(ocel, "order")

    for node in dfg.nodes:
        print(f"{node.activity}: {node.count} occurrences")

    for edge in dfg.edges:
        print(f"{edge.source} -> {edge.target}: {edge.count}x, "
              f"mean {edge.mean_duration_seconds:.0f}s")
```

## Raw SQL

When the DSL isn't enough, drop into DuckDB SQL directly:

```python
with OCEL.read("my-log") as ocel:
    result = ocel.sql("SELECT ocel_type, COUNT(*) FROM event GROUP BY 1")
    print(result.fetchall())
```

## Expression reference

| Category | Functions |
|----------|-----------|
| Column | `col(name)`, `lit(value)` |
| Comparison | `==`, `!=`, `<`, `>`, `<=`, `>=`, `.is_null()`, `.not_null()`, `.is_in(...)` |
| Logic | `&`, `\|`, `~` |
| Arithmetic | `+`, `-`, `*`, `/` |
| Aggregation | `count()`, `count_distinct(expr)`, `sum_(expr)`, `avg(expr)`, `min_(expr)`, `max_(expr)` |
| Functions | `abs_(expr)`, `coalesce(*exprs)`, `round_(expr, n)` |
| String | `.str.upper()`, `.str.lower()`, `.str.contains(pat)`, `.str.starts_with(s)`, `.str.len()` |
| Datetime | `.dt.year()`, `.dt.month()`, `.dt.day()`, `.dt.hour()` |
| Conditional | `when(pred).then(val).otherwise(val)` |
| Window | `row_number()`, `.lag()`, `.lead()` -- via `.over(partition_by=..., order_by=...)` |
| Sorting | `asc(expr)`, `desc(expr)` |
| Relations | `has_event(type)`, `has_object(type)`, `cooccurs_with(type)`, `linked(type)` |
| Alias | `.alias(name)` |

## Storage layout

oceldb uses a canonical directory format:

```
my-log/
  manifest.json           # schema, provenance, storage metadata
  event.parquet           # events (ocel_id, ocel_type, ocel_time, + custom attrs)
  object.parquet          # object identities (ocel_id, ocel_type)
  object_change.parquet   # raw sparse object-history rows (ocel_id, ocel_type, ocel_time, ocel_changed_field, + custom attrs)
  event_object.parquet    # event-to-object relations (ocel_event_id, ocel_object_id, ocel_qualifier)
  object_object.parquet   # object-to-object relations (ocel_source_id, ocel_target_id, ocel_qualifier)
```

Convert from OCEL 2.0 SQLite with:

```python
from oceldb.io import convert_sqlite

convert_sqlite("source.sqlite", "target-directory", overwrite=True)
```

## License

MIT
