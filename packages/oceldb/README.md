## oceldb

`oceldb` is a DuckDB-backed Python library for working with OCEL 2.0 logs
without materializing the full log into Python objects. The library converts
strict OCEL 2.0 SQLite files into a parquet-based storage format and exposes a
single lazy query API for filtering, grouping, sublog creation, and export.

## Fixed Public Contract

The core `OCEL` class is intentionally thin. It is responsible for:

- identifying the dataset source
- exposing stable metadata
- owning lifecycle and cleanup
- exposing the query entrypoint
- exposing explicit write/export operations
- exposing an advanced raw SQL escape hatch

The main public entrypoints are:

```python
from oceldb.io import convert_sqlite, read_ocel

convert_sqlite("running-example.sqlite", "running-example.oceldb")

with read_ocel("running-example.oceldb") as ocel:
    print(ocel.metadata)
    print(ocel.inspect.overview())
```

## Storage Format

The canonical storage format is a directory containing:

- `manifest.json`
- `event.parquet`
- `object.parquet`
- `event_object.parquet`
- `object_object.parquet`

The canonical logical tables are:

- `event`: core event columns plus typed custom event attributes
- `object`: core object columns plus typed custom object attributes
- `event_object`
- `object_object`

Custom attributes are stored as real typed parquet columns instead of packed
JSON strings. This makes the DSL simpler and avoids repetitive casting during
query construction.

`read_ocel(...)` transparently accepts either a canonical directory or a
single-file packaged archive. Writing supports both via `packaged=False/True`.

## Query API

Querying starts from `ocel.query()` and then chooses a root inside the DSL.

```python
from oceldb.dsl import col, count, desc, has_event, related

with read_ocel("running-example.oceldb") as ocel:
    paid_orders = (
        ocel.query()
        .objects("order")
        .filter(has_event("Pay Order").exists())
    )

    print(paid_orders.count())
    print(paid_orders.ids())

    per_type = (
        ocel.query()
        .events()
        .group_by("ocel_type")
        .agg(count().alias("n"))
        .sort(desc("n"))
        .collect()
    )

    alice_orders = (
        ocel.query()
        .objects("order")
        .filter(related("customer").any(col("name") == "Alice"))
    )
```

The query surface is intentionally lazy and uniform across:

- `ocel.query().events(...)`
- `ocel.query().objects(...)`
- `ocel.query().event_objects()`
- `ocel.query().object_objects()`

Core operations include:

- `filter`
- `with_columns`
- `select`
- `group_by(...).agg(...)`
- `sort`
- `unique`
- `limit`
- `collect`
- `count`
- `ids`
- `to_ocel`
- `write`

## Derived Sublogs

Event- and object-rooted row-preserving queries can be materialized back into
derived OCELs.

```python
from oceldb.dsl import col

with read_ocel("running-example.oceldb") as ocel:
    (
        ocel.query()
        .objects("order")
        .filter(col("status") == "open")
        .write("open-orders.oceldb", packaged=True, overwrite=True)
    )
```

## Inspection Layer

Inspection helpers are implemented on top of the DSL, not as a separate query
engine. Examples include:

- `ocel.inspect.overview()`
- `ocel.inspect.types()`
- `ocel.inspect.attributes()`
- `ocel.inspect.event_object_stats()`

## Running Example

A larger synthetic running example is available under [examples/RUNNING_EXAMPLE.md](examples/RUNNING_EXAMPLE.md).
It models fulfillment and reverse logistics with orders, items, packages,
shipments, invoices, payments, supplier orders, and return cases.

Generate it with:

```bash
uv run python examples/generate_running_example.py
```

Run the companion analysis:

```bash
uv run python examples/analyze_running_example.py
```
