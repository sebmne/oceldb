# oceldb

`oceldb` is a Python library for working with OCEL 2.0 logs directly on disk.
It converts OCEL 2.0 SQLite logs into a simple parquet-based layout and exposes
a lazy query DSL built on DuckDB.

## What It Does

- keeps the log on disk instead of materializing it into Python objects
- stores custom attributes as typed parquet columns
- provides a compact, dataframe-like DSL for filtering, grouping, and sublogs
- supports raw SQL when you need full control

## Install

```bash
uv add oceldb
```

## Quick Start

```python
from oceldb import OCEL, convert_sqlite
from oceldb.dsl import col, count, has_event

convert_sqlite("running-example.sqlite", "running-example", overwrite=True)

with OCEL.read("running-example") as ocel:
    paid_orders = (
        ocel.query
        .objects("order")
        .where(has_event("Pay Order").exists())
    )

    event_counts = (
        ocel.query
        .events()
        .group_by("ocel_type")
        .agg(count().alias("n"))
        .sort("n", descending=True)
        .collect()
    )

    open_orders = (
        ocel.query
        .object_states("order")
        .latest()
        .where(col("status") == "open")
    )

    print(paid_orders.count())
    print(event_counts.fetchall())
    print(open_orders.count())
```

## Main API

The stable handle is `OCEL`.

```python
from oceldb.discovery import ocdfg
from oceldb.inspect import overview

with OCEL.read("my-log") as ocel:
    ocel.query.events(...)
    ocel.query.objects(...)
    ocel.query.object_states(...).latest()
    ocel.query.object_states(...).as_of("2024-01-01")
    ocel.query.object_changes(...)

    overview(ocel)
    ocdfg(ocel, "order")
    ocel.sql("SELECT * FROM event LIMIT 5")
```

Inspection and discovery are separate modules:

- `oceldb.inspect` for direct structural facts such as types, attributes, and log overview
- `oceldb.discovery` for mined artifacts such as OC-DFGs

Important distinction:

- `objects(...)` returns object identities
- `object_changes(...)` returns raw object history rows
- `object_states(...)` returns reconstructed object state snapshots

## Storage Layout

The canonical `oceldb` format is a directory:

- `manifest.json`
- `event.parquet`
- `object.parquet`
- `object_change.parquet`
- `event_object.parquet`
- `object_object.parquet`

This is the only supported on-disk format.

## Status

The project currently focuses on:

- a fixed `OCEL` class
- a clean IO layer
- a lazy DSL for OCEL analysis

Longer documentation and walkthroughs will follow later.
