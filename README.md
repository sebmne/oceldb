# oceldb

Filter and query [OCEL 2.0](https://www.ocel-standard.org/) event logs on disk via [DuckDB](https://duckdb.org/).

oceldb reads OCEL 2.0 SQLite files without loading them into memory, applies filters through lazy DuckDB views, and exports filtered subsets back to SQLite or pm4py.

## Installation

```bash
pip install oceldb
```

For pm4py export support:

```bash
pip install oceldb[pm4py]
```

## Quick Start

```python
from oceldb import Ocel, event, obj

# Read an OCEL 2.0 SQLite file
with Ocel.read("log.sqlite") as ocel:

    # Inspect the log
    print(ocel.summary())
    print(ocel.event_types().fetchall())
    print(ocel.attributes("event", "Create Order"))

    # Filter events and objects
    filtered = (
        ocel.view()
        .filter(event.type == "Create Order")
        .filter(event.time > "2022-01-01")
        .filter(obj.type == "order")
        .create()
    )

    # Export
    filtered.to_sqlite("filtered.sqlite")
    pm4py_ocel = filtered.to_pm4py()  # requires oceldb[pm4py]
    filtered.close()
```

## Filter Expressions

Build filters using the `event` and `obj` namespaces. Standard OCEL column names are aliased automatically (`type` -> `ocel_type`, `id` -> `ocel_id`, `time` -> `ocel_time`). Any other name passes through for per-type attribute filtering.

### Comparisons

```python
event.type == "Create Order"
event.type != "Create Order"
event.time > "2022-01-01"
event.time >= "2022-01-01"
event.amount < 100
event.amount <= 100
```

### Predicates

```python
event.type.is_in("Create Order", "Pay Order")
event.type.not_in("Cancel Order")
event.amount.is_null()
event.amount.not_null()
event.type.is_like("%Order%")
event.type.not_like("%Test%")
event.amount.is_between(10, 100)
event.amount.not_between(10, 100)
```

### Logical Combinators

```python
(event.type == "A") & (event.time > "2022-01-01")  # AND
(event.type == "A") | (event.type == "B")           # OR
~(event.type == "A")                                 # NOT
```

### Domain Separation

Each `.filter()` call must reference only event columns or only object columns. Use separate calls for each:

```python
ocel.view()
    .filter(event.type == "Create Order")   # event domain
    .filter(obj.type == "order")            # object domain
    .create()
```

## API Reference

### `Ocel`

| Method | Returns | Description |
|---|---|---|
| `Ocel.read(path)` | `Ocel` | Open an OCEL 2.0 SQLite file |
| `.sql(query)` | `DuckDBPyRelation` | Run arbitrary SQL against the log |
| `.view()` | `ViewBuilder` | Start building a filtered view |
| `.event_types()` | `DuckDBPyRelation` | Distinct event types |
| `.object_types()` | `DuckDBPyRelation` | Distinct object types |
| `.events()` | `DuckDBPyRelation` | All events |
| `.objects()` | `DuckDBPyRelation` | All objects |
| `.event_objects()` | `DuckDBPyRelation` | Event-to-object relations |
| `.object_objects()` | `DuckDBPyRelation` | Object-to-object relations |
| `.attributes(entity, ocel_type)` | `list[str]` | Per-type attribute names |
| `.summary()` | `Summary` | Counts and type lists |
| `.to_sqlite(path)` | `None` | Export to OCEL 2.0 SQLite |
| `.to_pm4py()` | `pm4py.OCEL` | Export to pm4py (optional dep) |
| `.close()` | `None` | Release resources |

### `Summary`

```python
@dataclass
class Summary:
    num_events: int
    num_objects: int
    num_event_types: int
    num_object_types: int
    event_types: list[str]
    object_types: list[str]
    num_e2o_relations: int
    num_o2o_relations: int
```

### `ViewBuilder`

Chain `.filter(expr)` calls and finalize with `.create()` to get a new filtered `Ocel`.

## How It Works

oceldb attaches the SQLite file read-only to DuckDB and creates lazy SQL views for filtered subsets. No data is copied into memory until you materialize results (via `.fetchall()`, `.to_sqlite()`, or `.to_pm4py()`). Filtered views use DuckDB's `UNION ALL BY NAME` to unify per-type attribute tables, allowing filters on type-specific columns to work across all event/object types.
