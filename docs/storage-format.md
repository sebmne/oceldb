# oceldb storage format

The on-disk layout oceldb uses to store OCEL 2.0 event logs. This is the
contract for anyone reading or writing oceldb files directly, with or without
the Python API.

An oceldb log is a directory of type-partitioned Parquet files plus a minimal
versioned manifest. The Parquet files are plain Parquet — any Arrow-native
engine can read them without a custom parser; `manifest.json` exists only to
mark the directory as a complete oceldb dataset and record its layout
version.

---

## Goals

The format is designed so a columnar engine (Polars, DuckDB, Arrow, …) can
query it efficiently without materialising the whole log into memory.
Per-type partitioning, sorting, denormalisation, and compression all follow
from that.

1. **Readable without oceldb.** `pl.scan_parquet(...)` or
   `duckdb.read_parquet(...)` opens any file directly.
2. **Self-describing.** Schemas, types, row data, counts, and time ranges are
   all recoverable from the directory structure and Parquet footers — nothing
   lives only in application code.
3. **Schema-stable across type evolution.** Adding a new event or object type
   adds a directory; it never rewrites existing files.
4. **Compact.** Format overhead stays negligible even at millions of rows.

**Assumption this format makes:** a low-to-moderate number of distinct event
and object types (tens to low hundreds), since each type gets its own
partition file. A log with per-case-unique type names would produce one tiny
file per case — this format is not designed for that.

---

## Directory structure

```
my-log/
  manifest.json
  events/
    ocel_type=Place%20Order/data.parquet
    ocel_type=Pay%20Order/data.parquet
  objects/
    ocel_type=order/data.parquet
    ocel_type=item/data.parquet
  object_changes/
    ocel_type=order/data.parquet
    ocel_type=item/data.parquet
  e2o.parquet
  o2o.parquet
```

`events`, `objects`, and `object_changes` are **Hive-partitioned by type**:
one subdirectory per type, named `ocel_type=<url-encoded name>`, each holding
a single `data.parquet`. `e2o` and `o2o` are flat single files.

Type names are **URL-encoded** in directory names (`Place Order` →
`Place%20Order`), the standard Hive convention. Engines that implement Hive
partitioning (Polars included, via `hive_partitioning=True`) decode this
automatically and inject the canonical name as the `ocel_type` column; a
reader that surfaces the raw directory string must URL-decode it itself.

A directory is only a valid oceldb dataset if `manifest.json` is present —
Parquet files alone, without the manifest, are not recognized.

### `manifest.json`

The commit marker for a version-2 dataset, written last. Exactly three
fields:

```json
{
  "format": "oceldb",
  "formatVersion": 2,
  "createdAt": "2026-07-15T12:34:56Z"
}
```

`createdAt` is the UTC creation time of this snapshot; writing a filtered or
transformed log produces a new snapshot and a new value. The manifest carries
no schema, table paths, counts, or provenance — those are all derived from
the Parquet files themselves (see [Manifest and Parquet
responsibilities](#manifest-and-parquet-responsibilities)).

---

## Tables

Reserved column names (`ocel_id`, `ocel_time`, `ocel_type`,
`ocel_changed_field`, `ocel_is_initial`, and the relation columns) are the
stable contract and are defined as constants in `oceldb.core.schema`.

### `events/ocel_type=<type>/data.parquet`

| Column | Type | Notes |
|---|---|---|
| `ocel_id` | string | |
| `ocel_time` | timestamp (µs, UTC) | |
| `<attrs…>` | typed | only this type's attributes |

`ocel_type` is not stored in the file — it is the Hive partition key. One
file per event type, since each type has its own attribute schema; a single
table unioning all types would need one mostly-`NULL` column per attribute
across the whole log. A type with no attributes is just `ocel_id` +
`ocel_time`.

Sorted by `ocel_time`, so an engine can skip whole row groups on a time
predicate using Parquet's row-group min/max statistics.

### `objects/ocel_type=<type>/data.parquet`

One file per object type, containing only `ocel_id` (sorted). The type comes
from the partition key.

This identity table exists because an object can be known only through the
relation tables (see [The `ocel_is_initial`
invariant](#the-ocel_is_initial-invariant)) with no attribute history at
all — without it, such objects would be invisible to any query that starts
from the object set.

### `object_changes/ocel_type=<type>/data.parquet`

| Column | Type | Notes |
|---|---|---|
| `ocel_id` | string | |
| `ocel_time` | timestamp (µs, UTC) | |
| `ocel_changed_field` | string, nullable | `NULL` on the initial-state row |
| `ocel_is_initial` | bool | `true` on at most one row per object |
| `<attrs…>` | typed | only this type's attributes |

This is a change log, not a snapshot: OCEL 2.0 models objects as evolving
entities, and keeping every recorded state (rather than one current-state row
per object) is what lets a query ask "what was this order's status at the
time of that payment?". The current state, or the state as of any instant, is
reconstructed by forward-filling each attribute per object over `ocel_time`;
`oceldb.operations.states` does exactly this.

Sorted by `(ocel_id, ocel_is_initial descending, ocel_time)`, so each
object's history is contiguous, its initial row (if any) always comes first
regardless of its timestamp, and the remaining rows are chronological — no
extra sort needed before a forward-fill.

#### The `ocel_is_initial` invariant

An object's initial known state — its attribute values as of the first
instant it is known, before any recorded change — is written as a row with
`ocel_is_initial = true` and `ocel_changed_field = NULL`. Its `ocel_time` is
that first-known instant, not a fixed sentinel value.

This is a producer contract that every reader and operation relies on:

- **At most one `ocel_is_initial = true` row per object.**
- **An object with no attribute history at all has zero rows** in
  `object_changes` — it exists only in `objects`. This is different from an
  object that has exactly one known state and never changes again: that
  object still gets its single `ocel_is_initial` row.
- The initial row's `ocel_time` must be less than or equal to every other
  change row's `ocel_time` for that object, since it is treated as the
  starting point of the object's forward-filled history.
- Operations that filter or prune `object_changes` by object id are always
  safe. Operations that filter `object_changes` by time must not assume the
  initial row can be dropped by a naive time-range predicate — check
  `ocel_is_initial` explicitly if the operation needs to preserve state
  reconstruction.

### `e2o.parquet`

The event-to-object relation, with denormalised type columns:

| Column | Type |
|---|---|
| `ocel_event_id` | string |
| `ocel_event_type` | string |
| `ocel_object_id` | string |
| `ocel_object_type` | string |
| `ocel_qualifier` | string, nullable |

Types are denormalised — a query filtering "events of type X involving
objects of type Y" runs on this table alone, without joining back to
`events`/`objects`. The cost is small: Parquet dictionary-encodes
low-cardinality strings, so each type name is stored once and referenced by
an integer per row.

Sorted by `(ocel_object_id, ocel_event_id)`, which accelerates
object-to-events lookups (state reconstruction, object-scoped filters) more
than event-to-objects lookups. This is a deliberate one-sided choice, not an
oversight — most operations start from an object or a type predicate rather
than a single event.

### `o2o.parquet`

The object-to-object relation: `ocel_source_id`, `ocel_source_type`,
`ocel_target_id`, `ocel_target_type`, `ocel_qualifier`. Same denormalisation
rationale as `e2o`; sorted by `(ocel_source_id, ocel_target_id)`. **Omitted
entirely when the log has no O2O relations** — a missing file means an empty
relation, not an error.

---

## Conventions

- **Compression:** ZSTD everywhere, combined with Parquet's automatic
  dictionary encoding for low-cardinality strings. No manual tuning needed.
- **Per-type files carry only that type's attributes.** Different types'
  files have different column sets; a reader unions them by name, treating an
  attribute absent from a type's file as `NULL` for those rows. oceldb
  derives the union schema from the Parquet footers.
- **Shared attribute columns must agree on a dtype across types.** Logs
  written by oceldb satisfy this by construction. A reader unions foreign
  partitions with relaxed supercasting (compatible numeric differences widen
  rather than fail).
- **Timestamps** are stored as microsecond, UTC-normalized Parquet
  timestamps regardless of the source's original precision or offset. This
  is an intentional fidelity trade for process-mining-scale granularity —
  the wall-clock instant is preserved, sub-microsecond precision and the
  source UTC offset are not.
- **Ids, types, and qualifiers** are strings; numeric attributes are
  `int64`/`double`; booleans are `bool`.

---

## Manifest and Parquet responsibilities

Parquet is the source of truth for all row-level facts: which types exist
(partition directories), which attributes a type has values for (Parquet
footers), and row counts or time ranges (row-group statistics) — all without
scanning column data.

The manifest is authoritative only for the native layout version and
snapshot creation time. This split means filters and transformations never
have to keep duplicated schema or summary metadata in sync with the data.

---

## Reading a log without oceldb

```python
import polars as pl

# Per-type tables (ocel_type comes from the path; URL-decode it: "Place%20Order" -> "Place Order")
events  = pl.scan_parquet("my-log/events/**/*.parquet",         hive_partitioning=True)
objects = pl.scan_parquet("my-log/objects/**/*.parquet",        hive_partitioning=True)
changes = pl.scan_parquet("my-log/object_changes/**/*.parquet", hive_partitioning=True)

# Flat relation tables
e2o = pl.read_parquet("my-log/e2o.parquet")
o2o = pl.read_parquet("my-log/o2o.parquet")   # may be absent -> empty relation
```

```sql
-- DuckDB
SELECT * FROM read_parquet('my-log/events/**/*.parquet', hive_partitioning = true);
SELECT * FROM 'my-log/e2o.parquet';
```

---

## Not in scope

- **Sub-partitioning by time** (`type` + year/month). Would accelerate
  time-range queries on very large logs; deferred until a concrete case
  justifies the added complexity.
- **Append mode.** Every write produces a complete, self-contained directory;
  there is no incremental/append write path.
- **File checksums and inventories.** May be added if a concrete integrity or
  remote-storage use case justifies their maintenance cost.
