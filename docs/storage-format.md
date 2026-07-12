# oceldb storage format

This document describes the on-disk layout that oceldb uses to store OCEL 2.0 logs, explains each design decision, and discusses the trade-offs involved. It is intended for downstream library authors, contributors, and anyone who wants to read oceldb files directly — without going through the Python API.

The short version: **an oceldb log is a directory of type-partitioned Parquet files plus a small versioned manifest.** The Parquet files remain directly readable by any Arrow-native engine; `manifest.json` preserves the complete declared OCEL schema, the layout version, and optional metadata.

---

## Goals

The format is designed around one primary constraint: **a columnar engine (Polars, DuckDB, Arrow, …) must be able to query it efficiently without first materialising the whole log into memory.** Everything else — per-type partitioning, sorting, denormalisation, compression — follows from that.

Secondary goals, in priority order:

1. **Readable without oceldb.** You can open any file with plain `pl.scan_parquet(...)` or `duckdb.read_parquet(...)` and understand what you see. No bespoke reader required.
2. **Self-describing.** Row data, counts, and time ranges are recoverable from the directory structure and Parquet footers. The manifest records only stable metadata that cannot be recovered faithfully from populated rows, notably unused types and all-null declared attributes.
3. **Schema-stable across type evolution.** Adding a new event or object type adds a directory; it never rewrites existing files.
4. **Compact on disk.** A log with 10M events should not cost gigabytes for format overhead alone.

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
  event_object.parquet
  object_object.parquet
```

`events`, `objects` and `object_changes` are **Hive-partitioned by type**: one subdirectory per type, named `ocel_type=<url-encoded name>`, each containing a single `data.parquet`. The two relation tables are flat single files.

Type names are **URL-encoded** in directory names (`Place Order` → `Place%20Order`). This keeps paths unambiguous across operating systems and shells without inventing a bespoke escaping scheme; the canonical name (with spaces, slashes, unicode, …) is always recoverable by URL-decoding. **Note for direct readers:** when you read with `hive_partitioning`, the injected `ocel_type` value is the *encoded* string — URL-decode it to get the canonical type name. (oceldb's own reader decodes it for you.)

The reserved column names (`ocel_id`, `ocel_time`, `ocel_type`, `ocel_changed_field`, the relation columns, …) are the stable contract and are defined as constants in `oceldb.schema`.

The manifest is required. A directory containing only similarly named Parquet
files is not treated as an oceldb dataset.

### `manifest.json`

The manifest is written last and acts as the commit marker for a version 1
dataset. It contains:

- `format`: always `"oceldb"`;
- `formatVersion`: currently `1`;
- the complete declared event and object schemas;
- the stable paths and partition columns of the five logical tables;
- optional JSON-compatible dataset or provenance metadata.

It deliberately does **not** contain row counts, time ranges, or cached
summaries. Those values change when rows change and remain derived from Parquet
metadata, preventing a stale manifest from misreporting the actual data.

---

## Files

### `events/ocel_type=<type>/data.parquet`

One file per event type. Columns:

| Column | Type | Notes |
|---|---|---|
| `ocel_id` | string | |
| `ocel_time` | timestamp (µs) | |
| `<attrs…>` | typed | only this type's attributes |

`ocel_type` is **not stored** in the file — it is the Hive partition key, supplied by the directory name and injected on read.

**Why one file per type?** Each event type has its own attribute schema. A single wide table unioning all types would need one nullable column per attribute across the whole log — hundreds of mostly-NULL columns on a real log, wasting storage and I/O on every read. Per-type files mean a query for `Place Order` events reads only the `Place Order` file and only its columns. Types with no attributes are just `ocel_id` + `ocel_time`.

**Sorted by `ocel_time`.** Parquet stores per-row-group min/max statistics. A time-sorted file lets an engine skip whole row groups when a query has a time predicate, turning an O(n) scan into something close to O(log n) for selective filters.

### `objects/ocel_type=<type>/data.parquet`

One file per object type, containing only `ocel_id` (sorted). The type comes from the partition key.

This identity table exists because some objects appear only as references in the relation tables and have no attribute history at all. Without it, those objects would be invisible to any query that starts from the object set. The file is a single string column and compresses to almost nothing.

### `object_changes/ocel_type=<type>/data.parquet`

One file per object type. Columns:

| Column | Type | Notes |
|---|---|---|
| `ocel_id` | string | |
| `ocel_time` | timestamp (µs) | |
| `ocel_changed_field` | string | `NULL` on the synthetic initial-state row |
| `<attrs…>` | typed | only this type's attributes |

**Why a change log, not a snapshot?** OCEL 2.0 models objects as evolving entities. A snapshot table (one current-state row per object) would discard history; the change log keeps every state, which is what lets you ask "what was this order's status at the time of that payment?" The current state, or the state at any point in time, is reconstructed by forward-filling (last-non-null carry) per object over `ocel_time` — exposed as `OCEL.object_states()`.

**The synthetic initial-state row.** An object's initial attribute values are written as a change row with `ocel_time = 1970-01-01 00:00:00` and `ocel_changed_field = NULL`. Encoding the starting state as just another (pre-historical) change makes the forward-fill uniform — no special-casing the first row — and the epoch timestamp always sorts first as a stable anchor.

**Sorted by `(ocel_id, ocel_time)`** so each object's history is contiguous and ordered, which lets the fill-forward run without an extra sort.

### `event_object.parquet`

The event-to-object (E2O) relation, a flat file with **denormalised type columns**:

| Column | Type |
|---|---|
| `ocel_event_id` | string |
| `ocel_event_type` | string |
| `ocel_object_id` | string |
| `ocel_object_type` | string |
| `ocel_qualifier` | string (nullable) |

**Why denormalise the types?** The single most common process-mining access pattern is "events of type X involving objects of type Y." With the type columns inlined, that filter runs directly on this one table — no join back to `events`/`objects`. The cost is tiny: Parquet dictionary-encodes low-cardinality strings, so each type name is stored once and referenced by an integer per row. Sorted by `(ocel_object_id, ocel_event_id)` to collocate all events of a given object.

### `object_object.parquet`

The object-to-object (O2O) relation: `ocel_source_id`, `ocel_source_type`, `ocel_target_id`, `ocel_target_type`, `ocel_qualifier`. Same denormalisation rationale; sorted by `(ocel_source_id, ocel_target_id)`. **Omitted entirely when the log has no O2O relations** — readers treat a missing file as an empty relation.

---

## Conventions

- **Compression:** all files use **ZSTD** — a middle ground between Snappy (weaker ratio) and GZIP (slower). Combined with Parquet's automatic dictionary encoding for low-cardinality strings, no manual tuning is needed.
- **Per-type files carry only that type's declared attributes,** so different types' files have different column sets. A reader unions them **by name** — an attribute absent from a type's file is simply `NULL` for those rows. When an `OCELSchema` is available, empty and all-null declared attributes are retained as typed columns; manually constructed logs without metadata infer their schema from observed values.
- **Timestamps** are stored as microsecond Parquet timestamps; ids, types and qualifiers as strings; numeric attributes as `int64`/`double`, booleans as `bool`.

---

## Manifest and Parquet responsibilities

Parquet remains the source of truth for all row-level facts. Types represented
on disk can be discovered from partition directories, populated attribute
schemas from Parquet footers, and counts or time ranges from row-group
statistics without scanning column data.

The manifest is authoritative only for the native layout version, the complete
declared OCEL schema, and user metadata. This distinction avoids the drift
problem of older manifest designs that cached mutable totals while removing the
need to infer semantic declarations from empty physical files.

Version 1 still writes empty `data.parquet` partitions for declared types with
no instances and retains typed all-null columns. This keeps the dataset useful
to direct Parquet consumers that ignore the manifest. The manifest makes those
semantics explicit for oceldb and allows a future storage engine to avoid
depending on empty files without losing information.

---

## Why this beats the OCEL 2.0 SQLite / XML / JSON formats

OCEL 2.0 standardises three interchange encodings — XML, JSON, and SQLite. They are reasonable for *interchange*; they are poor for *working with large logs*. oceldb's Parquet layout is built for the analytical access patterns of process mining.

**Columnar, not document- or row-oriented.** Process-mining queries are overwhelmingly column-and-aggregate shaped: count events per activity, the time span of each type, forward-fill object attributes, join events to objects. Parquet stores each column contiguously, so a query reads only the columns it touches. XML and JSON must parse every field of every record into memory first; SQLite is a row store, so it reads whole rows off its B-trees even when you want one column.

**Partial reads and data skipping.** The per-type directories let an engine prune entire types it doesn't need; row-group min/max statistics let it skip blocks of rows by time or id. XML and JSON have no notion of partial reads at all — you parse the whole document before answering anything. SQLite can index, but it still pays row-store and page overhead and cannot prune by column.

**Self-describing, faithful types.** A Parquet footer carries a real typed schema — timestamps, `int64`, `double`, `bool`. JSON has no datetime and blurs int vs float; XML is strings all the way down; the OCEL 2.0 **SQLite** export leans on SQLite's loose dynamic typing, so a reader has to sniff each column's real type with `PRAGMA table_info`. oceldb's importer does both on the way in. Parquet keeps the types *in the file*.

**Compact.** Columnar layout + dictionary encoding + ZSTD typically produces files several times smaller than the equivalent XML/JSON text or a SQLite database, with no loss of fidelity.

**Larger-than-memory.** A streaming columnar engine reads and aggregates oceldb logs that don't fit in RAM. XML/JSON effectively require building the full document in memory.

**Ecosystem-native.** Parquet is the lingua franca of analytics. An oceldb log opens in one line with Polars, DuckDB, pandas/Arrow, Spark, or a cloud query engine — no OCEL-specific parser, no schema indirection. The partitioned directory layout sits naturally on object storage (S3/GCS) and parallelises across cores and machines. A single SQLite file is one lock-bound file; XML/JSON are opaque to every tool that isn't an OCEL reader.

| | XML / JSON | SQLite (OCEL 2.0) | oceldb Parquet |
|---|---|---|---|
| Orientation | document | row store | **columnar** |
| Column / partial reads | no | row-level | **column + row-group + type partition** |
| Data skipping | none | indexes only | **row-group stats + type pruning** |
| Types stored in file | weak / none | mostly `TEXT` + indirection | **full Parquet types** |
| Compression | none by default | page-level | **ZSTD + dictionary, columnar** |
| Larger-than-memory | no (full DOM) | partial | **yes (streaming)** |
| Tooling | OCEL readers only | sqlite tools | **Polars / DuckDB / Arrow / Spark / cloud** |
| Cloud & parallelism | poor | single file | **partitioned, object-storage native** |

**An honest caveat.** SQLite is a real database and a perfectly good interchange-plus-ad-hoc-query format; for small logs the difference is academic. The advantage of Parquet grows with scale and with the analytical (scan / aggregate / join) nature of the workload — which is exactly process mining on real-world logs. oceldb keeps DuckDB around precisely because it is excellent at *importing* the SQLite encoding; it just doesn't keep the log in it.

---

## Reading a log without oceldb

```python
import polars as pl

# Per-type tables (ocel_type comes from the path; URL-decode it: "Place%20Order" -> "Place Order")
events  = pl.scan_parquet("my-log/events/**/*.parquet",         hive_partitioning=True)
objects = pl.scan_parquet("my-log/objects/**/*.parquet",        hive_partitioning=True)
changes = pl.scan_parquet("my-log/object_changes/**/*.parquet", hive_partitioning=True)

# Flat relation tables
e2o = pl.read_parquet("my-log/event_object.parquet")
o2o = pl.read_parquet("my-log/object_object.parquet")   # may be absent -> empty relation
```

```sql
-- DuckDB
SELECT * FROM read_parquet('my-log/events/**/*.parquet', hive_partitioning = true);
SELECT * FROM 'my-log/event_object.parquet';
```

---

## Not in scope

- **Sub-partitioning by time** (`type` + year/month). Would further accelerate time-range queries on very large logs; deferred.
- **Append mode.** Writes always produce a complete, self-contained directory; incremental appends are not supported.
- **File checksums and inventories.** These may be added if a concrete integrity or remote-storage use case justifies their maintenance cost.
