# oceldb storage format

This document describes the on-disk layout that oceldb uses to store OCEL 2.0 logs, explains each design decision, and discusses the trade-offs involved. It is intended for downstream library authors, contributors, and anyone who wants to read oceldb files directly — without going through the Python API.

The short version: **an oceldb log is a directory of Parquet files, partitioned by type, and nothing else.** There is no database, no manifest, no index sidecar. The files *are* the format, and any columnar/Arrow-native engine can read them.

---

## Goals

The format is designed around one primary constraint: **a columnar engine (Polars, DuckDB, Arrow, …) must be able to query it efficiently without first materialising the whole log into memory.** Everything else — per-type partitioning, sorting, denormalisation, compression — follows from that.

Secondary goals, in priority order:

1. **Readable without oceldb.** You can open any file with plain `pl.scan_parquet(...)` or `duckdb.read_parquet(...)` and understand what you see. No bespoke reader required.
2. **Self-describing.** All structural metadata — the set of types, their attribute schemas, counts, and time ranges — is recoverable from the directory structure and the Parquet footers. No separate metadata file to maintain or to drift out of sync.
3. **Schema-stable across type evolution.** Adding a new event or object type adds a directory; it never rewrites existing files.
4. **Compact on disk.** A log with 10M events should not cost gigabytes for format overhead alone.

---

## Directory structure

```
my-log/
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
- **Per-type files carry only that type's attributes,** so different types' files have different column sets. A reader unions them **by name** — an attribute absent from a type's file is simply `NULL` for those rows. (oceldb's native writer additionally omits an attribute that is `NULL` for every row of a type; the SQLite importer keeps every declared attribute column.)
- **Timestamps** are stored as microsecond Parquet timestamps; ids, types and qualifiers as strings; numeric attributes as `int64`/`double`, booleans as `bool`.

---

## No manifest — the data is the source of truth

Earlier versions kept a `manifest.json` listing types, counts, time ranges and attribute schemas. It is gone, on purpose. Every piece of that metadata is already derivable from the layout:

- **Types** — the `ocel_type=…` partition directories.
- **Attribute schemas** — the Parquet column schema in each per-type footer.
- **Counts and time ranges** — Parquet row-group statistics (row counts; per-column min/max), read from footers **without scanning any data**.

So a manifest is redundant — and worse, it can *drift*: a filtered or edited log whose manifest still advertises the original totals is a silent correctness bug (one the manifest-first design actually had). Removing it makes the files the single source of truth: what you read is always what is there. "How many `Pay Order` events?" is answered by reading footers — cheap (metadata only), just not literally a single JSON read.

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
- **In-band format version.** The directory layout plus the reserved column names in `oceldb.schema` are the contract; there is currently no version marker file (a deliberate simplification over the old manifest). A breaking layout change would, if ever needed, introduce a small marker so readers can refuse versions they don't understand.
