# oceldb storage format (v1)

This document describes the on-disk layout that oceldb uses to store OCEL 2.0 logs, explains each design decision, and discusses the trade-offs involved. It is intended for downstream library authors, contributors, and anyone who wants to read oceldb files directly without going through the Python API.

---

## Goals

The format is designed around one primary constraint: **DuckDB must be able to query it efficiently without materialising anything into Python first.** Everything else — compression, sorting, denormalisation — follows from that.

Secondary goals, in order of priority:

1. **Readable without oceldb.** A downstream library author should be able to open any file with plain `duckdb.read_parquet(...)` and understand what they see.
2. **Schema-stable across type evolution.** Adding a new event or object type must not require rewriting existing files.
3. **Compact on disk.** A log with 10 M events should not cost gigabytes of storage for the format overhead alone.
4. **Manifest-first.** All structural metadata — type names, counts, attribute names, time ranges — must be readable from a single JSON file without touching any Parquet file.

---

## Directory structure

```
my-log/
  manifest.json
  events/
    ocel_type=Place%20Order/
      data.parquet
    ocel_type=Pay%20Order/
      data.parquet
  objects/
    ocel_type=order/
      data.parquet
    ocel_type=item/
      data.parquet
  object_changes/
    ocel_type=order/
      data.parquet
    ocel_type=item/
      data.parquet
  event_object.parquet
  object_object.parquet
```

Type names are URL-encoded when used as directory names (`Place Order` → `Place%20Order`). This keeps the paths unambiguous across operating systems and shells without inventing a bespoke escaping scheme. The canonical type name (with spaces, slashes, etc.) is always recoverable via URL-decoding.

---

## Files

### `manifest.json`

The manifest is the entry point for any tool that needs to understand the log. It contains:

- Format version (`oceldb_format_version`), so readers can refuse to open versions they don't understand.
- The full list of event and object types with their counts, time ranges, and attribute schemas.
- Global totals (event count, object count, E2O count, O2O count, overall time range).

Attribute schemas use Python-style type labels: `string`, `datetime`, `int`, `float`, and `bool`. The Parquet files still use the corresponding physical column types for efficient DuckDB execution.

Storing this in JSON rather than inside a Parquet file or a DuckDB catalog is a deliberate choice: JSON is universally readable, diff-friendly in version control, and does not require any special tooling to inspect. A tool that only needs to check "how many events are in this log" or "what attributes does the `order` type have" never needs to open a Parquet file.

### `events/ocel_type=<type>/data.parquet`

One file per event type. Columns:

| Column | Type |
|---|---|
| `ocel_id` | VARCHAR |
| `ocel_time` | TIMESTAMP |
| `<custom attrs>` | typed |

The `ocel_type` column is **not stored** in the file; it is a Hive partition key supplied by the directory name. DuckDB injects it automatically when reading via glob or `hive_partitioning=true`.

**Why one file per type?** Each event type has a distinct attribute schema. A flat, wide table that unions all types would require hundreds of nullable columns on a large log, wasting both storage and I/O bandwidth on columns that are almost always NULL for any given type filter. With per-type files, a query for `Place Order` events only reads the `Place Order` file — no other columns are touched.

**Sorted by `ocel_time`.** Parquet stores per-row-group min/max statistics. A time-sorted file means DuckDB can use these statistics to skip entire row groups when a query has a time predicate, turning an O(n) scan into something much closer to O(log n) for selective filters.

### `objects/ocel_type=<type>/data.parquet`

One file per object type. Contains only `ocel_id`. The `ocel_type` comes from the Hive partition key.

This table exists because some objects appear only as references in bridge tables (event-object or object-object relations) and have no attribute history at all. Without this identity table, those objects would be invisible to any query that starts from `objects()`. The file is tiny — a single VARCHAR column — and compresses to near nothing.

### `object_changes/ocel_type=<type>/data.parquet`

One file per object type. Columns:

| Column | Type | Notes |
|---|---|---|
| `ocel_id` | VARCHAR | |
| `ocel_time` | TIMESTAMP | |
| `ocel_changed_field` | VARCHAR | NULL for the synthetic initial-state row |
| `<custom attrs>` | typed | |

**Why a change log rather than a snapshot table?** OCEL 2.0 models objects as evolving entities with a change history. A snapshot table (one row per object, holding current state) would lose that history. The change-log layout preserves every historical state, which is necessary for questions like "what was the status of this order at the time of this payment event?" (answered by `object_states().as_of(t)`).

**The synthetic initial-state row.** The source OCEL's initial attribute values (the non-temporal, "starting state" rows in the SQLite `object_<type>` table) are translated into change rows with `ocel_time = 1970-01-01 00:00:00` and `ocel_changed_field = NULL`. This puts the initial state into the same representation as all other changes, which means the fill-forward window function (`LAST_VALUE(attr IGNORE NULLS) OVER ...`) works uniformly without special-casing the first row. The epoch timestamp is intentionally pre-historical: it is always earlier than any real event, so it sorts first and acts as a stable anchor for the window.

**Sorted by `(ocel_id, ocel_time)`.** The state-reconstruction window function partitions by `ocel_id` and orders by `ocel_time`. Pre-sorting the file ensures that rows for the same object are already contiguous and ordered, which lets DuckDB execute the window without a sort step when row groups happen to align with object boundaries on large files.

### `event_object.parquet`

The event-to-object relation table. Columns:

| Column | Type |
|---|---|
| `ocel_event_id` | VARCHAR |
| `ocel_event_type` | VARCHAR |
| `ocel_object_id` | VARCHAR |
| `ocel_object_type` | VARCHAR |
| `ocel_qualifier` | VARCHAR (nullable) |

**Why denormalise the type columns?** The single most common access pattern in process mining is "find all events of type X that involve objects of type Y." Without denormalised type columns, every such query requires a join back to the event or object tables to filter by type. With denormalisation, the filter runs directly against this flat table — no join needed. The storage overhead is small because Parquet uses dictionary encoding by default for low-cardinality VARCHAR columns; the type strings are stored once in a dictionary and referenced by integer indices in each row.

**Sorted by `(ocel_object_id, ocel_event_id)`.** The dominant access direction is "all events for object X", which corresponds to filtering by `ocel_object_id`. This sort order collocates rows for the same object, making those queries faster on large logs.

### `object_object.parquet`

The object-to-object relation table. Same rationale as `event_object.parquet` for denormalisation. Only written if the source log contains at least one O2O relation; the view generator skips registering the view when the file is absent.

---

## Compression and encoding

All Parquet files are written with ZSTD level 3 by default. ZSTD was chosen over Snappy (less compression) and GZIP (slower decompression). Level 3 is a reasonable middle ground between ratio and write speed; it can be overridden via the `compression` parameter on the conversion helpers.

Parquet's built-in dictionary encoding handles low-cardinality string columns (type names, qualifiers, status values) automatically. No manual encoding hints are needed.

---

## What is not in v1

The following were considered and deferred:

- **Sub-partitioning by time** (`type+time` layout). Partitioning events by both type and year/month would further speed up time-range queries on very large logs. This is reserved as a future `layout` value in the manifest. v1 readers must refuse to open a `layout` they do not recognise.
- **Append mode.** Writing new events or objects into an existing directory is not supported. All writes produce a complete, self-contained directory.
- **External Parquet row groups.** Splitting `data.parquet` per type into multiple files (e.g., one per year) for parallel writes is not in scope. The current single-file-per-type layout is simple and already benefits from DuckDB's parallel reader.
- **Non-Parquet backends.** The format is intentionally Parquet-only. CSV and JSON variants were not considered; they offer no meaningful advantage over Parquet for analytical workloads and complicate schema handling.

---

## Versioning and forward compatibility

The `oceldb_format_version` field in the manifest is a string, not a number, to leave room for non-linear version histories (e.g., `"1.1"` for a backwards-compatible addition). The current version is `"1"`.

A reader that encounters an unknown version must raise `UnsupportedFormatVersionError` rather than silently misreading the files. This is a hard requirement: a format change that rearranges columns or renames files would produce wrong results, not errors, if readers silently continued.

Downstream library authors who access `ocel.con` (the raw Ibis/DuckDB backend) or `ocel.manifest` directly should treat the column names in `oceldb.schema` as the stable contract. Those constants will not be renamed within a format version.
