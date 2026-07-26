# oceldb storage format

This document is the stable physical and logical contract for native oceldb
snapshots. Version 2 is the current format.

An oceldb snapshot is a directory of ordinary Parquet datasets plus a small
JSON commit marker. It is immutable: filtering or transforming a log and
writing it creates a new complete snapshot.

## Design goals

1. Lazy, projection-aware scans without loading the complete log.
2. Low memory use for both narrow queries and persistence.
3. Direct access from Polars, DuckDB, Arrow, and other Parquet engines.
4. Type-specific attribute schemas without a mostly-null global table.
5. Freedom to shard large tables without changing the logical format.

The type-partitioned tables assume a low-to-moderate number of event and
object types. Tens or low hundreds are expected; per-case-unique type names
are not.

## Version-2 layout

```text
my-log/
  manifest.json
  events/
    ocel_type=Place%20Order/
      part-00000.parquet
    ocel_type=Pay%20Order/
      part-00000.parquet
  objects/
    ocel_type=order/
      part-00000.parquet
  object_changes/
    ocel_type=order/
      part-00000.parquet
  e2o/
    part-00000.parquet
  o2o/
    part-00000.parquet
  indexes/
    e2o_by_object/
      part-00000.parquet
```

All five table directories always exist. E2O and O2O always contain at least
one Parquet part, including when the relation is empty. A partition or table
may contain multiple `*.parquet` parts; readers must treat the complete
directory as one logical dataset. Writers use bounded shards and row groups;
exchange-converter parser batches are not exposed as final Parquet fragments.

`events`, `objects`, and `object_changes` use Hive partitions named
`ocel_type=<URL-encoded type>`. The partition key is not stored in individual
files; readers inject it from the path. Type names must be URL-decoded when an
engine does not do that automatically.

## Manifest

`manifest.json` is written last and marks a complete snapshot:

```json
{
  "format": "oceldb",
  "formatVersion": 2,
  "createdAt": "2026-07-24T12:34:56Z"
}
```

These three fields are required. `createdAt` is a UTC snapshot-creation
timestamp. Readers accept additional fields so optional provenance,
checksums, or extension metadata can be introduced without changing the
version. Unknown fields never override row data or physical schemas.

Snapshots produced by oceldb also declare physical indexes:

```json
{
  "indexes": {
    "e2oByObject": {
      "path": "indexes/e2o_by_object",
      "sort": ["ocel_object_id", "ocel_event_id", "ocel_qualifier"]
    }
  }
}
```

They also declare the physical ordering of canonical relation datasets:

```json
{
  "relations": {
    "e2o": {
      "sort": ["ocel_event_id", "ocel_object_id", "ocel_qualifier"]
    },
    "o2o": {
      "sort": ["ocel_source_id", "ocel_target_id", "ocel_qualifier"]
    }
  }
}
```

Both declarations are optional for version-2 compatibility. A missing sort
declaration means that a reader must not assume physical row order. Readers
that do not understand indexes ignore them and continue to read the five
canonical tables.

The manifest does not duplicate table schemas, counts, or time ranges.
Parquet files and their footers remain the source of truth.

## Logical tables

### Events

Path: `events/ocel_type=<type>/*.parquet`

| Column | Type |
|---|---|
| `ocel_id` | non-null string |
| `ocel_time` | non-null timestamp (µs, UTC) |
| `<attributes...>` | typed, nullable |

Each event type stores only attributes with at least one observed non-null
value for that type. Declared-but-entirely-null attributes are intentionally
not preserved. Shared attribute names must use exactly the same dtype across
types. Attribute names must not reuse any reserved `ocel_*` core column name
defined by `oceldb.core.schema`.

Parts are sorted by `ocel_time` for Parquet row-group pruning of time
predicates.

### Objects

Path: `objects/ocel_type=<type>/*.parquet`

Each part contains one non-null string column, `ocel_id`, sorted by identifier.
The identity table is authoritative even when an object has no attributes or
relations.

### Object changes

Path: `object_changes/ocel_type=<type>/*.parquet`

| Column | Type |
|---|---|
| `ocel_id` | non-null string |
| `ocel_time` | non-null timestamp (µs, UTC) |
| `ocel_changed_field` | nullable string |
| `ocel_is_initial` | non-null boolean |
| `<attributes...>` | typed, nullable |

Parts are sorted by `(ocel_id, ocel_is_initial descending, ocel_time)`.
Object history is reconstructed by coalescing changes at one timestamp and
forward-filling attributes per object.

The stable change invariants are:

- An object has at most one initial row.
- An initial row has `ocel_changed_field = NULL`.
- A non-initial row has a non-empty `ocel_changed_field`.
- A non-initial row provides a non-null value in the named attribute column.
- The initial timestamp is not later than another change for that object.
- At most one non-null value exists for each
  `(ocel_id, ocel_time, attribute)`.
- An object without attribute history has no change rows and remains present
  in `objects`.

The last rule makes same-timestamp reconstruction deterministic without a
sequence column. Producers should coalesce compatible same-time changes into
one logical state transition.

### Event-to-object relations

Path: `e2o/**/*.parquet`

| Column | Type |
|---|---|
| `ocel_event_id` | non-null string |
| `ocel_event_type` | non-null string |
| `ocel_object_id` | non-null string |
| `ocel_object_type` | non-null string |
| `ocel_qualifier` | nullable string |

Event and object types are denormalized to support type filtering without
joining entity tables. Relation row order is not part of the format. The
directory form permits future sharding or physical partitioning while
preserving this logical schema.

oceldb writers physically sort canonical E2O rows by
`(ocel_event_id, ocel_object_id, ocel_qualifier)`. Individual files contain
consecutive ranges of that ordering. The ordering is a performance property,
not part of the logical relation semantics.

### Object-to-object relations

Path: `o2o/**/*.parquet`

| Column | Type |
|---|---|
| `ocel_source_id` | non-null string |
| `ocel_source_type` | non-null string |
| `ocel_target_id` | non-null string |
| `ocel_target_type` | non-null string |
| `ocel_qualifier` | nullable string |

Relation row order is not part of the format.

oceldb writers physically sort O2O rows by
`(ocel_source_id, ocel_target_id, ocel_qualifier)`.

### Object-oriented E2O index

Path: `indexes/e2o_by_object/*.parquet`

The optional object-oriented index contains exactly the canonical E2O rows
with the canonical E2O schema, physically sorted by
`(ocel_object_id, ocel_event_id, ocel_qualifier)`. It is committed
transactionally with the canonical tables. Object-oriented accessors and
exact distinct-event counts use this representation; event-oriented queries
continue to use `e2o/`.

Keeping the index outside `e2o/` ensures that generic recursive scans of the
canonical relation never see duplicate logical rows.

## Global logical invariants

- Event identifiers are unique.
- Object identifiers are unique.
- Every object change references an existing object with the same type.
- Every E2O endpoint references an existing event/object with the same
  denormalized type.
- Every O2O endpoint references an existing object with the same denormalized
  type.
- Required identifiers and type names are non-empty.
- Core column dtypes match the canonical schemas exactly.

`OCEL.open(path)` validates the manifest and physical schemas without reading
row data. `OCEL.open(path, validate=True)` and `ocel.validate()` additionally
execute the global logical checks.

## Physical conventions

- ZSTD compression for committed parts.
- Bounded relation shards of at most 250,000 rows.
- Parquet row groups of at most 50,000 rows for relation datasets.
- UTC-normalized microsecond timestamps.
- String identifiers, types, and qualifiers.
- Strict shared attribute dtypes; no implicit widening during open.
- Attribute presence is based on observed non-null values.
- Type partition names use the canonical URL encoding produced by the writer.
- The complete directory is staged beside the target and installed by rename.
- A writer reopens and physically validates staged output before installation.
- `overwrite=True` only replaces a physically valid native snapshot. A final
  symlink or unrelated existing path is rejected.

Installation is transactional at the process-error boundary: failures before
installation leave the existing target untouched, and a failed replacement
attempt restores its backup when the target is still free. The format assumes
a single writer per target path. A process or machine crash during the short
directory-swap window can leave an adjacent hidden `.backup-*` directory that
contains the previous complete snapshot; readers never treat staging or backup
directories as committed snapshots.

## Reading without oceldb

```python
import polars as pl

events = pl.scan_parquet(
    "my-log/events/ocel_type=*/*.parquet",
    hive_partitioning=True,
)
objects = pl.scan_parquet(
    "my-log/objects/ocel_type=*/*.parquet",
    hive_partitioning=True,
)
changes = pl.scan_parquet(
    "my-log/object_changes/ocel_type=*/*.parquet",
    hive_partitioning=True,
)
e2o = pl.scan_parquet("my-log/e2o/**/*.parquet")
o2o = pl.scan_parquet("my-log/o2o/**/*.parquet")
```

Different type partitions can have different attribute columns, so generic
readers may need an explicit union schema or a missing-column option.

```sql
SELECT *
FROM read_parquet(
  'my-log/events/ocel_type=*/*.parquet',
  hive_partitioning = true,
  union_by_name = true
);

SELECT * FROM read_parquet('my-log/e2o/**/*.parquet');
```

The optional index is independently readable when an object-oriented physical
order is useful:

```sql
SELECT *
FROM read_parquet('my-log/indexes/e2o_by_object/**/*.parquet');
```

## Deliberately deferred

- Physical E2O/O2O type partitioning.
- Append or in-place mutation.
- Checksums and file inventories.
- A target-oriented O2O index.

These can be added behind the dataset-directory abstraction after workload
benchmarks justify them.
