# Performance benchmarks

These measurements answer two separate questions:

1. Does the native Parquet layout provide low-latency access and scale with the
   amount of data?
2. How much memory do whole-log validation, pruning, state reconstruction, and
   persistence require?

The reproducible runner lives in [`benchmarks/benchmark.py`](../benchmarks/benchmark.py).
Each case runs in a fresh process, reports the median after one warmup, and
records process peak RSS so Polars and DuckDB native allocations are included.
Query timings are warm-cache measurements, not cold-disk claims.

## Environment

- MacBook Air, Apple M4, 10 CPU cores, 24 GB RAM
- macOS 26.5.1, arm64
- Python 3.13.12
- Polars 1.41.2, DuckDB 1.5.1
- Default Polars pool: 10 threads
- Measured 2026-07-14

## Datasets

The real Container Logistics dataset contains 35,413 events, 13,910 objects,
13,052 object changes, 74,272 E2O relations, and 15,926 O2O relations. Its
native directory occupies 0.6 MiB.

The large synthetic dataset contains 1,000,000 events, 300,000 objects, 600,000
object changes, 3,000,000 E2O relations, and 300,000 O2O relations: 5.2 million
logical rows in total. Its repeated deterministic values compress to only 9.7
MiB, so its file size is not representative of real logs. It is useful for
measuring algorithmic scaling and memory behavior.

## Native operations

Times are median milliseconds. RSS is the peak process resident set in MiB and
includes a roughly 95 MiB interpreter/library baseline.

| Operation | Real ms | Real RSS | 1M-event ms | 1M-event RSS |
|---|---:|---:|---:|---:|
| Open and validate physical layout | 2.71 | 96 | 2.30 | 96 |
| Validate logical OCEL | 27.31 | 201 | 385.72 | 482 |
| Describe all tables (cold) | 3.15 | 136 | 9.51 | 284 |
| Count all events | 0.94 | 122 | 2.76 | 146 |
| Count one event type | 0.24 | 106 | 0.30 | 106 |
| Direct Polars count of that partition | 0.35 | 106 | 0.37 | 106 |
| Count a 20% time window | 1.74 | 128 | 4.26 | 155 |
| Event-type filter, collect full core | 9.99 | 168 | 45.41 | 301 |
| Event-count object filter, collect full core | 22.91 | 216 | 373.48 | 784 |
| Reconstruct one object type's states | 6.04 | 178 | 58.07 | 516 |
| Flatten the busiest object type | 10.12 | 196 | 100.21 | 536 |
| Rewrite unchanged native dataset | 39.48 | 187 | 406.35 | 505 |
| Rewrite after filtering one event type | — | — | 260.41 | 395 |
| Rewrite detached/transformed frames | 82.16 | 249 | 665.96 | 1,000 |

On the large synthetic event-value scan, oceldb took 1.05 ms and direct Polars
took 0.97 ms. That difference is within normal run-to-run noise and shows that
the native scan layer adds essentially no cost once the lazy frame exists.
Simple counts may be answered from Parquet metadata and must not be interpreted
as physical row-scan throughput.

The 250,000-event synthetic run was approximately one quarter of the large
workload. Full object-count filtering took 91 ms, state reconstruction 14 ms,
and flattening 35 ms. The corresponding large transformation timings are close
to linear rather than showing a scaling cliff.

## BPIC 2017 large real-world log

An additional 683.1 MiB BPIC 2017 SQLite log provides realistic event
attributes and compression entropy:

- 1,202,267 events across 26 event types
- 106,162 objects across 4 object types
- 106,162 object-change rows
- 2,404,534 E2O and 74,504 O2O relations
- 3,893,629 logical rows in total
- 22 columns in the combined event table

Strict conversion produced a 39.5 MiB native dataset, a 17.3x reduction from
the SQLite source. One fresh strict conversion took 3.68 seconds. Repeated
warm-cache import benchmarks took 2.04 seconds with validation disabled and
4.05 seconds with strict validation, peaking at 1.00 and 1.80 GiB respectively.

| Native operation | Median | Peak RSS |
|---|---:|---:|
| Open and validate physical layout | 4.12 ms | 97 MiB |
| Validate logical OCEL | 274.51 ms | 533 MiB |
| Describe all tables (cold) | 10.10 ms | 286 MiB |
| Count all events | 4.60 ms | 175 MiB |
| Materialize all 1.2M events and 22 columns | 29.32 ms | 694 MiB |
| Direct Polars full-event materialization | 30.82 ms | 685 MiB |
| Count busiest event type through `events(type)` | 0.33 ms | 107 MiB |
| Direct Polars count of that type partition | 0.40 ms | 106 MiB |
| Count a 20% time window | 7.32 ms | 176 MiB |
| Event-type filter, collect full core | 20.62 ms | 213 MiB |
| Stored event/object-type view, materialize events | 31.54 ms | 588 MiB |
| Stored event/object-type view, first HTML representation | 39.56 ms | 451 MiB |
| Materialize all five tables of that view once | 52.74 ms | 673 MiB |
| Materialized view, repeated event access | 0.009 ms | 438 MiB |
| Event-count object filter, collect full core | 236.60 ms | 797 MiB |
| Reconstruct one object type's states | 26.58 ms | 353 MiB |
| Flatten the busiest object type | 258.91 ms | 1,404 MiB |
| Rewrite unchanged native dataset | 299.79 ms | 523 MiB |
| Rewrite detached/transformed frames | 1,405.81 ms | 1,366 MiB |
| Rewrite after filtering one event type | 555.09 ms | 771 MiB |

The full event scan is effectively identical to direct Polars. Type-specific
access now retains the native partition scans and unions only the requested
`data.parquet` files. This removed the former eager attribute-presence query:
the BPIC count improved from 30.59 ms and 578 MiB to 0.33 ms and 107 MiB. It is
now within benchmark noise of scanning the partition directly.

Native persistence has two explicit paths. After one logical validation,
unchanged native tables are copied directly into the transactional staging
directory. Modified or in-memory tables are normalized lazily and written with
streaming Parquet sinks. The unchanged BPIC rewrite improved from the original
1,619 ms and 1,213 MiB to 300 ms and 523 MiB. Its peak is now set by logical
validation; the copy and transactional installation add little measurable
memory. The detached benchmark deliberately removes reusable native provenance
to measure the streaming fallback independently.

Each logical table now owns its combined lazy plan, optional per-type plans,
and reusable native source path in one `OCELTable`. Event/object predicates,
relation filters, pruning, and type renaming preserve per-type plans where that
is safe; many-to-one type renames merge their old partitions. The dedicated
filtered rewrite case measures this third, partition-aware path. On the large
synthetic log it takes 260 ms and 395 MiB. Unlike the unchanged and
detached cases, it also evaluates the filter and rebuilds the connected OCEL
core, so its runtime is not a like-for-like writer comparison.

Canonical type filters now specialize this path further: they select or drop
known Parquet partitions and apply the matching predicate directly to the
denormalized event/object type in E2O. They no longer discover the same IDs by
joining a filtered entity scan back into E2O. This preserves the public filter
semantics while reducing the million-event full-core type filter from 113 ms
and 539 MiB to 45 ms and 301 MiB. On BPIC it fell from 100 ms and 514 MiB to
21 ms and 213 MiB.

This also fixes the common stored-view path. On BPIC, retaining a composed
event/object-type view and materializing its events now takes 32 ms; the same
path previously took about 200 ms. Notebook representations no longer launch
seven independent collections over the pruning graph. Counts, types, and time
bounds are collected through one optimizer-visible summary plan and its small
immutable result is cached on the OCEL. The first BPIC HTML representation is
40 ms and subsequent representations are below timer resolution; the reported
slow case was approximately 1.5 seconds.

`OCELTable` now deliberately keeps its combined and per-type plans separate.
Previously, every transformation rebuilt the combined frame by concatenating
the transformed partition plans. A cross-table membership join was therefore
copied into each partition and copied again by the next filter. On the examined
three-stage sub-log, the object-change plan grew to 88 scans and 96 joins. The
same plan now has 25 scans and 24 joins: transformations apply once to the
combined frame, while type-specific access still uses the partition plans.

A Python reference retains a lazy query plan, not evaluated rows. Consequently
a non-materialized sub-log still has to evaluate its selection predicates and
membership joins, whereas counts on an unfiltered native log can often use
Parquet metadata. For a completed sub-log reused by independent operations,
`OCEL.materialize()` is the explicit reuse boundary. Materializing the busiest
BPIC type view's five tables takes 53 ms and retains about 52 MiB of logical
frame data; subsequent full event access is about 0.009 ms. Keeping this
explicit avoids forcing eager execution and its memory cost on one-shot or
still-being-composed pipelines.

Native writes now perform only a narrow schema/attribute preflight on the
source plan. They then copy or stream all tables into the transactional staging
directory, validate the physical layout and reopened logical dataset there,
and write the manifest only after both checks pass. This validates exactly what
will be committed without executing a transformed pruning graph once for
validation and again for persistence. The million-event filtered rewrite fell
from 1.31 seconds to 685 ms at that stage; the later type-filter specialization
reduced the current result to 260 ms. Unchanged and detached rewrites remained
stable at 399 ms and 668 ms respectively.

Logical validation now aggregates table-level checks once, uses one streaming
exact anti-join per reference contract, and performs a small lookup only when
an invalid `(id, type)` pair needs diagnosis. Native type partitions reuse the
attribute-schema guarantee already established by physical layout validation;
transformed and in-memory frames still receive row-level attribute placement
checks. On BPIC this reduced validation from 566 ms and roughly 1 GiB to 275 ms
and 533 MiB without weakening missing-reference or type-mismatch diagnostics.

Flattening now filters and deduplicates the case relations first, reconstructs
object attributes only for related cases, and sorts the narrow case/event keys
before attaching event payload columns with an order-preserving join. It also
uses attribute-only object states instead of constructing unused causing-event
metadata. The retained BPIC output is 259 MiB; peak RSS fell from about 1.79 GiB
to 1.39 GiB. Its runtime rose from roughly 222 ms to 261 ms because BPIC has a
wide payload and no object attributes. On the dynamic-state synthetic workload,
both time and memory improved: 148 ms to 110 ms and 658 MiB to 495 MiB.

A subsequent plan profile showed that static/dynamic attribute classification
was unnecessarily reconstructing and forward-filling the complete state history
before the returned lazy plan reconstructed it again. Forward-filling repeats
existing values and cannot change distinct-value classification, so flattening
now classifies directly from sparse changes and reconstructs states only when a
dynamic attribute exists. In an immediate paired million-event run, median time
fell from 106.97 ms to 100.21 ms; peak RSS remained within allocator noise
(529 versus 536 MiB). The static-heavy Container run is now about 10 ms.

On BPIC, which has no case-object attributes, the dominant work remains the
final 22-column event-payload hash join and its 259 MiB materialized result.
Forced streaming, per-event-type joins, packed payload structs, cardinality
annotations, and repeated narrow attribute joins were measured and rejected:
each worsened either peak memory or runtime. This cost cannot currently be
removed inside the existing Polars `LazyFrame` contract without changing output
semantics or coupling flattening to a particular sink.

## Parallelism

The large workload was also run with `POLARS_MAX_THREADS=1`:

| Operation | 10 threads | 1 thread | Parallel peak RSS | Single peak RSS |
|---|---:|---:|---:|---:|
| Event-type full-core filter | 113 ms | 467 ms | 539 MiB | 384 MiB |
| Event-count object filter | 373 ms | 1,390 ms | 784 MiB | 513 MiB |
| Object states | 58 ms | 228 ms | 516 MiB | 336 MiB |

Parallel execution gives roughly 2.2–4.1x speedups on these operations, with a
30–68% increase in peak memory.

## SQLite import

The direct DuckDB-backed SQLite-to-native path was measured separately:

| Source | Validation | Logical output rows | Median | Peak RSS |
|---|---|---:|---:|---:|
| Container Logistics, 15.4 MiB | none | 152,573 | 100 ms | 155 MiB |
| Order Management, 9.1 MiB | none | 218,642 | 122 ms | 179 MiB |
| Order Management, 9.1 MiB | strict | 218,642 | 227 ms | 346 MiB |
| BPIC 2017, 683.1 MiB | none | 3,893,629 | 2.04 s | 1,028 MiB |
| BPIC 2017, 683.1 MiB | strict | 3,893,629 | 4.05 s | 1,844 MiB |

Strict import of the supplied Container Logistics SQLite file intentionally
fails because the source stores 1,999 literal `"null"` strings in a column
declared as `REAL`. This is input-quality rejection, not an importer crash.

## Verdict and optimization targets

The library is fast: native opening is a few milliseconds, scans are effectively
direct Polars scans, the 22-column BPIC event table materializes at direct
Polars speed, smaller real-log transformations complete in single-digit to low
double-digit milliseconds, and synthetic timings scale approximately linearly.
SQLite ingestion ranges from sub-quarter-second for the smaller available logs
to roughly 2–4 seconds for the 683 MiB BPIC source.

The library is not yet memory-efficient for all large whole-log operations.
BPIC flattening and detached streaming rewrite each peak around 1.4 GiB.
Logical validation is about 533 MiB. These figures are acceptable on the 24 GB
benchmark machine but wide flattening and transformed persistence would
constrain much larger logs.

Safe partition lineage through common transformations and repeated summary
access are now implemented. The remaining measured priorities are:

1. Reuse shared selections from arbitrary attribute/count filters when several
   pruned tables are consumed independently; canonical type views already avoid
   this recomputation.
2. Revisit wide payload-join memory when Polars offers a lower-memory join plan
   that preserves the existing generic `LazyFrame` contract.

Performance work should optimize these measured paths while retaining this
runner as a regression benchmark.
