# Benchmarks

The benchmark harness exercises the public `OCEL` API and native storage
format with deterministic synthetic logs. Generation is batched, so a profile
can exceed available memory without first constructing the complete log in
RAM. Every measured sample runs in a fresh Python process.

## Quick start

Generate a temporary `small` dataset and run every workload:

```bash
uv run python benchmarks/benchmark.py suite \
  --rounds 3 \
  --warmups 1 \
  --output /tmp/oceldb-benchmark.json
```

Keep the generated snapshot for repeated comparisons:

```bash
uv run python benchmarks/benchmark.py generate /tmp/oceldb-small \
  --profile small \
  --validate

uv run python benchmarks/benchmark.py run /tmp/oceldb-small \
  --rounds 5 \
  --warmups 1 \
  --include-plans \
  --output /tmp/oceldb-small.json
```

Use `--case` more than once to select workloads:

```bash
uv run python benchmarks/benchmark.py run /tmp/oceldb-small \
  --case open \
  --case event_typed_scan \
  --case native_rewrite
```

Run `--help` on the program or a subcommand for the complete option list.

## Profiles

| Profile | Events | Objects | E2O | Object states | O2O |
| --- | ---: | ---: | ---: | ---: | ---: |
| `small` | 50,000 | 15,000 | 148,500 | 60,000 | 15,000 |
| `medium` | 1,000,000 | 300,000 | 2,970,000 | 1,800,000 | 600,000 |
| `large` | 10,000,000 | 3,000,000 | 39,600,000 | 24,000,000 | 6,000,000 |

The presets contain skewed event and object types, sparse type-dependent
attributes, high-cardinality values, multiple state changes, qualified E2O
and O2O relations, and deliberately relationless events and objects. The seed
and every shape parameter can be overridden from the command line.

`--batch-size` controls source-generation memory, not native Parquet row-group
size. Generation time is reported separately and is never included in workload
measurements.

## Measurement model

Each sample includes opening the native snapshot and executing the selected
public API path. The result is consumed so lazy plans cannot be optimized away.
The runner reports median wall time, the sample range in JSON, and maximum peak
resident memory. Peak RSS includes the Python interpreter and native Polars
allocations.

Warm-ups populate operating-system file caches, but do not reuse a Python
process. These are warm-cache benchmarks, not controlled cold-disk benchmarks.
Use `--threads 1` for a reproducible single-thread comparison; otherwise Polars
chooses its normal thread-pool size.

The optimized plans emitted by `--include-plans` or the `plans` command are
useful regression evidence for partition and predicate pruning:

```bash
uv run python benchmarks/benchmark.py plans /tmp/oceldb-small
```

Do not compare reports unless the dataset profile, oceldb and Polars versions,
Python version, machine, operating system, and thread count match. See
[`docs/benchmarks.md`](../docs/benchmarks.md) for workload definitions and the
release measurement protocol.

## Exchange conversion

The conversion harness measures direct SQLite, JSON, or XML conversion in a
fresh process per sample:

```bash
uv run python benchmarks/conversion.py path/to/log.sqlite \
  --validate \
  --rounds 3 \
  --warmups 1 \
  --output /tmp/oceldb-conversion.json
```

It reports conversion wall time, peak RSS, source and native byte sizes, the
parser batch size, validation mode, raw samples, and environment metadata.
Final native shards are compacted independently of the parser batch size.
Keep the same source file for before-and-after comparisons.

## Competitor comparison

The competitive harness compares the overlapping OCEL capabilities of oceldb
with PM4Py and r4pm. Both competitors are pinned as optional benchmark
dependencies and are not installed with oceldb:

```bash
uv sync --group benchmark

uv run --group benchmark python benchmarks/competitor_comparison.py \
  path/to/log.sqlite \
  --rounds 3 \
  --warmups 1 \
  --threads 1 \
  --timeout 300 \
  --output /tmp/oceldb-competitors.json
```

When `--native` is omitted, the runner creates and validates a temporary
native snapshot before measurement. Pass an existing snapshot to avoid that
preparation on repeated runs:

```bash
uv run --group benchmark python benchmarks/competitor_comparison.py \
  path/to/log.sqlite \
  --native /tmp/log-native \
  --case load \
  --case event_type_filter \
  --case object_count_filter
```

Every sample runs in a fresh child process. The implementation order
alternates between pairs, thread limits apply to Polars and common numerical
backends, and timeouts and process failures are recorded rather than discarded.
The report contains raw samples, operation and total times, peak RSS, exact
package versions, selected types and timestamps, and a repeated-access
break-even estimate.

Each completed pair must produce the same canonical structural summary before
its timings are accepted. The gate compares row counts, identity-column
summaries, relation structure, qualifiers, time bounds, and flattened-log
structure. Initial object-state rows are excluded from the structural gate
because oceldb represents them as state rows while PM4Py stores them on the
object table.

r4pm 0.5.5 participates in ingestion, repeated loading, and flattening. Its
Python API does not expose induced-sublog equivalents for the event-type,
object-type, timestamp, or relation-count filters, so those cells are reported
as unsupported. The harness does not implement substitute filters on r4pm's
behalf.

The workloads deliberately distinguish two timings:

- `operation_seconds` measures the operation plus complete result reduction
  after its input is ready;
- `total_seconds` additionally includes opening the native snapshot for
  oceldb or re-reading the exchange file for a competitor.

The `ingest` workload is inherently architectural: PM4Py and r4pm's DataFrame
API create eager in-memory OCELs, while oceldb creates a durable native
snapshot. It is reported as such and must not be described as an equivalent
persistence benchmark. `--validate-ingest` enables oceldb's full logical
validation, but the competitors have no equivalent step in this harness, so
validated and unvalidated reports must not be mixed.
