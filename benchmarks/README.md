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
