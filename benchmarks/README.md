# Benchmarks

The benchmark harness is standalone and adds no runtime or test dependency. It
generates deterministic OCEL data with Polars, writes the native partitioned
Parquet layout, and runs each measured operation in a separate process. Peak
RSS therefore includes native allocations made by Polars and DuckDB.

Run the default synthetic suite:

```bash
uv run python benchmarks/benchmark.py suite --output /tmp/oceldb-benchmark.json
```

Benchmark an existing native dataset:

```bash
uv run python benchmarks/benchmark.py run path/to/native-log
```

Benchmark the direct SQLite-to-native import path:

```bash
uv run python benchmarks/benchmark.py sqlite-import path/to/log.sqlite
```

This throughput command defaults to `--validation none`; pass
`--validation strict` to include the input-quality checks.

Generate a reusable larger dataset and benchmark it separately:

```bash
uv run python benchmarks/benchmark.py generate /tmp/oceldb-large \
  --events 1000000 --objects 300000 --overwrite
uv run python benchmarks/benchmark.py run /tmp/oceldb-large --rounds 5
```

Use `--threads 1` to measure single-threaded behavior. Without it, Polars uses
its default thread pool. Query results are warm-cache medians after the selected
number of `--warmups`; the harness does not claim cold-disk timings.

The direct Polars cases are deliberately included as baselines. They reveal how
much overhead comes from oceldb's validation, type reconstruction, and pruning
rather than Parquet scanning itself.

`stored_view_events` and `stored_view_html` retain a composed event/object-type
view before consuming it. They guard the interactive notebook path against
accidentally rebuilding nested pruning selections for every display.
`materialize_view` measures the one-time transition to an in-memory OCEL, while
`materialized_view_events` measures repeated access after that boundary.

`native_rewrite` measures the provenance-aware path for an opened native log.
`filtered_native_rewrite` measures a common transformed path that retains
per-type partition plans but can no longer copy the original table directory.
`native_streaming_rewrite` detaches the same lazy frames first, forcing the
general transformed/in-memory streaming writer. Keeping both prevents native
file reuse from hiding fallback writer costs.

The latest measured results and interpretation are documented in
[`docs/benchmarks.md`](../docs/benchmarks.md).

Large source logs are local benchmark inputs and are intentionally ignored by
Git; place SQLite logs directly in this directory when using them.
