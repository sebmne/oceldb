# Core benchmark methodology

The benchmark suite protects the performance and memory contract of the
`OCEL` handle, its lazy operations, and the native storage format. It is a
regression tool, not a claim about all real OCEL workloads.

## Synthetic data model

Generation uses deterministic, vectorized Polars batches and stages those
batches as Parquet before invoking the real native writer. This bounds source
generation memory while ensuring the generated snapshot goes through the same
public write path as user data.

The three standard profiles scale row counts, type cardinality, attribute
width, state histories, and relations together. They include:

- skewed event and object type distributions;
- sparse attributes whose presence depends on type;
- high-cardinality numeric and string values;
- initial object states and subsequent single-field changes;
- typed and qualified E2O and O2O relations; and
- relationless events and objects to exercise induced-sublog semantics.

The generated data is deterministic for a given profile and seed. Its value
distribution is synthetic and should not be used to predict compression ratios
for a particular production log.

## Workloads

| Workload | Contract under test |
| --- | --- |
| `open` | Manifest, schemas, and Parquet metadata without row materialization |
| `validate` | Complete logical integrity validation |
| `event_full_scan` | Full event scan and aggregation |
| `event_typed_scan` | Type-partition-pruned event scan |
| `e2o_filtered_scan` | Denormalized relation filter by object type |
| `o2o_filtered_scan` | Denormalized relation filter by source type |
| `object_states` | Forward-filled object state reconstruction |
| `filter_event_type` | Event-type induced sublog across all five tables |
| `filter_event_count` | Event filter based on related object counts |
| `filter_object_event_count` | Object filter based on related event counts |
| `view` | Composed event/object-type view |
| `project` | Single-object projection |
| `flatten` | Classical flattening for one object type |
| `native_rewrite` | Transactional copy of an unchanged native handle |
| `filtered_rewrite` | Native write after a lazy transformation |

Every case is executed in a fresh child process and fully consumes its result.
Timing starts before `OCEL.open()`, so each number represents an end-to-end
public API action. Peak RSS includes interpreter startup, the opened handle,
Polars execution, and result reduction. It is a high-water mark rather than an
incremental allocation measurement.

## Reproducible comparisons

Use the same persistent dataset for before-and-after runs:

```bash
uv run python benchmarks/benchmark.py generate /tmp/oceldb-small \
  --profile small \
  --validate

uv run python benchmarks/benchmark.py run /tmp/oceldb-small \
  --rounds 5 \
  --warmups 1 \
  --threads 1 \
  --include-plans \
  --output /tmp/before.json
```

After the code change, repeat only the `run` command and write a second report.
Do not regenerate between paired measurements. Avoid other CPU-, memory-, and
disk-intensive activity while measuring.

For a release:

1. Run the `small` profile as a fast regression gate.
2. Run `medium` with at least five measured rounds.
3. Inspect optimized plans for lost partition or predicate pruning.
4. Add a representative real OCEL when one can be used reproducibly.
5. Record the JSON reports with the commit and native format version.

The report captures dataset size, library versions, Python and platform
metadata, thread count, raw samples, medians, ranges, and peak RSS. Compare
numbers only when those inputs match. Cache state and normal system variance
still apply; investigate sustained changes across multiple samples rather than
a single outlier.
