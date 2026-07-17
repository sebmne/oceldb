"""Reproducible native-storage benchmarks for oceldb.

Run ``python benchmarks/benchmark.py --help`` from the repository root.
"""

import argparse
import gc
import json
import math
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.parse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from oceldb import OCEL
from oceldb.operations.filters import (
    filter_events_by_time,
    filter_events_by_type,
    filter_objects_by_event_count,
)
from oceldb.io import import_ocel
from oceldb.transformations import flatten, view

JSON = dict[str, Any]
CASES = (
    "open",
    "validate",
    "describe",
    "event_count",
    "event_materialize",
    "direct_event_materialize",
    "event_type_count",
    "direct_event_type_count",
    "event_time_window",
    "filter_event_type_core",
    "stored_view_events",
    "stored_view_html",
    "materialize_view",
    "materialized_view_events",
    "filter_object_event_count_core",
    "object_states",
    "flatten",
    "native_rewrite",
    "filtered_native_rewrite",
    "native_streaming_rewrite",
)


def synthetic_ocel(
    *,
    events: int,
    objects: int,
    relations_per_event: int,
    changes_per_object: int,
    event_types: int,
    object_types: int,
) -> OCEL:
    """Build a deterministic, vectorized synthetic OCEL without Python rows."""
    _positive("events", events)
    _positive("objects", objects)
    _positive("relations_per_event", relations_per_event)
    _positive("changes_per_object", changes_per_object)
    _positive("event_types", event_types)
    _positive("object_types", object_types)

    event_index = _indices(events)
    event_frame = event_index.with_columns(
        _identifier("e", pl.col("_index")).alias("ocel_id"),
        (
            pl.datetime(2024, 1, 1, time_zone="UTC")
            + pl.duration(seconds=pl.col("_index"))
        ).alias("ocel_time"),
        (pl.col("_index") % 10_000).cast(pl.Float64).alias("value"),
        _type_name("event", pl.col("_index") % event_types).alias("ocel_type"),
    ).select("ocel_id", "ocel_time", "value", "ocel_type")

    object_index = _indices(objects)
    object_frame = object_index.with_columns(
        _identifier("o", pl.col("_index")).alias("ocel_id"),
        _type_name("object", pl.col("_index") % object_types).alias("ocel_type"),
    ).select("ocel_id", "ocel_type")

    change_frame = (
        _indices(objects * changes_per_object)
        .with_columns(
            (pl.col("_index") // changes_per_object).alias("_object"),
            (pl.col("_index") % changes_per_object).alias("_change"),
        )
        .with_columns(
            _identifier("o", pl.col("_object")).alias("ocel_id"),
            (
                pl.datetime(2023, 1, 1, time_zone="UTC")
                + pl.duration(days=pl.col("_change"))
            ).alias("ocel_time"),
            pl.lit("state").alias("ocel_changed_field"),
            _type_name("state", pl.col("_change")).alias("state"),
            _type_name("object", pl.col("_object") % object_types).alias("ocel_type"),
        )
        .select(
            "ocel_id",
            "ocel_time",
            "ocel_changed_field",
            "state",
            "ocel_type",
        )
    )

    relation_frame = (
        _indices(events * relations_per_event)
        .with_columns(
            (pl.col("_index") // relations_per_event).alias("_event"),
            (pl.col("_index") % relations_per_event).alias("_slot"),
        )
        .with_columns(
            ((pl.col("_event") * 31 + pl.col("_slot") * 9_973) % objects).alias(
                "_object"
            )
        )
        .with_columns(
            _identifier("e", pl.col("_event")).alias("ocel_event_id"),
            _type_name("event", pl.col("_event") % event_types).alias(
                "ocel_event_type"
            ),
            _identifier("o", pl.col("_object")).alias("ocel_object_id"),
            _type_name("object", pl.col("_object") % object_types).alias(
                "ocel_object_type"
            ),
            _type_name("role", pl.col("_slot")).alias("ocel_qualifier"),
        )
        .select(
            "ocel_event_id",
            "ocel_event_type",
            "ocel_object_id",
            "ocel_object_type",
            "ocel_qualifier",
        )
    )

    o2o_frame = (
        object_index.with_columns(((pl.col("_index") + 1) % objects).alias("_target"))
        .with_columns(
            _identifier("o", pl.col("_index")).alias("ocel_source_id"),
            _type_name("object", pl.col("_index") % object_types).alias(
                "ocel_source_type"
            ),
            _identifier("o", pl.col("_target")).alias("ocel_target_id"),
            _type_name("object", pl.col("_target") % object_types).alias(
                "ocel_target_type"
            ),
            pl.lit("next").alias("ocel_qualifier"),
        )
        .select(
            "ocel_source_id",
            "ocel_source_type",
            "ocel_target_id",
            "ocel_target_type",
            "ocel_qualifier",
        )
    )

    return OCEL.from_frames(
        events=event_frame.lazy(),
        objects=object_frame.lazy(),
        object_changes=change_frame.lazy(),
        event_object=relation_frame.lazy(),
        object_object=o2o_frame.lazy(),
    )


def generate_dataset(path: Path, args: argparse.Namespace) -> JSON:
    log = synthetic_ocel(
        events=args.events,
        objects=args.objects,
        relations_per_event=args.relations_per_event,
        changes_per_object=args.changes_per_object,
        event_types=args.event_types,
        object_types=args.object_types,
    )
    started = time.perf_counter()
    log.write(path, overwrite=args.overwrite)
    elapsed = time.perf_counter() - started
    return {
        "events": args.events,
        "objects": args.objects,
        "object_changes": args.objects * args.changes_per_object,
        "e2o": args.events * args.relations_per_event,
        "o2o": args.objects,
        "seconds": elapsed,
        "rows_per_second": _total_rows(args) / elapsed,
        "storage_bytes": directory_size(path),
    }


def dataset_context(path: Path) -> JSON:
    log = OCEL.open(path)
    summary = log.describe()
    change_counts = _type_counts(log.object_changes())
    relation_counts = _type_counts(log.event_object(), column="ocel_object_type")
    event_type = _largest(summary.event_types)
    state_type = _largest(change_counts) or _largest(summary.object_types)
    flatten_type = _largest(relation_counts) or _largest(summary.object_types)
    event_schema = log.events().collect_schema().names()

    start = summary.start_time
    end = summary.end_time
    if start is not None and end is not None:
        span = end - start
        window_start = start + span * 0.4
        window_end = start + span * 0.6
    else:
        window_start = window_end = None

    return {
        "events": summary.events,
        "objects": summary.objects,
        "object_changes": summary.object_changes,
        "e2o": summary.e2o,
        "o2o": summary.o2o,
        "total_rows": sum(
            (
                summary.events,
                summary.objects,
                summary.object_changes,
                summary.e2o,
                summary.o2o,
            )
        ),
        "event_type": event_type,
        "event_type_rows": summary.event_types.get(event_type, 0),
        "state_type": state_type,
        "state_type_rows": change_counts.get(state_type, 0),
        "flatten_type": flatten_type,
        "flatten_type_rows": relation_counts.get(flatten_type, 0),
        "window_start": window_start.isoformat() if window_start else None,
        "window_end": window_end.isoformat() if window_end else None,
        "has_event_value": "value" in event_schema,
        "storage_bytes": directory_size(path),
    }


def benchmark_dataset(
    path: Path,
    *,
    rounds: int,
    warmups: int,
    threads: int | None,
) -> JSON:
    context = dataset_context(path)
    cases: list[str] = list(CASES)
    if context["has_event_value"]:
        cases[4:4] = ["event_value_sum", "direct_event_value_sum"]

    results: list[JSON] = []
    for case in cases:
        case_rounds = (
            min(rounds, 2)
            if case
            in {
                "native_rewrite",
                "filtered_native_rewrite",
                "native_streaming_rewrite",
            }
            else rounds
        )
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "_worker",
            str(path.resolve()),
            case,
            "--rounds",
            str(case_rounds),
            "--warmups",
            str(warmups),
            "--context",
            json.dumps(context),
        ]
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        if threads is not None:
            environment["POLARS_MAX_THREADS"] = str(threads)
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        result = json.loads(completed.stdout)
        rows = work_rows(case, context)
        result["work_rows"] = rows
        result["million_rows_per_second"] = (
            rows / (result["median_ms"] / 1_000) / 1_000_000 if rows else None
        )
        results.append(result)

    return {
        "environment": environment_info(threads),
        "dataset": context,
        "results": results,
    }


def benchmark_sqlite_import(
    source: Path,
    *,
    rounds: int,
    warmups: int,
    threads: int | None,
    validation: str,
) -> JSON:
    context = {"source_bytes": source.stat().st_size, "validation": validation}
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "_worker",
        str(source.resolve()),
        "sqlite_import",
        "--rounds",
        str(rounds),
        "--warmups",
        str(warmups),
        "--context",
        json.dumps(context),
    ]
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    if threads is not None:
        environment["POLARS_MAX_THREADS"] = str(threads)
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )
    return {
        "environment": environment_info(threads),
        "source": {"path": str(source), **context},
        "results": [json.loads(completed.stdout)],
    }


def run_worker(args: argparse.Namespace) -> None:
    context = json.loads(args.context)
    path = Path(args.dataset)
    with tempfile.TemporaryDirectory(prefix="oceldb-benchmark-worker-") as raw:
        operation = benchmark_operation(args.case, path, context, Path(raw))
        for _ in range(args.warmups):
            operation()
        timings: list[float] = []
        result: object = None
        for _ in range(args.rounds):
            gc.collect()
            started = time.perf_counter_ns()
            result = operation()
            timings.append((time.perf_counter_ns() - started) / 1_000_000)
        ordered = sorted(timings)
        p95_index = max(0, math.ceil(0.95 * len(ordered)) - 1)
        output = {
            "case": args.case,
            "rounds": args.rounds,
            "median_ms": statistics.median(timings),
            "min_ms": min(timings),
            "p95_ms": ordered[p95_index],
            "peak_rss_mib": peak_rss_mib(),
            "result": result,
        }
        print(json.dumps(output, default=str))


def benchmark_operation(
    case: str, path: Path, context: JSON, scratch: Path
) -> Callable[[], object]:
    if case == "sqlite_import":
        target = scratch / "imported"
        return lambda: _import_sqlite(path, target, str(context["validation"]))
    if case == "open":
        return lambda: int(OCEL.open(path) is not None)

    log = OCEL.open(path)
    event_type = str(context["event_type"])
    state_type = str(context["state_type"])
    flatten_type = str(context["flatten_type"])

    if case == "validate":
        return lambda: _validate(log)
    if case == "describe":
        return lambda: log._replace().describe().events
    if case == "event_count":
        return lambda: _count(log.events())
    if case == "event_materialize":
        return lambda: log.events().collect().height
    if case == "direct_event_materialize":
        glob = str(path / "events" / "ocel_type=*" / "data.parquet")
        return lambda: pl.scan_parquet(glob, hive_partitioning=True).collect().height
    if case == "event_value_sum":
        return lambda: log.events().select(pl.col("value").sum()).collect().item()
    if case == "direct_event_value_sum":
        glob = str(path / "events" / "ocel_type=*" / "data.parquet")
        return lambda: (
            pl.scan_parquet(glob).select(pl.col("value").sum()).collect().item()
        )
    if case == "event_type_count":
        return lambda: _count(log.events(event_type))
    if case == "direct_event_type_count":
        encoded = urllib.parse.quote(event_type, safe="")
        file = path / "events" / f"ocel_type={encoded}" / "data.parquet"
        return lambda: _count(pl.scan_parquet(file))
    if case == "event_time_window":
        return lambda: _count(
            filter_events_by_time(
                log,
                start=str(context["window_start"]),
                end=str(context["window_end"]),
            ).events()
        )
    if case == "filter_event_type_core":
        return lambda: _all_counts(filter_events_by_type(log, event_type))
    if case == "stored_view_events":
        filtered = view(
            log,
            event_types=event_type,
            object_types=flatten_type,
        )
        return lambda: filtered.events().collect().height
    if case == "stored_view_html":
        return lambda: len(
            view(
                log,
                event_types=event_type,
                object_types=flatten_type,
            )._repr_html_()
        )
    if case == "materialize_view":
        return lambda: (
            view(
                log,
                event_types=event_type,
                object_types=flatten_type,
            )
            .materialize()
            .describe()
            .events
        )
    if case == "materialized_view_events":
        filtered = view(
            log,
            event_types=event_type,
            object_types=flatten_type,
        ).materialize()
        return lambda: filtered.events().collect().height
    if case == "filter_object_event_count_core":
        return lambda: _all_counts(filter_objects_by_event_count(log, min_count=2))
    if case == "object_states":
        return lambda: log.object_states(state_type).collect().height
    if case == "flatten":
        return lambda: flatten(log, flatten_type).collect().height
    if case == "native_rewrite":
        target = scratch / "rewrite"
        return lambda: _rewrite(log, target)
    if case == "filtered_native_rewrite":
        target = scratch / "filtered-rewrite"
        filtered = filter_events_by_type(log, event_type)
        return lambda: _rewrite(filtered, target)
    if case == "native_streaming_rewrite":
        target = scratch / "streaming-rewrite"
        return lambda: _streaming_rewrite(log, target)
    raise ValueError(f"Unknown benchmark case: {case}")


def print_report(payload: JSON) -> None:
    context = payload["dataset"]
    environment = payload["environment"]
    print(
        f"Dataset: {context['events']:,} events, {context['objects']:,} objects, "
        f"{context['e2o']:,} E2O, {_mib(context['storage_bytes']):.1f} MiB"
    )
    print(
        f"Runtime: Python {environment['python']}, Polars {environment['polars']}, "
        f"DuckDB {environment['duckdb']}, {environment['threads']} Polars threads"
    )
    print()
    print(
        f"{'case':36} {'median ms':>10} {'p95 ms':>10} "
        f"{'peak MiB':>10} {'M rows/s':>10}"
    )
    print("-" * 82)
    for result in payload["results"]:
        throughput = result["million_rows_per_second"]
        throughput_text = f"{throughput:.2f}" if throughput is not None else "-"
        print(
            f"{result['case']:36} {result['median_ms']:10.2f} "
            f"{result['p95_ms']:10.2f} {result['peak_rss_mib']:10.1f} "
            f"{throughput_text:>10}"
        )


def print_sqlite_report(payload: JSON) -> None:
    source = payload["source"]
    result = payload["results"][0]
    print(f"SQLite source: {_mib(source['source_bytes']):.1f} MiB ({source['path']})")
    print(
        f"sqlite_import ({source['validation']} validation): "
        f"median {result['median_ms']:.2f} ms, "
        f"p95 {result['p95_ms']:.2f} ms, peak {result['peak_rss_mib']:.1f} MiB"
    )


def work_rows(case: str, context: JSON) -> int | None:
    if case in {
        "event_count",
        "event_materialize",
        "direct_event_materialize",
        "event_value_sum",
        "direct_event_value_sum",
    }:
        return int(context["events"])
    if case in {"event_type_count", "direct_event_type_count"}:
        return int(context["event_type_rows"])
    if case == "event_time_window":
        return int(context["events"])
    if case == "object_states":
        return int(context["state_type_rows"])
    if case == "flatten":
        return int(context["flatten_type_rows"])
    if case in {
        "validate",
        "describe",
        "native_rewrite",
        "filtered_native_rewrite",
        "native_streaming_rewrite",
    }:
        return int(context["total_rows"])
    return None


def environment_info(threads: int | None) -> JSON:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "polars": pl.__version__,
        "duckdb": duckdb.__version__,
        "threads": threads if threads is not None else pl.thread_pool_size(),
    }


def peak_rss_mib() -> float:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return float(value) / divisor


def directory_size(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _indices(count: int) -> pl.DataFrame:
    return pl.DataFrame({"_index": pl.arange(0, count, eager=True, dtype=pl.Int64)})


def _identifier(prefix: str, index: pl.Expr) -> pl.Expr:
    return pl.concat_str([pl.lit(prefix), index.cast(pl.String)])


def _type_name(prefix: str, index: pl.Expr) -> pl.Expr:
    return pl.concat_str([pl.lit(f"{prefix}_"), index.cast(pl.String)])


def _type_counts(frame: pl.LazyFrame, *, column: str = "ocel_type") -> dict[str, int]:
    counts = frame.group_by(column).len().collect()
    return {
        str(name): int(count) for name, count in counts.iter_rows() if name is not None
    }


def _largest(counts: Mapping[str, int]) -> str:
    return max(counts.items(), key=lambda item: item[1])[0] if counts else ""


def _count(frame: pl.LazyFrame) -> int:
    return int(frame.select(pl.len()).collect().item())


def _all_counts(log: OCEL) -> int:
    return sum(
        _count(frame)
        for frame in (
            log.events(),
            log.objects(),
            log.object_changes(),
            log.event_object(),
            log.object_object(),
        )
    )


def _validate(log: OCEL) -> int:
    log.validate()
    return 1


def _rewrite(log: OCEL, target: Path) -> int:
    log.write(target, overwrite=True)
    return directory_size(target)


def _streaming_rewrite(log: OCEL, target: Path) -> int:
    detached = OCEL.from_frames(
        events=log.events(),
        objects=log.objects(),
        object_changes=log.object_changes(),
        event_object=log.event_object(),
        object_object=log.object_object(),
        metadata=log.metadata,
    )
    detached.write(target, overwrite=True)
    return directory_size(target)


def _import_sqlite(source: Path, target: Path, validation: str) -> int:
    import_ocel(source, target, overwrite=True, validation=validation)  # type: ignore[arg-type]
    return directory_size(target)


def _positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive.")


def _total_rows(args: argparse.Namespace) -> int:
    return (
        args.events
        + args.objects
        + args.objects * args.changes_per_object
        + args.events * args.relations_per_event
        + args.objects
    )


def _mib(value: int) -> float:
    return value / (1024 * 1024)


def add_profile_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--events", type=int, default=250_000)
    parser.add_argument("--objects", type=int, default=75_000)
    parser.add_argument("--relations-per-event", type=int, default=3)
    parser.add_argument("--changes-per-object", type=int, default=2)
    parser.add_argument("--event-types", type=int, default=8)
    parser.add_argument("--object-types", type=int, default=6)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="write a synthetic native dataset")
    generate.add_argument("dataset", type=Path)
    add_profile_arguments(generate)
    generate.add_argument("--overwrite", action="store_true")

    run = commands.add_parser("run", help="benchmark an existing native dataset")
    run.add_argument("dataset", type=Path)
    run.add_argument("--rounds", type=int, default=5)
    run.add_argument("--warmups", type=int, default=1)
    run.add_argument("--threads", type=int)
    run.add_argument("--output", type=Path)

    sqlite = commands.add_parser(
        "sqlite-import", help="benchmark SQLite-to-native import"
    )
    sqlite.add_argument("source", type=Path)
    sqlite.add_argument("--rounds", type=int, default=3)
    sqlite.add_argument("--warmups", type=int, default=1)
    sqlite.add_argument("--threads", type=int)
    sqlite.add_argument(
        "--validation", choices=("strict", "warn", "none"), default="none"
    )
    sqlite.add_argument("--output", type=Path)

    suite = commands.add_parser(
        "suite", help="generate and benchmark a temporary dataset"
    )
    add_profile_arguments(suite)
    suite.add_argument("--rounds", type=int, default=5)
    suite.add_argument("--warmups", type=int, default=1)
    suite.add_argument("--threads", type=int)
    suite.add_argument("--output", type=Path)
    suite.set_defaults(overwrite=False)

    worker = commands.add_parser("_worker", help=argparse.SUPPRESS)
    worker.add_argument("dataset")
    worker.add_argument("case")
    worker.add_argument("--rounds", type=int, required=True)
    worker.add_argument("--warmups", type=int, required=True)
    worker.add_argument("--context", required=True)
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "_worker":
        run_worker(args)
        return
    if args.command == "generate":
        print(json.dumps(generate_dataset(args.dataset, args), indent=2))
        return
    if args.command == "run":
        payload = benchmark_dataset(
            args.dataset,
            rounds=args.rounds,
            warmups=args.warmups,
            threads=args.threads,
        )
    elif args.command == "sqlite-import":
        payload = benchmark_sqlite_import(
            args.source,
            rounds=args.rounds,
            warmups=args.warmups,
            threads=args.threads,
            validation=args.validation,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="oceldb-benchmark-suite-") as raw:
            dataset = Path(raw) / "synthetic"
            generation = generate_dataset(dataset, args)
            payload = benchmark_dataset(
                dataset,
                rounds=args.rounds,
                warmups=args.warmups,
                threads=args.threads,
            )
            payload["generation"] = generation
    if args.command == "sqlite-import":
        print_sqlite_report(payload)
    else:
        print_report(payload)
    if args.output is not None:
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
