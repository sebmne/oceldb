"""Correctness-gated, fresh-process comparison with OCEL competitors.

Run ``python benchmarks/competitor_comparison.py --help`` from the repository
root. Competitors are optional benchmark dependencies; install them with
``uv sync --group benchmark``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from typing import Any, Literal

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on Windows
    resource = None

JSON = dict[str, Any]
Implementation = Literal["oceldb", "pm4py", "r4pm"]


@dataclass(frozen=True)
class Workload:
    """One semantically matched end-to-end workload."""

    name: str
    description: str


WORKLOADS = (
    Workload(
        "ingest",
        "exchange-file ingestion to a query-ready representation",
    ),
    Workload(
        "load",
        "repeated source/native load and complete logical-table summary",
    ),
    Workload(
        "event_type_filter",
        "largest event-type induced sublog and summary",
    ),
    Workload(
        "object_type_filter",
        "largest object-type induced sublog and summary",
    ),
    Workload(
        "time_filter",
        "middle half of the event-time range and induced-sublog summary",
    ),
    Workload(
        "object_count_filter",
        "events related to at least one object of the selected type",
    ),
    Workload(
        "flatten",
        "classical flattening on the largest object type and summary",
    ),
)
WORKLOAD_NAMES = tuple(workload.name for workload in WORKLOADS)
IMPLEMENTATIONS: tuple[Implementation, ...] = ("oceldb", "pm4py", "r4pm")
SUPPORTED_IMPLEMENTATIONS: dict[str, tuple[Implementation, ...]] = {
    "ingest": IMPLEMENTATIONS,
    "load": IMPLEMENTATIONS,
    "event_type_filter": ("oceldb", "pm4py"),
    "object_type_filter": ("oceldb", "pm4py"),
    "time_filter": ("oceldb", "pm4py"),
    "object_count_filter": ("oceldb", "pm4py"),
    "flatten": IMPLEMENTATIONS,
}


def run_comparison(
    source: Path,
    native: Path,
    *,
    source_format: str,
    selectors: JSON,
    cases: tuple[str, ...],
    rounds: int,
    warmups: int,
    threads: int,
    timeout: float | None,
    batch_size: int,
    validate_ingest: bool,
) -> JSON:
    """Run paired samples and verify equivalent result signatures."""
    if rounds < 1:
        raise ValueError("rounds must be positive.")
    if warmups < 0:
        raise ValueError("warmups must be non-negative.")
    if threads < 1:
        raise ValueError("threads must be positive.")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be positive.")
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")
    unknown = sorted(set(cases) - set(WORKLOAD_NAMES))
    if unknown:
        raise ValueError(f"Unknown workloads: {unknown}.")

    measurements: list[JSON] = []
    for case_index, case in enumerate(cases):
        for warmup in range(warmups):
            paired = _run_pair(
                source,
                native,
                source_format=source_format,
                selectors=selectors,
                case=case,
                pair_index=case_index + warmup,
                threads=threads,
                timeout=timeout,
                batch_size=batch_size,
                validate_ingest=validate_ingest,
            )
            _check_signatures(case, paired)

        samples: dict[Implementation, list[JSON]] = {
            implementation: [] for implementation in IMPLEMENTATIONS
        }
        for round_index in range(rounds):
            paired = _run_pair(
                source,
                native,
                source_format=source_format,
                selectors=selectors,
                case=case,
                pair_index=case_index + warmups + round_index,
                threads=threads,
                timeout=timeout,
                batch_size=batch_size,
                validate_ingest=validate_ingest,
            )
            _check_signatures(case, paired)
            for implementation, sample in paired.items():
                samples[implementation].append(sample)

        measurements.append(
            {
                "case": case,
                "description": _workload(case).description,
                "implementations": {
                    implementation: _summarize_samples(samples[implementation])
                    for implementation in IMPLEMENTATIONS
                },
            }
        )

    return {
        "schema_version": 2,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "comparison_scope": (
            "OCEL exchange ingestion, repeated access, semantically matched "
            "filters, and flattening; unsupported workloads are not synthesized"
        ),
        "dataset": {
            "source": str(source.resolve()),
            "source_format": source_format,
            "source_bytes": source.stat().st_size,
            "native": str(native.resolve()),
            "native_bytes": _directory_bytes(native),
        },
        "configuration": {
            "rounds": rounds,
            "warmups": warmups,
            "threads": threads,
            "timeout_seconds": timeout,
            "batch_size": batch_size,
            "validate_oceldb_ingest": validate_ingest,
            "correctness_gate": "canonical structural summaries must match",
        },
        "selectors": selectors,
        "environment": _environment(),
        "measurements": measurements,
        "amortization": _amortization(measurements),
        "caveats": [
            (
                "PM4Py and r4pm DataFrame ingestion create eager in-memory OCELs; "
                "oceldb ingestion creates a durable native snapshot."
            ),
            (
                "Operation time excludes input setup. Total time includes competitor "
                "exchange loading or oceldb native opening."
            ),
            (
                "Peak RSS includes the Python interpreter, imported dependencies, "
                "input setup, operation, and result verification."
            ),
            (
                "The correctness gate compares selected log structure, not every "
                "type-specific event or object attribute value."
            ),
            (
                "r4pm 0.5.5 exposes no equivalent induced-sublog filters, so it is "
                "included only for ingestion, repeated loading, and flattening."
            ),
        ],
    }


def run_child_case(
    implementation: Implementation,
    case: str,
    source: Path,
    native: Path,
    *,
    source_format: str,
    selectors: JSON,
    batch_size: int,
    validate_ingest: bool,
) -> JSON:
    """Execute one implementation and workload in the current process."""
    imported_at = time.perf_counter()
    if implementation == "oceldb":
        from oceldb import OCEL
        from oceldb.io import convert_ocel
        from oceldb.operations import (
            filter_events_by_object_count,
            filter_events_by_time,
            filter_events_by_type,
            filter_objects_by_type,
            flatten,
        )

        imported = time.perf_counter() - imported_at
        if case == "ingest":
            with tempfile.TemporaryDirectory(
                prefix="oceldb-competitor-ingest-"
            ) as directory:
                target = Path(directory) / "native"
                started = time.perf_counter()
                result = convert_ocel(
                    source,
                    target,
                    format=source_format,
                    batch_size=batch_size,
                    validate=validate_ingest,
                )
                signature = _oceldb_signature(result)
                operation = time.perf_counter() - started
                native_bytes = _directory_bytes(target)
            return _completed_sample(
                imported,
                setup=0.0,
                operation=operation,
                signature=signature,
                extra={"native_bytes": native_bytes},
            )

        setup_started = time.perf_counter()
        ocel = OCEL.open(native)
        setup = time.perf_counter() - setup_started
        operation_started = time.perf_counter()
        if case == "load":
            signature = _oceldb_signature(ocel)
        elif case == "event_type_filter":
            signature = _oceldb_signature(
                filter_events_by_type(ocel, str(selectors["event_type"]))
            )
        elif case == "object_type_filter":
            signature = _oceldb_signature(
                filter_objects_by_type(ocel, str(selectors["object_type"]))
            )
        elif case == "time_filter":
            signature = _oceldb_signature(
                filter_events_by_time(
                    ocel,
                    start=str(selectors["time_start"]),
                    end=str(selectors["time_end"]),
                )
            )
        elif case == "object_count_filter":
            signature = _oceldb_signature(
                filter_events_by_object_count(
                    ocel,
                    object_types=str(selectors["object_type"]),
                    min_count=1,
                )
            )
        elif case == "flatten":
            signature = _oceldb_flatten_signature(
                flatten(ocel, str(selectors["object_type"]))
            )
        else:
            raise ValueError(f"Unknown workload {case!r}.")
        return _completed_sample(
            imported,
            setup=setup,
            operation=time.perf_counter() - operation_started,
            signature=signature,
        )

    if implementation == "pm4py":
        import pm4py

        imported = time.perf_counter() - imported_at
        reader = _pm4py_reader(pm4py, source_format)
        if case == "ingest":
            started = time.perf_counter()
            result = reader(str(source))
            signature = _pm4py_signature(result)
            return _completed_sample(
                imported,
                setup=0.0,
                operation=time.perf_counter() - started,
                signature=signature,
            )

        setup_started = time.perf_counter()
        ocel = reader(str(source))
        setup = time.perf_counter() - setup_started
        operation_started = time.perf_counter()
        if case == "load":
            signature = _pm4py_signature(ocel)
        elif case == "event_type_filter":
            signature = _pm4py_signature(
                pm4py.filter_ocel_event_attribute(
                    ocel,
                    ocel.event_activity,
                    [str(selectors["event_type"])],
                )
            )
        elif case == "object_type_filter":
            signature = _pm4py_signature(
                pm4py.filter_ocel_object_attribute(
                    ocel,
                    ocel.object_type_column,
                    [str(selectors["object_type"])],
                )
            )
        elif case == "time_filter":
            signature = _pm4py_signature(
                pm4py.filter_ocel_events_timestamp(
                    ocel,
                    datetime.fromisoformat(str(selectors["time_start"])),
                    datetime.fromisoformat(str(selectors["time_end"])),
                )
            )
        elif case == "object_count_filter":
            signature = _pm4py_signature(
                pm4py.filter_ocel_object_per_type_count(
                    ocel,
                    {str(selectors["object_type"]): 1},
                )
            )
        elif case == "flatten":
            signature = _pm4py_flatten_signature(
                pm4py.ocel_flattening(
                    ocel,
                    str(selectors["object_type"]),
                )
            )
        else:
            raise ValueError(f"Unknown workload {case!r}.")
        return _completed_sample(
            imported,
            setup=setup,
            operation=time.perf_counter() - operation_started,
            signature=signature,
        )

    if implementation == "r4pm":
        import r4pm
        from r4pm import bindings

        imported = time.perf_counter() - imported_at
        if case == "ingest":
            started = time.perf_counter()
            result = r4pm.df.import_ocel(str(source))
            signature = _r4pm_signature(result)
            return _completed_sample(
                imported,
                setup=0.0,
                operation=time.perf_counter() - started,
                signature=signature,
            )

        if case == "load":
            setup_started = time.perf_counter()
            result = r4pm.df.import_ocel(str(source))
            setup = time.perf_counter() - setup_started
            operation_started = time.perf_counter()
            signature = _r4pm_signature(result)
            return _completed_sample(
                imported,
                setup=setup,
                operation=time.perf_counter() - operation_started,
                signature=signature,
            )

        if case == "flatten":
            setup_started = time.perf_counter()
            ocel_id = r4pm.import_item("SlimLinkedOCEL", str(source))
            setup = time.perf_counter() - setup_started
            operation_started = time.perf_counter()
            log_id = bindings.flatten_ocel_on(
                ocel_id,
                str(selectors["object_type"]),
            )
            signature = _r4pm_flatten_signature(r4pm.item_to_df(log_id))
            return _completed_sample(
                imported,
                setup=setup,
                operation=time.perf_counter() - operation_started,
                signature=signature,
            )

        raise ValueError(f"r4pm does not support workload {case!r}.")

    raise ValueError(f"Unknown implementation {implementation!r}.")


def _run_pair(
    source: Path,
    native: Path,
    *,
    source_format: str,
    selectors: JSON,
    case: str,
    pair_index: int,
    threads: int,
    timeout: float | None,
    batch_size: int,
    validate_ingest: bool,
) -> dict[Implementation, JSON]:
    supported = SUPPORTED_IMPLEMENTATIONS[case]
    order = supported if pair_index % 2 == 0 else supported[::-1]
    result: dict[Implementation, JSON] = {}
    for implementation in order:
        result[implementation] = _run_child(
            implementation,
            case,
            source,
            native,
            source_format=source_format,
            selectors=selectors,
            threads=threads,
            timeout=timeout,
            batch_size=batch_size,
            validate_ingest=validate_ingest,
        )
    return result


def _run_child(
    implementation: Implementation,
    case: str,
    source: Path,
    native: Path,
    *,
    source_format: str,
    selectors: JSON,
    threads: int,
    timeout: float | None,
    batch_size: int,
    validate_ingest: bool,
) -> JSON:
    script = Path(__file__).resolve()
    environment = os.environ.copy()
    local_source = str(script.parents[1] / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        os.pathsep.join((local_source, existing)) if existing else local_source
    )
    environment.update(
        {
            "POLARS_MAX_THREADS": str(threads),
            "OMP_NUM_THREADS": str(threads),
            "OPENBLAS_NUM_THREADS": str(threads),
            "MKL_NUM_THREADS": str(threads),
            "NUMEXPR_NUM_THREADS": str(threads),
            "MPLCONFIGDIR": str(
                Path(tempfile.gettempdir()) / "oceldb-competitor-matplotlib"
            ),
        }
    )
    command = [
        sys.executable,
        str(script),
        str(source),
        "--_case",
        implementation,
        case,
        "--native",
        str(native),
        "--format",
        source_format,
        "--selectors",
        json.dumps(selectors, separators=(",", ":")),
        "--batch-size",
        str(batch_size),
    ]
    if validate_ingest:
        command.append("--validate-ingest")
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {
            "status": "timeout",
            "timeout_seconds": timeout,
        }
    if completed.returncode:
        return {
            "status": "error",
            "returncode": completed.returncode,
            "stderr": completed.stderr.strip()[-4_000:],
        }
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError:
        return {
            "status": "error",
            "returncode": completed.returncode,
            "stderr": (
                "Child produced invalid JSON. stdout="
                f"{completed.stdout[-2_000:]!r}; stderr={completed.stderr[-2_000:]!r}"
            ),
        }


def _check_signatures(case: str, paired: dict[Implementation, JSON]) -> None:
    completed = {
        implementation: sample["signature"]
        for implementation, sample in paired.items()
        if sample.get("status") == "completed"
    }
    if len(completed) < 2:
        return
    signatures = list(completed.values())
    if any(signature != signatures[0] for signature in signatures[1:]):
        raise RuntimeError(
            f"Correctness gate failed for {case!r}. "
            + "; ".join(
                f"{implementation}={json.dumps(signature, sort_keys=True)}"
                for implementation, signature in completed.items()
            )
        )


def _summarize_samples(samples: list[JSON]) -> JSON:
    completed = [sample for sample in samples if sample.get("status") == "completed"]
    result: JSON = {
        "supported": bool(samples),
        "status_counts": {
            status: sum(sample.get("status") == status for sample in samples)
            for status in ("completed", "timeout", "error")
        },
        "samples": samples,
    }
    if not completed:
        return result
    for key in (
        "import_seconds",
        "setup_seconds",
        "operation_seconds",
        "total_seconds",
    ):
        values = [float(sample[key]) for sample in completed]
        result[f"median_{key}"] = statistics.median(values)
        result[f"min_{key}"] = min(values)
        result[f"max_{key}"] = max(values)
    peaks = [
        int(peak)
        for sample in completed
        if (peak := sample.get("peak_rss_bytes")) is not None
    ]
    result["max_peak_rss_bytes"] = max(peaks) if peaks else None
    result["signature"] = completed[0]["signature"]
    return result


def _completed_sample(
    imported: float,
    *,
    setup: float,
    operation: float,
    signature: JSON,
    extra: JSON | None = None,
) -> JSON:
    result: JSON = {
        "status": "completed",
        "import_seconds": imported,
        "setup_seconds": setup,
        "operation_seconds": operation,
        "total_seconds": setup + operation,
        "peak_rss_bytes": _peak_rss_bytes(),
        "signature": signature,
    }
    if extra:
        result.update(extra)
    return result


def _oceldb_signature(ocel: object) -> JSON:
    import polars as pl

    from oceldb.core import schema as s

    events = getattr(ocel, "events")()
    objects = getattr(ocel, "objects")()
    changes = getattr(ocel, "object_changes")().filter(~pl.col(s.OCEL_IS_INITIAL))
    e2o = getattr(ocel, "e2o")()
    o2o = getattr(ocel, "o2o")()
    return {
        "events": _polars_signature(
            events,
            (s.OCEL_ID, s.OCEL_TYPE),
            time_column=s.OCEL_TIME,
        ),
        "objects": _polars_signature(objects, (s.OCEL_ID, s.OCEL_TYPE)),
        "object_changes": _polars_signature(
            changes,
            (s.OCEL_ID, s.OCEL_TYPE, s.OCEL_CHANGED_FIELD),
            time_column=s.OCEL_TIME,
        ),
        "e2o": _polars_signature(
            e2o,
            (
                s.OCEL_EVENT_ID,
                s.OCEL_EVENT_TYPE,
                s.OCEL_OBJECT_ID,
                s.OCEL_OBJECT_TYPE,
                s.OCEL_QUALIFIER,
            ),
        ),
        "o2o": _polars_signature(
            o2o,
            (s.OCEL_SOURCE_ID, s.OCEL_TARGET_ID, s.OCEL_QUALIFIER),
        ),
    }


def _pm4py_signature(ocel: object) -> JSON:
    return {
        "events": _pandas_signature(
            getattr(ocel, "events"),
            ("ocel:eid", "ocel:activity"),
            time_column="ocel:timestamp",
        ),
        "objects": _pandas_signature(
            getattr(ocel, "objects"),
            ("ocel:oid", "ocel:type"),
        ),
        "object_changes": _pandas_signature(
            getattr(ocel, "object_changes"),
            ("ocel:oid", "ocel:type", "ocel:field"),
            time_column="ocel:timestamp",
        ),
        "e2o": _pandas_signature(
            getattr(ocel, "relations"),
            (
                "ocel:eid",
                "ocel:activity",
                "ocel:oid",
                "ocel:type",
                "ocel:qualifier",
            ),
        ),
        "o2o": _pandas_signature(
            getattr(ocel, "o2o"),
            ("ocel:oid", "ocel:oid_2", "ocel:qualifier"),
        ),
    }


def _r4pm_signature(ocel: object) -> JSON:
    tables = ocel
    return {
        "events": _polars_signature(
            tables["events"],
            ("ocel:eid", "ocel:activity"),
            time_column="ocel:timestamp",
        ),
        "objects": _polars_signature(
            tables["objects"],
            ("ocel:oid", "ocel:type"),
        ),
        "object_changes": _polars_signature(
            tables["object_changes"],
            ("ocel:oid", "ocel:type", "ocel:field"),
            time_column="ocel:timestamp",
        ),
        "e2o": _polars_signature(
            tables["relations"],
            (
                "ocel:eid",
                "ocel:activity",
                "ocel:oid",
                "ocel:type",
                "ocel:qualifier",
            ),
        ),
        "o2o": _polars_signature(
            tables["o2o"],
            ("ocel:oid", "ocel:oid_2", "ocel:qualifier"),
        ),
    }


def _oceldb_flatten_signature(frame: object) -> JSON:
    return _polars_signature(
        frame,
        ("case:concept:name", "concept:name"),
        time_column="time:timestamp",
    )


def _pm4py_flatten_signature(frame: object) -> JSON:
    return _pandas_signature(
        frame,
        ("case:concept:name", "concept:name"),
        time_column="time:timestamp",
    )


def _r4pm_flatten_signature(frame: object) -> JSON:
    return _polars_signature(
        frame,
        ("case:concept:name", "concept:name"),
        time_column="time:timestamp",
    )


def _polars_signature(
    frame: object,
    columns: Sequence[str],
    *,
    time_column: str | None = None,
) -> JSON:
    import polars as pl

    lazy = frame.lazy() if isinstance(frame, pl.DataFrame) else frame
    expressions: list[pl.Expr] = [pl.len().alias("rows")]
    for index, column in enumerate(columns):
        value = pl.col(column).cast(pl.String)
        expressions.extend(
            (
                value.null_count().alias(f"nulls_{index}"),
                value.str.len_chars().sum().alias(f"chars_{index}"),
                value.min().alias(f"min_{index}"),
                value.max().alias(f"max_{index}"),
            )
        )
    if time_column is not None:
        expressions.extend(
            (
                pl.col(time_column).min().alias("time_min"),
                pl.col(time_column).max().alias("time_max"),
            )
        )
    row = (
        lazy.select(*expressions)
        .collect(engine="streaming")
        .row(
            0,
            named=True,
        )
    )
    return _normalize_signature(row, len(columns), time_column is not None)


def _pandas_signature(
    frame: object,
    columns: Sequence[str],
    *,
    time_column: str | None = None,
) -> JSON:
    result: JSON = {"rows": len(frame)}
    for index, column in enumerate(columns):
        values = frame[column]
        non_null = values.dropna().astype(str)
        result[f"nulls_{index}"] = int(values.isna().sum())
        result[f"chars_{index}"] = int(non_null.str.len().sum())
        result[f"min_{index}"] = None if non_null.empty else str(non_null.min())
        result[f"max_{index}"] = None if non_null.empty else str(non_null.max())
    if time_column is not None:
        values = frame[time_column].dropna()
        result["time_min"] = None if values.empty else _normalize_time(values.min())
        result["time_max"] = None if values.empty else _normalize_time(values.max())
    return result


def _normalize_signature(
    row: dict[str, object],
    columns: int,
    has_time: bool,
) -> JSON:
    result: JSON = {"rows": int(row["rows"] or 0)}
    for index in range(columns):
        result[f"nulls_{index}"] = int(row[f"nulls_{index}"] or 0)
        result[f"chars_{index}"] = int(row[f"chars_{index}"] or 0)
        minimum = row[f"min_{index}"]
        maximum = row[f"max_{index}"]
        result[f"min_{index}"] = None if minimum is None else str(minimum)
        result[f"max_{index}"] = None if maximum is None else str(maximum)
    if has_time:
        result["time_min"] = _normalize_time(row["time_min"])
        result["time_max"] = _normalize_time(row["time_max"])
    return result


def _normalize_time(value: object) -> str | None:
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if not isinstance(value, datetime):
        return str(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _selectors(native: Path) -> JSON:
    import polars as pl

    from oceldb import OCEL
    from oceldb.core import schema as s

    ocel = OCEL.open(native)
    event_type = _largest_type(ocel.events(), s.OCEL_TYPE, "event")
    object_type = _largest_type(ocel.objects(), s.OCEL_TYPE, "object")
    limits = (
        ocel.events()
        .select(
            pl.col(s.OCEL_TIME).min().alias("minimum"),
            pl.col(s.OCEL_TIME).max().alias("maximum"),
        )
        .collect(engine="streaming")
        .row(0, named=True)
    )
    minimum = limits["minimum"]
    maximum = limits["maximum"]
    if not isinstance(minimum, datetime) or not isinstance(maximum, datetime):
        raise ValueError("The benchmark source must contain at least one event.")
    span = maximum - minimum
    return {
        "event_type": event_type,
        "object_type": object_type,
        "time_start": _normalize_time(minimum + span / 4),
        "time_end": _normalize_time(maximum - span / 4),
        "object_count_minimum": 1,
    }


def _largest_type(frame: object, column: str, context: str) -> str:
    import polars as pl

    result = (
        frame.group_by(column)
        .agg(pl.len().alias("_count"))
        .sort("_count", column, descending=[True, False])
        .limit(1)
        .collect(engine="streaming")
    )
    if not result.height:
        raise ValueError(f"The benchmark source has no {context} types.")
    return str(result.item(0, column))


def _prepare_native(
    source: Path,
    target: Path,
    *,
    source_format: str,
    batch_size: int,
) -> JSON:
    from oceldb.io import convert_ocel

    started = time.perf_counter()
    convert_ocel(
        source,
        target,
        format=source_format,
        batch_size=batch_size,
        validate=True,
    )
    return {
        "seconds": time.perf_counter() - started,
        "bytes": _directory_bytes(target),
        "validated": True,
    }


def _pm4py_reader(module: object, source_format: str) -> Callable[[str], object]:
    readers = {
        "sqlite": "read_ocel2_sqlite",
        "json": "read_ocel2_json",
        "xml": "read_ocel2_xml",
    }
    return getattr(module, readers[source_format])


def _amortization(measurements: list[JSON]) -> JSON | None:
    by_case = {measurement["case"]: measurement for measurement in measurements}
    if "ingest" not in by_case or "load" not in by_case:
        return None
    ingestion = by_case["ingest"]["implementations"]["oceldb"]
    oceldb_load = by_case["load"]["implementations"]["oceldb"]
    conversion = ingestion.get("median_operation_seconds")
    native_access = oceldb_load.get("median_total_seconds")
    if conversion is None or native_access is None:
        return None
    result: JSON = {}
    for competitor in ("pm4py", "r4pm"):
        competitor_load = by_case["load"]["implementations"][competitor]
        exchange_access = competitor_load.get("median_total_seconds")
        if exchange_access is None:
            continue
        saving = float(exchange_access) - float(native_access)
        result[competitor] = {
            "formula": (
                f"oceldb_ingest / ({competitor}_repeated_load - oceldb_repeated_load)"
            ),
            "repeated_access_saving_seconds": saving,
            "break_even_repeated_accesses": (
                float(conversion) / saving if saving > 0 else None
            ),
        }
    return result or None


def _workload(name: str) -> Workload:
    return next(workload for workload in WORKLOADS if workload.name == name)


def _peak_rss_bytes() -> int | None:
    if resource is None:
        return None
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum if platform.system() == "Darwin" else maximum * 1_024)


def _directory_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _environment() -> JSON:
    versions = {}
    for package in ("oceldb", "pm4py", "r4pm", "polars", "pandas", "python"):
        if package == "python":
            versions[package] = platform.python_version()
            continue
        versions[package] = _package_version(package)
    return {
        **versions,
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
    }


def _package_version(package: str) -> str | None:
    try:
        return importlib.metadata.version(package)
    except importlib.metadata.PackageNotFoundError:
        return None


def _human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1_024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1_024
    raise AssertionError("unreachable")


def _print_report(report: JSON) -> None:
    print(f"Source: {report['dataset']['source']}")
    print(
        f"Versions: oceldb {report['environment']['oceldb']}, "
        f"PM4Py {report['environment']['pm4py']}, "
        f"r4pm {report['environment']['r4pm']}"
    )
    print()
    print(
        f"{'Workload':<28} {'Library':<8} {'Operation':>12} "
        f"{'Total':>12} {'Peak RSS':>12}"
    )
    print("-" * 77)
    for measurement in report["measurements"]:
        for index, implementation in enumerate(IMPLEMENTATIONS):
            summary = measurement["implementations"][implementation]
            label = measurement["case"] if index == 0 else ""
            operation = summary.get("median_operation_seconds")
            total = summary.get("median_total_seconds")
            peak = summary.get("max_peak_rss_bytes")
            operation_text = "n/a" if operation is None else f"{operation:.3f}s"
            total_text = "n/a" if total is None else f"{total:.3f}s"
            peak_text = "n/a" if peak is None else _human_bytes(int(peak))
            print(
                f"{label:<28} {implementation:<8} {operation_text:>12} "
                f"{total_text:>12} {peak_text:>12}"
            )
    amortization = report.get("amortization")
    if amortization:
        lines = [
            f"{competitor}: {result['break_even_repeated_accesses']:.2f}"
            for competitor, result in amortization.items()
            if result["break_even_repeated_accesses"] is not None
        ]
        if lines:
            print()
            print(
                "Native conversion break-even (repeated accesses): " + ", ".join(lines)
            )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("source", type=Path)
    result.add_argument("--native", type=Path)
    result.add_argument("--format", choices=("sqlite", "json", "xml"))
    result.add_argument(
        "--case",
        action="append",
        choices=WORKLOAD_NAMES,
        dest="cases",
        help="Workload to run; repeat to select multiple (default: all).",
    )
    result.add_argument("--rounds", type=int, default=3)
    result.add_argument("--warmups", type=int, default=1)
    result.add_argument("--threads", type=int, default=1)
    result.add_argument("--timeout", type=float)
    result.add_argument("--batch-size", type=int, default=10_000)
    result.add_argument(
        "--validate-ingest",
        action="store_true",
        help=(
            "Validate oceldb's measured ingestion. The competitors have no "
            "equivalent full logical-validation step, so this changes semantics."
        ),
    )
    result.add_argument("--output", type=Path)
    result.add_argument("--json", action="store_true", dest="json_output")
    result.add_argument("--_case", nargs=2, metavar=("IMPLEMENTATION", "WORKLOAD"))
    result.add_argument("--selectors", help=argparse.SUPPRESS)
    return result


def main() -> None:
    args = parser().parse_args()
    if args._case:
        if args.native is None or args.format is None or args.selectors is None:
            raise ValueError("Internal child invocation is incomplete.")
        implementation, case = args._case
        print(
            json.dumps(
                run_child_case(
                    implementation,
                    case,
                    args.source,
                    args.native,
                    source_format=args.format,
                    selectors=json.loads(args.selectors),
                    batch_size=args.batch_size,
                    validate_ingest=args.validate_ingest,
                ),
                separators=(",", ":"),
            )
        )
        return

    source = args.source.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"OCEL exchange source does not exist: {source}")
    missing = [
        package for package in ("pm4py", "r4pm") if _package_version(package) is None
    ]
    if missing:
        raise RuntimeError(
            f"Missing benchmark dependencies {missing}. "
            "Run `uv sync --group benchmark`."
        )

    from oceldb.io import detect_format

    source_format = detect_format(source) if args.format is None else args.format
    cases = tuple(args.cases or WORKLOAD_NAMES)
    preparation: JSON | None = None

    if args.native is not None:
        native = args.native.resolve()
        if not native.exists():
            native.parent.mkdir(parents=True, exist_ok=True)
            preparation = _prepare_native(
                source,
                native,
                source_format=source_format,
                batch_size=args.batch_size,
            )
        report = run_comparison(
            source,
            native,
            source_format=source_format,
            selectors=_selectors(native),
            cases=cases,
            rounds=args.rounds,
            warmups=args.warmups,
            threads=args.threads,
            timeout=args.timeout,
            batch_size=args.batch_size,
            validate_ingest=args.validate_ingest,
        )
    else:
        with tempfile.TemporaryDirectory(
            prefix="oceldb-competitor-comparison-"
        ) as directory:
            native = Path(directory) / "native"
            preparation = _prepare_native(
                source,
                native,
                source_format=source_format,
                batch_size=args.batch_size,
            )
            report = run_comparison(
                source,
                native,
                source_format=source_format,
                selectors=_selectors(native),
                cases=cases,
                rounds=args.rounds,
                warmups=args.warmups,
                threads=args.threads,
                timeout=args.timeout,
                batch_size=args.batch_size,
                validate_ingest=args.validate_ingest,
            )

    report["native_preparation"] = preparation
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    if args.json_output:
        print(text, end="")
    else:
        _print_report(report)
        if args.output is not None:
            print(f"\nJSON report: {args.output}")


if __name__ == "__main__":
    main()
