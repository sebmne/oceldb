"""Deterministic synthetic benchmarks for oceldb.

Run ``python benchmarks/benchmark.py --help`` from the repository root.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, fields, replace
import importlib.metadata
import json
import os
import platform
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from typing import Any

import polars as pl

from oceldb import OCEL
from oceldb.operations import (
    filter_events_by_object_count,
    filter_events_by_type,
    filter_objects_by_event_count,
    flatten,
    project,
    view,
)

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on Windows
    resource = None

JSON = dict[str, Any]


@dataclass(frozen=True)
class Profile:
    """Shape and distribution of one deterministic synthetic OCEL."""

    name: str
    events: int
    objects: int
    relations_per_event: int
    states_per_object: int
    o2o_per_object: int
    event_types: int
    object_types: int
    event_attributes: int
    object_attributes: int
    qualifiers: int
    relationless_event_fraction: float
    relationless_object_fraction: float
    type_skew: float
    seed: int
    batch_size: int

    @property
    def related_events(self) -> int:
        """Number of events that receive E2O relations."""
        return int(self.events * (1.0 - self.relationless_event_fraction))

    @property
    def related_objects(self) -> int:
        """Number of objects eligible as E2O targets."""
        return int(self.objects * (1.0 - self.relationless_object_fraction))

    @property
    def rows(self) -> dict[str, int]:
        """Expected row count of every logical table."""
        return {
            "events": self.events,
            "objects": self.objects,
            "object_changes": self.objects * self.states_per_object,
            "e2o": self.related_events * self.relations_per_event,
            "o2o": self.objects * self.o2o_per_object,
        }

    def validate(self) -> None:
        """Reject profiles that cannot form a valid, useful OCEL."""
        positive = {
            "events": self.events,
            "objects": self.objects,
            "relations_per_event": self.relations_per_event,
            "states_per_object": self.states_per_object,
            "event_types": self.event_types,
            "object_types": self.object_types,
            "qualifiers": self.qualifiers,
            "batch_size": self.batch_size,
        }
        for name, value in positive.items():
            if isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer.")
        non_negative = {
            "o2o_per_object": self.o2o_per_object,
            "event_attributes": self.event_attributes,
            "object_attributes": self.object_attributes,
        }
        for name, value in non_negative.items():
            if isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer.")
        fractions = {
            "relationless_event_fraction": self.relationless_event_fraction,
            "relationless_object_fraction": self.relationless_object_fraction,
            "type_skew": self.type_skew,
        }
        for name, value in fractions.items():
            if not 0.0 <= value < 1.0:
                raise ValueError(f"{name} must be in [0, 1).")
        if self.event_types > self.events:
            raise ValueError("event_types must not exceed events.")
        if self.object_types > self.objects:
            raise ValueError("object_types must not exceed objects.")
        if self.related_events < 1:
            raise ValueError("At least one event must have an E2O relation.")
        if self.related_objects < self.relations_per_event:
            raise ValueError(
                "The related object population must be at least relations_per_event."
            )
        if self.states_per_object > 1 and self.object_attributes < 1:
            raise ValueError(
                "object_attributes must be positive when states_per_object > 1."
            )


PROFILES: dict[str, Profile] = {
    "small": Profile(
        name="small",
        events=50_000,
        objects=15_000,
        relations_per_event=3,
        states_per_object=4,
        o2o_per_object=1,
        event_types=8,
        object_types=5,
        event_attributes=6,
        object_attributes=6,
        qualifiers=6,
        relationless_event_fraction=0.01,
        relationless_object_fraction=0.02,
        type_skew=0.65,
        seed=17,
        batch_size=100_000,
    ),
    "medium": Profile(
        name="medium",
        events=1_000_000,
        objects=300_000,
        relations_per_event=3,
        states_per_object=6,
        o2o_per_object=2,
        event_types=20,
        object_types=10,
        event_attributes=10,
        object_attributes=10,
        qualifiers=12,
        relationless_event_fraction=0.01,
        relationless_object_fraction=0.02,
        type_skew=0.70,
        seed=17,
        batch_size=250_000,
    ),
    "large": Profile(
        name="large",
        events=10_000_000,
        objects=3_000_000,
        relations_per_event=4,
        states_per_object=8,
        o2o_per_object=2,
        event_types=40,
        object_types=20,
        event_attributes=16,
        object_attributes=16,
        qualifiers=20,
        relationless_event_fraction=0.01,
        relationless_object_fraction=0.02,
        type_skew=0.75,
        seed=17,
        batch_size=500_000,
    ),
}


@dataclass(frozen=True)
class Workload:
    """One named operation executed in an isolated child process."""

    name: str
    description: str


WORKLOADS = (
    Workload("open", "manifest and Parquet-footer open"),
    Workload("validate", "complete logical validation"),
    Workload("event_full_scan", "full event scan and aggregation"),
    Workload("event_typed_scan", "partition-pruned event scan"),
    Workload("e2o_filtered_scan", "denormalized E2O type filter"),
    Workload("o2o_filtered_scan", "denormalized O2O endpoint-type filter"),
    Workload("object_states", "forward-filled state reconstruction"),
    Workload("filter_event_type", "event-type induced sublog"),
    Workload("filter_event_count", "event related-object count filter"),
    Workload("filter_object_event_count", "object related-event count filter"),
    Workload("view", "composed event/object-type view"),
    Workload("project", "single-object projection"),
    Workload("flatten", "one object-type classical flattening"),
    Workload("native_rewrite", "transactional unchanged-native copy"),
    Workload("filtered_rewrite", "filtered OCEL native write"),
)
WORKLOAD_NAMES = tuple(workload.name for workload in WORKLOADS)


@dataclass(frozen=True)
class SyntheticSource:
    """Batched Parquet source tables used as input to the native writer."""

    root: Path
    profile: Profile

    def ocel(self) -> OCEL:
        """Open the generated source parts as one lazy OCEL."""
        return OCEL.from_frames(
            events=_scan_parts(self.root / "events"),
            objects=_scan_parts(self.root / "objects"),
            object_changes=_scan_parts(self.root / "object_changes"),
            e2o=_scan_parts(self.root / "e2o"),
            o2o=_scan_parts(self.root / "o2o"),
        )


def prepare_source(root: Path, profile: Profile) -> SyntheticSource:
    """Write deterministic source tables in bounded in-memory batches."""
    profile.validate()
    root.mkdir(parents=True, exist_ok=True)
    _write_batches(
        root / "events",
        profile.events,
        profile.batch_size,
        lambda start, stop: _event_batch(profile, start, stop),
    )
    _write_batches(
        root / "objects",
        profile.objects,
        profile.batch_size,
        lambda start, stop: _object_batch(profile, start, stop),
    )
    _write_batches(
        root / "object_changes",
        profile.rows["object_changes"],
        profile.batch_size,
        lambda start, stop: _change_batch(profile, start, stop),
    )
    _write_batches(
        root / "e2o",
        profile.rows["e2o"],
        profile.batch_size,
        lambda start, stop: _e2o_batch(profile, start, stop),
    )
    _write_batches(
        root / "o2o",
        profile.rows["o2o"],
        profile.batch_size,
        lambda start, stop: _o2o_batch(profile, start, stop),
    )
    return SyntheticSource(root=root, profile=profile)


def generate_native(
    path: Path,
    profile: Profile,
    *,
    overwrite: bool = False,
    validate: bool = False,
) -> JSON:
    """Generate and write one native synthetic snapshot."""
    profile.validate()
    path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(
        prefix=f".{path.name}.synthetic-source-",
        dir=path.parent,
    ) as directory:
        source = prepare_source(Path(directory), profile)
        source.ocel().write(path, overwrite=overwrite, validate=validate)
    return {
        "path": str(path.resolve()),
        "profile": asdict(profile),
        "rows": profile.rows,
        "seconds": time.perf_counter() - started,
        "bytes": _dataset_bytes(path),
    }


def run_case(path: Path, case: str) -> JSON:
    """Execute one workload and return its wall time and peak RSS."""
    started = time.perf_counter()
    result = _execute_case(path, case)
    return {
        "case": case,
        "seconds": time.perf_counter() - started,
        "peak_rss_bytes": _peak_rss_bytes(),
        "result": result,
    }


def run_benchmarks(
    path: Path,
    *,
    cases: tuple[str, ...],
    rounds: int,
    warmups: int,
    threads: int | None,
    include_plans: bool = False,
) -> JSON:
    """Measure selected workloads in fresh child processes."""
    if rounds < 1:
        raise ValueError("rounds must be positive.")
    if warmups < 0:
        raise ValueError("warmups must be non-negative.")
    if threads is not None and threads < 1:
        raise ValueError("threads must be positive.")
    unknown = sorted(set(cases) - set(WORKLOAD_NAMES))
    if unknown:
        raise ValueError(f"Unknown workloads: {unknown}.")

    measurements: list[JSON] = []
    for case in cases:
        for _ in range(warmups):
            _run_child(path, case, threads=threads)
        samples = [_run_child(path, case, threads=threads) for _ in range(rounds)]
        seconds = [float(sample["seconds"]) for sample in samples]
        peaks = [
            int(peak)
            for sample in samples
            if (peak := sample["peak_rss_bytes"]) is not None
        ]
        measurements.append(
            {
                "case": case,
                "description": _workload(case).description,
                "median_seconds": statistics.median(seconds),
                "min_seconds": min(seconds),
                "max_seconds": max(seconds),
                "max_peak_rss_bytes": max(peaks) if peaks else None,
                "samples": samples,
            }
        )

    report: JSON = {
        "schema_version": 1,
        "created_at": _utc_now(),
        "dataset": {
            "path": str(path.resolve()),
            "bytes": _dataset_bytes(path),
        },
        "environment": _environment(threads),
        "rounds": rounds,
        "warmups": warmups,
        "measurements": measurements,
    }
    if include_plans:
        report["plans"] = query_plans(path)
    return report


def query_plans(path: Path) -> dict[str, str]:
    """Return optimized plans for representative lazy workloads."""
    ocel = OCEL.open(path)
    event_type = _first(ocel.event_types(), "event type")
    object_type = _first(ocel.object_types(), "object type")
    selected_view = view(
        ocel,
        event_types=event_type,
        object_types=object_type,
    )
    return {
        "event_typed_scan": ocel.events(event_type).explain(optimized=True),
        "e2o_filtered_scan": ocel.e2o(object_types=object_type).explain(optimized=True),
        "object_states": ocel.object_states(object_type).explain(optimized=True),
        "view_events": selected_view.events().explain(optimized=True),
        "view_e2o": selected_view.e2o().explain(optimized=True),
    }


def _execute_case(path: Path, case: str) -> object:
    ocel = OCEL.open(path)
    event_type = _first(ocel.event_types(), "event type")
    object_type = _first(ocel.object_types(), "object type")
    if case == "open":
        return {
            "event_types": len(ocel.event_types()),
            "object_types": len(ocel.object_types()),
        }
    if case == "validate":
        ocel.validate()
        return True
    if case == "event_full_scan":
        return _row(
            ocel.events()
            .select(
                pl.len().alias("rows"),
                pl.col("ocel_id").str.len_bytes().sum().alias("id_bytes"),
                pl.col("ocel_time").cast(pl.Int64).min().alias("first_time"),
            )
            .collect(engine="streaming")
        )
    if case == "event_typed_scan":
        return _row(
            ocel.events(event_type)
            .select(
                pl.len().alias("rows"),
                pl.col("ocel_id").str.len_bytes().sum().alias("id_bytes"),
                pl.col("ocel_time").cast(pl.Int64).min().alias("first_time"),
            )
            .collect(engine="streaming")
        )
    if case == "e2o_filtered_scan":
        return _scalar(
            ocel.e2o(object_types=object_type)
            .select(pl.len())
            .collect(engine="streaming")
        )
    if case == "o2o_filtered_scan":
        return _scalar(
            ocel.o2o(source_types=object_type)
            .select(pl.len())
            .collect(engine="streaming")
        )
    if case == "object_states":
        states = ocel.object_states(object_type)
        expressions: list[pl.Expr] = [pl.len().alias("rows")]
        if "object_attr_0" in states.collect_schema():
            expressions.append(pl.col("object_attr_0").sum().alias("state_value"))
        return _row(states.select(*expressions).collect(engine="streaming"))
    if case == "filter_event_type":
        return _consume_ocel(filter_events_by_type(ocel, event_type))
    if case == "filter_event_count":
        return _consume_ocel(
            filter_events_by_object_count(
                ocel,
                min_count=2,
                object_types=object_type,
            )
        )
    if case == "filter_object_event_count":
        return _consume_ocel(
            filter_objects_by_event_count(
                ocel,
                object_types=object_type,
                min_count=2,
            )
        )
    if case == "view":
        return _consume_ocel(
            view(
                ocel,
                event_types=event_type,
                object_types=object_type,
            )
        )
    if case == "project":
        object_id = str(
            _scalar(
                ocel.objects(object_type)
                .select("ocel_id")
                .limit(1)
                .collect(engine="streaming")
            )
        )
        return _consume_ocel(project(ocel, object_id))
    if case == "flatten":
        flattened = flatten(ocel, object_type)
        expressions = [pl.len().alias("rows")]
        if "event_value" in flattened.collect_schema():
            expressions.append(pl.col("event_value").sum().alias("event_value"))
        return _row(flattened.select(*expressions).collect(engine="streaming"))
    if case == "native_rewrite":
        return _rewrite(ocel)
    if case == "filtered_rewrite":
        return _rewrite(filter_events_by_type(ocel, event_type))
    raise ValueError(f"Unknown workload {case!r}.")


def _consume_ocel(ocel: OCEL) -> dict[str, int]:
    """Execute all five tables while retaining only their row counts."""
    return {
        "events": _count(ocel.events()),
        "objects": _count(ocel.objects()),
        "object_changes": _count(ocel.object_changes()),
        "e2o": _count(ocel.e2o()),
        "o2o": _count(ocel.o2o()),
    }


def _rewrite(ocel: OCEL) -> JSON:
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory) / "snapshot"
        ocel.write(target)
        return {"bytes": _dataset_bytes(target)}


def _run_child(path: Path, case: str, *, threads: int | None) -> JSON:
    script = Path(__file__).resolve()
    environment = os.environ.copy()
    source = str(script.parents[1] / "src")
    existing_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        os.pathsep.join((source, existing_pythonpath))
        if existing_pythonpath
        else source
    )
    if threads is not None:
        environment["POLARS_MAX_THREADS"] = str(threads)
    completed = subprocess.run(
        [sys.executable, str(script), "_case", str(path), case],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Workload {case!r} failed with exit code {completed.returncode}:\n"
            f"{completed.stderr.strip()}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Workload {case!r} produced invalid JSON:\n{completed.stdout}"
        ) from exc


def _event_batch(profile: Profile, start: int, stop: int) -> pl.DataFrame:
    frame = _indices(start, stop).with_columns(
        _type_index(
            pl.col("_index"),
            count=profile.event_types,
            skew=profile.type_skew,
            seed=profile.seed,
        ).alias("_type")
    )
    attributes = [
        pl.when(
            ((pl.col("_type") + index) % min(profile.event_types, 3) == 0)
            & ((pl.col("_index") + index + profile.seed) % 5 != 0)
        )
        .then(_numeric_value(pl.col("_index"), index + profile.seed))
        .otherwise(None)
        .cast(pl.Float64)
        .alias(f"event_attr_{index}")
        for index in range(profile.event_attributes)
    ]
    return frame.with_columns(
        _identifier("e", pl.col("_index")).alias("ocel_id"),
        (
            pl.datetime(2024, 1, 1, time_zone="UTC")
            + pl.duration(seconds=pl.col("_index"))
        ).alias("ocel_time"),
        _type_name("event", pl.col("_type")).alias("ocel_type"),
        _numeric_value(pl.col("_index"), profile.seed)
        .cast(pl.Float64)
        .alias("event_value"),
        pl.concat_str(
            pl.lit("code-"),
            _numeric_value(pl.col("_index"), profile.seed + 101).cast(pl.String),
        ).alias("event_code"),
        *attributes,
    ).select(
        "ocel_id",
        "ocel_time",
        "ocel_type",
        "event_value",
        "event_code",
        *(f"event_attr_{index}" for index in range(profile.event_attributes)),
    )


def _object_batch(profile: Profile, start: int, stop: int) -> pl.DataFrame:
    return (
        _indices(start, stop)
        .with_columns(
            _identifier("o", pl.col("_index")).alias("ocel_id"),
            _type_name(
                "object",
                _type_index(
                    pl.col("_index"),
                    count=profile.object_types,
                    skew=profile.type_skew,
                    seed=profile.seed + 1,
                ),
            ).alias("ocel_type"),
        )
        .select("ocel_id", "ocel_type")
    )


def _change_batch(profile: Profile, start: int, stop: int) -> pl.DataFrame:
    frame = (
        _indices(start, stop)
        .with_columns(
            (pl.col("_index") // profile.states_per_object).alias("_object"),
            (pl.col("_index") % profile.states_per_object).alias("_state"),
        )
        .with_columns(
            _type_index(
                pl.col("_object"),
                count=profile.object_types,
                skew=profile.type_skew,
                seed=profile.seed + 1,
            ).alias("_type")
        )
    )
    if profile.object_attributes:
        active_attribute = (
            pl.col("_type") * 2 + pl.col("_state").clip(lower_bound=1) - 1
        ) % profile.object_attributes
        changed_field = (
            pl.when(pl.col("_state") == 0)
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(
                pl.concat_str(
                    pl.lit("object_attr_"),
                    active_attribute.cast(pl.String),
                )
            )
        )
        per_type = min(2, profile.object_attributes)
        attributes = []
        for index in range(profile.object_attributes):
            belongs_to_type = (
                index
                + profile.object_attributes
                - ((pl.col("_type") * 2) % profile.object_attributes)
            ) % profile.object_attributes < per_type
            has_value = ((pl.col("_state") == 0) & belongs_to_type) | (
                (pl.col("_state") > 0) & (active_attribute == index)
            )
            attributes.append(
                pl.when(has_value)
                .then(
                    _numeric_value(
                        pl.col("_object") * profile.states_per_object
                        + pl.col("_state"),
                        profile.seed + index,
                    )
                )
                .otherwise(None)
                .cast(pl.Int64)
                .alias(f"object_attr_{index}")
            )
    else:
        changed_field = pl.lit(None, dtype=pl.String)
        attributes = []
    return frame.with_columns(
        _identifier("o", pl.col("_object")).alias("ocel_id"),
        (
            pl.datetime(2023, 1, 1, time_zone="UTC")
            + pl.duration(days=pl.col("_state"))
        ).alias("ocel_time"),
        changed_field.alias("ocel_changed_field"),
        (pl.col("_state") == 0).alias("ocel_is_initial"),
        _type_name("object", pl.col("_type")).alias("ocel_type"),
        *attributes,
    ).select(
        "ocel_id",
        "ocel_time",
        "ocel_changed_field",
        "ocel_is_initial",
        "ocel_type",
        *(f"object_attr_{index}" for index in range(profile.object_attributes)),
    )


def _e2o_batch(profile: Profile, start: int, stop: int) -> pl.DataFrame:
    return (
        _indices(start, stop)
        .with_columns(
            (pl.col("_index") // profile.relations_per_event).alias("_event"),
            (pl.col("_index") % profile.relations_per_event).alias("_slot"),
        )
        .with_columns(
            (
                (pl.col("_event") * 31 + pl.col("_slot") * 9_973 + profile.seed)
                % profile.related_objects
            ).alias("_object")
        )
        .with_columns(
            _identifier("e", pl.col("_event")).alias("ocel_event_id"),
            _type_name(
                "event",
                _type_index(
                    pl.col("_event"),
                    count=profile.event_types,
                    skew=profile.type_skew,
                    seed=profile.seed,
                ),
            ).alias("ocel_event_type"),
            _identifier("o", pl.col("_object")).alias("ocel_object_id"),
            _type_name(
                "object",
                _type_index(
                    pl.col("_object"),
                    count=profile.object_types,
                    skew=profile.type_skew,
                    seed=profile.seed + 1,
                ),
            ).alias("ocel_object_type"),
            _type_name(
                "role",
                (pl.col("_slot") + pl.col("_event")) % profile.qualifiers,
            ).alias("ocel_qualifier"),
        )
        .select(
            "ocel_event_id",
            "ocel_event_type",
            "ocel_object_id",
            "ocel_object_type",
            "ocel_qualifier",
        )
    )


def _o2o_batch(profile: Profile, start: int, stop: int) -> pl.DataFrame:
    if profile.o2o_per_object:
        frame = (
            _indices(start, stop)
            .with_columns(
                (pl.col("_index") // profile.o2o_per_object).alias("_source"),
                (pl.col("_index") % profile.o2o_per_object).alias("_slot"),
            )
            .with_columns(
                (
                    (pl.col("_source") + 1 + pl.col("_slot") * 7_919 + profile.seed)
                    % profile.objects
                ).alias("_target")
            )
        )
    else:
        frame = _indices(0, 0).with_columns(
            pl.lit(None, dtype=pl.Int64).alias("_source"),
            pl.lit(None, dtype=pl.Int64).alias("_slot"),
            pl.lit(None, dtype=pl.Int64).alias("_target"),
        )
    return frame.with_columns(
        _identifier("o", pl.col("_source")).alias("ocel_source_id"),
        _type_name(
            "object",
            _type_index(
                pl.col("_source"),
                count=profile.object_types,
                skew=profile.type_skew,
                seed=profile.seed + 1,
            ),
        ).alias("ocel_source_type"),
        _identifier("o", pl.col("_target")).alias("ocel_target_id"),
        _type_name(
            "object",
            _type_index(
                pl.col("_target"),
                count=profile.object_types,
                skew=profile.type_skew,
                seed=profile.seed + 1,
            ),
        ).alias("ocel_target_type"),
        _type_name("link", pl.col("_slot") % profile.qualifiers).alias(
            "ocel_qualifier"
        ),
    ).select(
        "ocel_source_id",
        "ocel_source_type",
        "ocel_target_id",
        "ocel_target_type",
        "ocel_qualifier",
    )


def _write_batches(
    directory: Path,
    rows: int,
    batch_size: int,
    build: Callable[[int, int], pl.DataFrame],
) -> None:
    directory.mkdir()
    if rows == 0:
        build(0, 0).write_parquet(directory / "part-00000.parquet")
        return
    for part, start in enumerate(range(0, rows, batch_size)):
        stop = min(start + batch_size, rows)
        build(start, stop).write_parquet(
            directory / f"part-{part:05d}.parquet",
            compression="lz4",
            statistics=True,
        )


def _scan_parts(directory: Path) -> pl.LazyFrame:
    return pl.scan_parquet(directory / "*.parquet")


def _indices(start: int, stop: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "_index": pl.int_range(
                start,
                stop,
                dtype=pl.Int64,
                eager=True,
            )
        }
    )


def _identifier(prefix: str, index: pl.Expr) -> pl.Expr:
    return pl.concat_str(pl.lit(prefix), index.cast(pl.String))


def _type_name(prefix: str, index: pl.Expr) -> pl.Expr:
    return pl.concat_str(pl.lit(f"{prefix}-"), index.cast(pl.String))


def _type_index(
    index: pl.Expr,
    *,
    count: int,
    skew: float,
    seed: int,
) -> pl.Expr:
    if count == 1:
        return pl.lit(0, dtype=pl.Int64)
    threshold = int(skew * 1_000)
    bucket = (index + seed) % 1_000
    return (
        pl.when(index < count)
        .then(index)
        .otherwise(
            pl.when(bucket < threshold)
            .then(pl.lit(0))
            .otherwise(1 + (((index + seed) // 1_000) % (count - 1)))
        )
        .cast(pl.Int64)
    )


def _numeric_value(index: pl.Expr, salt: int) -> pl.Expr:
    return ((index * 1_000_003 + salt * 97_409) % 2_147_483_647).cast(pl.Int64)


def _count(frame: pl.LazyFrame) -> int:
    return int(_scalar(frame.select(pl.len()).collect(engine="streaming")))


def _scalar(frame: pl.DataFrame) -> object:
    return _json_scalar(frame.item())


def _row(frame: pl.DataFrame) -> JSON:
    return {
        name: _json_scalar(value) for name, value in frame.row(0, named=True).items()
    }


def _json_scalar(value: object) -> object:
    if hasattr(value, "item"):
        value = value.item()  # type: ignore[union-attr]
    return value


def _first(values: list[str], context: str) -> str:
    if not values:
        raise ValueError(f"Benchmark dataset has no {context}.")
    return values[0]


def _dataset_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _peak_rss_bytes() -> int | None:
    if resource is None:
        return None
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum if platform.system() == "Darwin" else maximum * 1_024)


def _environment(threads: int | None) -> JSON:
    try:
        version = importlib.metadata.version("oceldb")
    except importlib.metadata.PackageNotFoundError:
        version = "local"
    return {
        "oceldb": version,
        "polars": pl.__version__,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "polars_max_threads": threads,
    }


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _workload(name: str) -> Workload:
    return next(workload for workload in WORKLOADS if workload.name == name)


def _profile_from_args(args: argparse.Namespace) -> Profile:
    profile = PROFILES[args.profile]
    updates: dict[str, object] = {}
    for field in fields(Profile):
        if field.name == "name":
            continue
        value = getattr(args, field.name, None)
        if value is not None:
            updates[field.name] = value
    if updates:
        updates["name"] = f"{profile.name}-custom"
    result = replace(profile, **updates)
    result.validate()
    return result


def _selected_cases(values: list[str] | None) -> tuple[str, ...]:
    if values is None:
        return WORKLOAD_NAMES
    return tuple(dict.fromkeys(values))


def _add_profile_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument("--profile", choices=tuple(PROFILES), default="small")
    integer_fields = (
        "events",
        "objects",
        "relations_per_event",
        "states_per_object",
        "o2o_per_object",
        "event_types",
        "object_types",
        "event_attributes",
        "object_attributes",
        "qualifiers",
        "seed",
        "batch_size",
    )
    for name in integer_fields:
        command.add_argument(f"--{name.replace('_', '-')}", dest=name, type=int)
    float_fields = (
        "relationless_event_fraction",
        "relationless_object_fraction",
        "type_skew",
    )
    for name in float_fields:
        command.add_argument(f"--{name.replace('_', '-')}", dest=name, type=float)


def _add_run_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--case",
        dest="cases",
        action="append",
        choices=WORKLOAD_NAMES,
        help="Workload to run; repeat to select multiple. Defaults to all.",
    )
    command.add_argument("--rounds", type=int, default=3)
    command.add_argument("--warmups", type=int, default=1)
    command.add_argument("--threads", type=int)
    command.add_argument("--include-plans", action="store_true")
    command.add_argument("--json", action="store_true", dest="json_output")
    command.add_argument("--output", type=Path)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)

    generate_command = commands.add_parser(
        "generate",
        help="Generate a reusable synthetic native snapshot.",
    )
    generate_command.add_argument("path", type=Path)
    _add_profile_arguments(generate_command)
    generate_command.add_argument("--overwrite", action="store_true")
    generate_command.add_argument("--validate", action="store_true")

    run_command = commands.add_parser(
        "run",
        help="Benchmark an existing native snapshot.",
    )
    run_command.add_argument("path", type=Path)
    _add_run_arguments(run_command)

    suite_command = commands.add_parser(
        "suite",
        help="Generate a synthetic snapshot and benchmark it.",
    )
    _add_profile_arguments(suite_command)
    _add_run_arguments(suite_command)
    suite_command.add_argument(
        "--dataset",
        type=Path,
        help="Keep the generated dataset at this path.",
    )
    suite_command.add_argument("--overwrite", action="store_true")
    suite_command.add_argument("--validate-generation", action="store_true")

    plans_command = commands.add_parser(
        "plans",
        help="Print representative optimized Polars query plans as JSON.",
    )
    plans_command.add_argument("path", type=Path)
    plans_command.add_argument("--output", type=Path)

    case_command = commands.add_parser("_case")
    case_command.add_argument("path", type=Path)
    case_command.add_argument("case", choices=WORKLOAD_NAMES)
    return result


def _emit_report(report: JSON, args: argparse.Namespace) -> None:
    output = getattr(args, "output", None)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    if getattr(args, "json_output", False):
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    _print_report(report)
    if output is not None:
        print(f"\nJSON report: {output}")


def _print_report(report: JSON) -> None:
    print(f"Dataset: {report['dataset']['path']}")
    print(f"Size: {_human_bytes(int(report['dataset']['bytes']))}")
    print(f"Rounds: {report['rounds']} measured, {report['warmups']} warm-up(s)")
    print()
    print(f"{'workload':30} {'median':>12} {'peak RSS':>12}")
    print("-" * 58)
    for measurement in report["measurements"]:
        peak = measurement["max_peak_rss_bytes"]
        print(
            f"{measurement['case']:30} "
            f"{measurement['median_seconds']:>10.4f}s "
            f"{_human_bytes(peak) if peak is not None else 'n/a':>12}"
        )


def _human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1_024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1_024
    raise AssertionError("unreachable")


def main() -> None:
    args = parser().parse_args()
    if args.command == "generate":
        print(
            json.dumps(
                generate_native(
                    args.path,
                    _profile_from_args(args),
                    overwrite=args.overwrite,
                    validate=args.validate,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return
    if args.command == "run":
        _emit_report(
            run_benchmarks(
                args.path,
                cases=_selected_cases(args.cases),
                rounds=args.rounds,
                warmups=args.warmups,
                threads=args.threads,
                include_plans=args.include_plans,
            ),
            args,
        )
        return
    if args.command == "suite":
        profile = _profile_from_args(args)
        if args.dataset is not None:
            generation = generate_native(
                args.dataset,
                profile,
                overwrite=args.overwrite,
                validate=args.validate_generation,
            )
            report = run_benchmarks(
                args.dataset,
                cases=_selected_cases(args.cases),
                rounds=args.rounds,
                warmups=args.warmups,
                threads=args.threads,
                include_plans=args.include_plans,
            )
        else:
            with tempfile.TemporaryDirectory(prefix="oceldb-benchmark-") as directory:
                dataset = Path(directory) / "native"
                generation = generate_native(
                    dataset,
                    profile,
                    validate=args.validate_generation,
                )
                report = run_benchmarks(
                    dataset,
                    cases=_selected_cases(args.cases),
                    rounds=args.rounds,
                    warmups=args.warmups,
                    threads=args.threads,
                    include_plans=args.include_plans,
                )
        report["profile"] = asdict(profile)
        report["expected_rows"] = profile.rows
        report["generation"] = generation
        _emit_report(report, args)
        return
    if args.command == "plans":
        plans = query_plans(args.path)
        text = json.dumps(plans, indent=2, sort_keys=True) + "\n"
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(text, encoding="utf-8")
        print(text, end="")
        return
    print(json.dumps(run_case(args.path, args.case), separators=(",", ":")))


if __name__ == "__main__":
    main()
