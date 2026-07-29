"""Fresh-process benchmarks for OCEL exchange-to-native conversion."""

from __future__ import annotations

import argparse
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
from typing import Any

from oceldb.io import ExchangeFormat, convert_ocel, detect_format

try:
    import resource
except ImportError:  # pragma: no cover - unavailable on Windows
    resource = None

JSON = dict[str, Any]


def run_case(
    source: Path,
    *,
    format: ExchangeFormat | None,
    batch_size: int,
    validate: bool,
) -> JSON:
    """Convert once and return wall time, peak RSS, and native size."""
    with tempfile.TemporaryDirectory(
        prefix="oceldb-conversion-benchmark-"
    ) as directory:
        target = Path(directory) / "native"
        started = time.perf_counter()
        converted = convert_ocel(
            source,
            target,
            format=format,
            batch_size=batch_size,
            validate=validate,
        )
        seconds = time.perf_counter() - started
        return {
            "seconds": seconds,
            "peak_rss_bytes": _peak_rss_bytes(),
            "native_bytes": _directory_bytes(target),
            "event_types": len(converted.event_types()),
            "object_types": len(converted.object_types()),
        }


def run_benchmark(
    source: Path,
    *,
    format: ExchangeFormat | None,
    batch_size: int,
    validate: bool,
    rounds: int,
    warmups: int,
) -> JSON:
    """Run conversion samples in isolated child processes."""
    if rounds < 1:
        raise ValueError("rounds must be positive.")
    if warmups < 0:
        raise ValueError("warmups must be non-negative.")
    if batch_size < 1:
        raise ValueError("batch_size must be positive.")
    selected = detect_format(source) if format is None else format
    for _ in range(warmups):
        _child(source, selected, batch_size, validate)
    samples = [_child(source, selected, batch_size, validate) for _ in range(rounds)]
    seconds = [float(sample["seconds"]) for sample in samples]
    peaks = [
        int(peak)
        for sample in samples
        if (peak := sample["peak_rss_bytes"]) is not None
    ]
    return {
        "schema_version": 1,
        "source": {
            "path": str(source.resolve()),
            "bytes": source.stat().st_size,
            "format": selected,
        },
        "configuration": {
            "batch_size": batch_size,
            "validate": validate,
            "rounds": rounds,
            "warmups": warmups,
        },
        "environment": _environment(),
        "median_seconds": statistics.median(seconds),
        "min_seconds": min(seconds),
        "max_seconds": max(seconds),
        "max_peak_rss_bytes": max(peaks) if peaks else None,
        "samples": samples,
    }


def _child(
    source: Path,
    format: ExchangeFormat,
    batch_size: int,
    validate: bool,
) -> JSON:
    script = Path(__file__).resolve()
    environment = os.environ.copy()
    local_source = str(script.parents[1] / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        os.pathsep.join((local_source, existing)) if existing else local_source
    )
    command = [
        sys.executable,
        str(script),
        str(source),
        "--_case",
        "--format",
        format,
        "--batch-size",
        str(batch_size),
    ]
    if validate:
        command.append("--validate")
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    if completed.returncode:
        raise RuntimeError(
            f"Conversion benchmark failed with exit code {completed.returncode}:\n"
            f"{completed.stderr.strip()}"
        )
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Conversion benchmark produced invalid JSON:\n{completed.stdout}"
        ) from exc


def _peak_rss_bytes() -> int | None:
    if resource is None:
        return None
    maximum = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(maximum if platform.system() == "Darwin" else maximum * 1_024)


def _directory_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def _environment() -> JSON:
    try:
        version = importlib.metadata.version("oceldb")
    except importlib.metadata.PackageNotFoundError:
        version = "local"
    return {
        "oceldb": version,
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
    }


def _human_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1_024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1_024
    raise AssertionError("unreachable")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("source", type=Path)
    result.add_argument("--format", choices=("sqlite", "json", "xml"))
    result.add_argument("--batch-size", type=int, default=10_000)
    result.add_argument("--validate", action="store_true")
    result.add_argument("--rounds", type=int, default=3)
    result.add_argument("--warmups", type=int, default=1)
    result.add_argument("--json", action="store_true", dest="json_output")
    result.add_argument("--output", type=Path)
    result.add_argument("--_case", action="store_true", dest="case")
    return result


def main() -> None:
    args = parser().parse_args()
    selected: ExchangeFormat | None = args.format
    if args.case:
        print(
            json.dumps(
                run_case(
                    args.source,
                    format=selected,
                    batch_size=args.batch_size,
                    validate=args.validate,
                ),
                separators=(",", ":"),
            )
        )
        return
    report = run_benchmark(
        args.source,
        format=selected,
        batch_size=args.batch_size,
        validate=args.validate,
        rounds=args.rounds,
        warmups=args.warmups,
    )
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    if args.json_output:
        print(text, end="")
        return
    peak = report["max_peak_rss_bytes"]
    print(f"Source: {report['source']['path']}")
    print(f"Format: {report['source']['format']}")
    print(f"Median: {report['median_seconds']:.3f}s")
    print(f"Peak RSS: {_human_bytes(peak) if peak is not None else 'n/a'}")
    if args.output is not None:
        print(f"JSON report: {args.output}")


if __name__ == "__main__":
    main()
