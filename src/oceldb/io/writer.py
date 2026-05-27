"""Write canonical sources as persisted Parquet logs."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import TypeAlias

import duckdb

from oceldb.io.source import Canonical, Source, temporary_view
from oceldb.io.sql import encode_type_name, quote_identifier, sql_string
from oceldb.storage.manifest import EventTypeInfo, ObjectTypeInfo
from oceldb.storage.metadata import build_manifest, count_rows, event_stats

ProgressCallback: TypeAlias = Callable[[str], None]


def write_log(
    target: Path,
    source: Source,
    *,
    source_kind: str,
    source_path: Path | None,
    overwrite: bool = False,
    compression: str = "zstd",
    progress: ProgressCallback | None = None,
) -> None:
    """Materialize *source* as an oceldb Parquet directory at *target*."""
    _prepare_target(target, overwrite)
    with _cleanup_on_failure(target):
        target.mkdir(parents=True)
        con = duckdb.connect()
        try:
            canon = source.attach(con)
            say: ProgressCallback = progress or (lambda _msg: None)

            say("Writing events")
            event_infos = _write_events(con, target, canon, compression, progress)

            say("Writing objects")
            object_infos = _write_objects(con, target, canon, compression, progress)

            say("Writing event_object.parquet")
            e2o_count = _write_bridge(
                con,
                target / "event_object.parquet",
                canon.event_object,
                order_by=("ocel_object_id", "ocel_event_id"),
                compression=compression,
            )

            say("Writing object_object.parquet")
            o2o_count = 0
            if canon.object_object is not None:
                o2o_count = _write_bridge(
                    con,
                    target / "object_object.parquet",
                    canon.object_object,
                    order_by=("ocel_source_id", "ocel_target_id"),
                    compression=compression,
                )

            say("Writing manifest.json")
            build_manifest(
                source_kind=source_kind,
                source_path=source_path,
                event_types=event_infos,
                object_types=object_infos,
                e2o_count=e2o_count,
                o2o_count=o2o_count,
            ).save(target / "manifest.json")
        finally:
            con.close()


def _write_events(
    con: duckdb.DuckDBPyConnection,
    target: Path,
    canon: Canonical,
    compression: str,
    progress: ProgressCallback | None,
) -> dict[str, EventTypeInfo]:
    infos: dict[str, EventTypeInfo] = {}
    for type_name, attrs in canon.event_types.items():
        if progress is not None:
            progress(f"  events/{type_name}")
        out_dir = target / "events" / f"ocel_type={encode_type_name(type_name)}"
        out_dir.mkdir(parents=True)
        with temporary_view(con, canon.events_for(type_name)) as view:
            _copy_parquet(
                con,
                view=view,
                path=out_dir / "data.parquet",
                columns=("ocel_id", "ocel_time", *attrs),
                order_by=("ocel_time",),
                compression=compression,
            )
            count, lo, hi = event_stats(con, view)
        infos[type_name] = EventTypeInfo(
            count=count, time_range=(lo, hi), attributes=dict(attrs)
        )
    return infos


def _write_objects(
    con: duckdb.DuckDBPyConnection,
    target: Path,
    canon: Canonical,
    compression: str,
    progress: ProgressCallback | None,
) -> dict[str, ObjectTypeInfo]:
    infos: dict[str, ObjectTypeInfo] = {}
    for type_name, attrs in canon.object_types.items():
        if progress is not None:
            progress(f"  objects/{type_name}")
        encoded = encode_type_name(type_name)

        obj_dir = target / "objects" / f"ocel_type={encoded}"
        obj_dir.mkdir(parents=True)
        object_count = _write_relation(
            con,
            canon.objects_for(type_name),
            obj_dir / "data.parquet",
            columns=("ocel_id",),
            order_by=("ocel_id",),
            compression=compression,
        )

        ch_dir = target / "object_changes" / f"ocel_type={encoded}"
        ch_dir.mkdir(parents=True)
        change_count = _write_relation(
            con,
            canon.object_changes_for(type_name),
            ch_dir / "data.parquet",
            columns=("ocel_id", "ocel_time", "ocel_changed_field", *attrs),
            order_by=("ocel_id", "ocel_time"),
            compression=compression,
        )

        infos[type_name] = ObjectTypeInfo(
            object_count=object_count,
            change_count=change_count,
            attributes=dict(attrs),
        )
    return infos


def _write_bridge(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    relation: duckdb.DuckDBPyRelation,
    *,
    order_by: tuple[str, ...],
    compression: str,
) -> int:
    return _write_relation(
        con, relation, path, columns=None, order_by=order_by, compression=compression
    )


def _write_relation(
    con: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    path: Path,
    *,
    columns: tuple[str, ...] | None,
    order_by: tuple[str, ...],
    compression: str,
) -> int:
    with temporary_view(con, relation) as view:
        _copy_parquet(
            con,
            view=view,
            path=path,
            columns=columns,
            order_by=order_by,
            compression=compression,
        )
        return count_rows(con, view)


def _copy_parquet(
    con: duckdb.DuckDBPyConnection,
    *,
    view: str,
    path: Path,
    columns: tuple[str, ...] | None,
    order_by: tuple[str, ...],
    compression: str,
) -> None:
    col_sql = (
        "*" if columns is None else ", ".join(quote_identifier(c) for c in columns)
    )
    order_sql = ", ".join(quote_identifier(c) for c in order_by)
    con.execute(f"""
        COPY (
            SELECT {col_sql} FROM {quote_identifier(view)}
            ORDER BY {order_sql}
        ) TO {sql_string(str(path))}
        (FORMAT PARQUET, COMPRESSION {compression})
    """)


def _prepare_target(target: Path, overwrite: bool) -> None:
    if not target.exists():
        return
    if not overwrite:
        raise FileExistsError(
            f"Target already exists: {target}. Pass overwrite=True to replace it."
        )
    if target.is_dir():
        shutil.rmtree(target)
    else:
        target.unlink()


@contextmanager
def _cleanup_on_failure(target: Path):
    try:
        yield
    except BaseException:
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        raise
