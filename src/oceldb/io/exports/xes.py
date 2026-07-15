"""write_xes: export a flattened log to XES format."""

import html
from pathlib import Path
from typing import IO

import polars as pl

from oceldb.io._paths import atomic_file
from oceldb.io._values import encode_scalar
from oceldb.io.errors import io_boundary


def write_xes(
    log: pl.LazyFrame | pl.DataFrame,
    path: str | Path,
    *,
    overwrite: bool = False,
) -> None:
    """Write a flattened log to XES format.

    Accepts the output of :func:`~oceldb.transformations.flatten` directly.
    Each unique value in ``case:concept:name`` becomes a ``<trace>``; each
    row becomes an ``<event>``. Column types are mapped to XES attribute
    types (``string``, ``date``, ``float``, ``int``, ``boolean``).

    The input is materialized and sorted once, then the XML is emitted one row
    at a time. The flattened Polars frame must therefore fit in memory, while
    XML construction itself adds only constant memory overhead.

    Args:
        log: A flattened log, typically produced by
            ``ocel >> flatten("order")``. Accepts both lazy and eager frames.
        path: Destination ``.xes`` file path.
        overwrite: Replace an existing file when ``True``. The default
            raises :class:`FileExistsError`.

    Raises:
        FileExistsError: If ``path`` exists and ``overwrite`` is ``False``.

    Examples:
        >>> from oceldb.io import write_xes
        >>> from oceldb.transformations import flatten
        >>> write_xes(ocel >> flatten("order"), "orders.xes")
        >>> write_xes(ocel >> flatten("order"), "orders.xes", overwrite=True)
    """
    with io_boundary("write XES to", path):
        _write_xes(log, path, overwrite=overwrite)


def _write_xes(
    log: pl.LazyFrame | pl.DataFrame,
    path: str | Path,
    *,
    overwrite: bool,
) -> None:
    df = log.collect() if isinstance(log, pl.LazyFrame) else log

    sort_cols = ["case:concept:name", "time:timestamp"]
    if "ocel_event_id" in df.columns:
        sort_cols.append("ocel_event_id")
    df = df.sort(sort_cols)

    schema = df.schema
    case_cols = [c for c in df.columns if c.startswith("case:")]
    event_cols = [c for c in df.columns if c not in case_cols]

    with atomic_file(path, overwrite=overwrite) as (_, staging):
        with staging.open("w", encoding="utf-8") as f:
            _write_header(f)
            current_case: str | None = None
            for row in df.iter_rows(named=True):
                case = str(row["case:concept:name"])
                if case != current_case:
                    if current_case is not None:
                        f.write("  </trace>\n")
                    f.write("  <trace>\n")
                    f.write(f'    <string key="concept:name" value="{_x(case)}"/>\n')
                    for col in case_cols:
                        if col == "case:concept:name" or row[col] is None:
                            continue
                        key = col.removeprefix("case:")
                        xtype = _xes_type(schema[col])
                        xval = _xes_value(row[col])
                        f.write(f'    <{xtype} key="{_x(key)}" value="{_x(xval)}"/>\n')
                    current_case = case
                f.write("    <event>\n")
                for col in event_cols:
                    val = row[col]
                    if val is None:
                        continue
                    xtype = _xes_type(schema[col])
                    xval = _xes_value(val)
                    f.write(f'      <{xtype} key="{_x(col)}" value="{_x(xval)}"/>\n')
                f.write("    </event>\n")
            if current_case is not None:
                f.write("  </trace>\n")
            f.write("</log>\n")


def _write_header(f: IO[str]) -> None:
    f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    f.write('<log xes.version="1.0" xmlns="http://www.xes-standard.org/">\n')
    f.write(
        '  <extension name="Concept" prefix="concept"'
        ' uri="http://www.xes-standard.org/concept.xesext"/>\n'
    )
    f.write(
        '  <extension name="Time" prefix="time"'
        ' uri="http://www.xes-standard.org/time.xesext"/>\n'
    )
    f.write('  <classifier name="Activity" keys="concept:name"/>\n')


def _xes_type(dtype: pl.DataType) -> str:
    if isinstance(dtype, (pl.Datetime, pl.Date)):
        return "date"
    if isinstance(dtype, (pl.Float32, pl.Float64)):
        return "float"
    if dtype.is_integer():
        return "int"
    if isinstance(dtype, pl.Boolean):
        return "boolean"
    return "string"


def _xes_value(val: object) -> str:
    return str(encode_scalar(val, style="xes"))


def _x(s: str) -> str:
    return html.escape(s, quote=True)
