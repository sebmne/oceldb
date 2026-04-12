from __future__ import annotations

import argparse
from pathlib import Path

from oceldb import asc, avg, col, count, desc, read_ocel

DEFAULT_DATASET = Path("examples/data/atlas-fulfillment.oceldb")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a small example analysis over the atlas-fulfillment OCEL.",
    )
    parser.add_argument(
        "source",
        nargs="?",
        type=Path,
        default=DEFAULT_DATASET,
        help="Path to the packaged running example.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.source.expanduser().resolve()

    with read_ocel(source) as ocel:
        print("Overview")
        print(ocel.inspect.overview())
        print()

        event_type_rows = (
            ocel.query()
            .events()
            .group_by("ocel_type")
            .agg(count().alias("events"))
            .sort(desc("events"), asc("ocel_type"))
            .limit(12)
            .collect()
            .fetchall()
        )
        _print_table(
            "Top Event Types",
            ("event_type", "events"),
            event_type_rows,
        )

        order_rows = (
            ocel.query()
            .objects("order")
            .group_by("country", "channel", "status")
            .agg(
                count().alias("orders"),
                avg(col("order_value")).alias("avg_order_value"),
            )
            .sort(desc("orders"), desc("avg_order_value"))
            .limit(12)
            .collect()
            .fetchall()
        )
        _print_table(
            "Order Status Mix",
            (
                "country",
                "channel",
                "status",
                "orders",
                "avg_order_value",
            ),
            order_rows,
        )

        supplier_rows = (
            ocel.query()
            .objects("supplier_order")
            .group_by("supplier", "status")
            .agg(
                count().alias("supplier_orders"),
                avg(col("lead_time_days")).alias("avg_lead_time_days"),
            )
            .sort(desc("supplier_orders"), desc("avg_lead_time_days"))
            .collect()
            .fetchall()
        )
        _print_table(
            "Supplier Pressure",
            ("supplier", "status", "supplier_orders", "avg_lead_time_days"),
            supplier_rows,
        )

        object_type_rows = (
            ocel.query()
            .objects()
            .group_by("ocel_type")
            .agg(
                count().alias("objects"),
            )
            .sort(desc("objects"), asc("ocel_type"))
            .collect()
            .fetchall()
        )
        _print_table(
            "Object Type Sizes",
            ("object_type", "objects"),
            object_type_rows,
        )


def _print_table(title: str, headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    print(title)
    if not rows:
        print("(no rows)")
        print()
        return

    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(_format_value(value)))

    header_line = " | ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    divider = "-+-".join("-" * width for width in widths)
    print(header_line)
    print(divider)
    for row in rows:
        print(
            " | ".join(
                _format_value(value).ljust(widths[index])
                for index, value in enumerate(row)
            )
        )
    print()


def _format_value(value: object) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


if __name__ == "__main__":
    main()
