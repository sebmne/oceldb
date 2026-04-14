from __future__ import annotations

from pathlib import Path

from oceldb import OCEL, col, count, row_number
from oceldb.discovery import ocdfg


def main() -> None:
    dataset = Path(__file__).parent / "data" / "smoke-example"

    with OCEL.read(dataset) as ocel:
        event_count = ocel.query.events().count()
        object_count = ocel.query.objects().count()

        event_type_counts = (
            ocel.query
            .events()
            .group_by("ocel_type")
            .agg(count().alias("n"))
            .sort("ocel_type")
            .collect()
            .fetchall()
        )

        timeline = (
            ocel.query
            .event_occurrences("order")
            .with_columns(
                seq=row_number().over(
                    partition_by="ocel_object_id",
                    order_by=("ocel_event_time", "ocel_event_id"),
                ),
                previous=col("ocel_event_type").lag().over(
                    partition_by="ocel_object_id",
                    order_by=("ocel_event_time", "ocel_event_id"),
                ),
            )
            .select("ocel_object_id", "seq", "previous", "ocel_event_type")
            .sort("ocel_object_id", "seq")
            .collect()
            .fetchall()
        )

        dfg = ocdfg(ocel, "order")

    print(f"events={event_count}")
    print(f"objects={object_count}")
    print(f"event_type_counts={event_type_counts}")
    print(f"timeline={timeline}")
    print(f"dfg_nodes={[(node.activity, node.count) for node in dfg.nodes]}")


if __name__ == "__main__":
    main()
