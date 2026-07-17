import json
from datetime import datetime, timezone

import polars as pl
import pytest

from oceldb import OCEL


def test_open_native_layout_with_short_relation_names(tmp_path) -> None:
    base = tmp_path / "log"
    event_dir = base / "events" / "ocel_type=Create"
    second_event_dir = base / "events" / "ocel_type=Ship%20Order"
    object_dir = base / "objects" / "ocel_type=Order"
    change_dir = base / "object_changes" / "ocel_type=Order"
    event_dir.mkdir(parents=True)
    second_event_dir.mkdir(parents=True)
    object_dir.mkdir(parents=True)
    change_dir.mkdir(parents=True)

    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pl.DataFrame({"ocel_id": ["e1"], "ocel_time": [now], "cost": [2.5]}).write_parquet(
        event_dir / "data.parquet"
    )
    pl.DataFrame(
        {"ocel_id": ["e2"], "ocel_time": [now], "carrier": ["DHL"]}
    ).write_parquet(second_event_dir / "data.parquet")
    pl.DataFrame({"ocel_id": ["o1"]}).write_parquet(object_dir / "data.parquet")
    pl.DataFrame(
        {
            "ocel_id": ["o1"],
            "ocel_time": [now],
            "ocel_changed_field": pl.Series([None], dtype=pl.String),
            "ocel_is_initial": [True],
            "status": ["new"],
        }
    ).write_parquet(change_dir / "data.parquet")
    pl.DataFrame(
        {
            "ocel_event_id": ["e1"],
            "ocel_event_type": ["Create"],
            "ocel_object_id": ["o1"],
            "ocel_object_type": ["Order"],
            "ocel_qualifier": ["created"],
        }
    ).write_parquet(base / "e2o.parquet")

    manifest = {
        "format": "oceldb",
        "formatVersion": 2,
        "createdAt": "2026-01-01T00:00:00Z",
    }
    (base / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    ocel = OCEL.open(base)

    assert ocel.events("Create").collect().get_column("cost").to_list() == [2.5]
    assert ocel.events("Ship Order").collect().get_column("carrier").to_list() == [
        "DHL"
    ]
    assert ocel.e2o().collect().get_column("ocel_object_id").to_list() == ["o1"]
    assert ocel.o2o().collect().is_empty()

    written = tmp_path / "written"
    ocel.write(written)

    assert (written / "e2o.parquet").is_file()
    assert not (written / "o2o.parquet").exists()
    assert set(
        pl.read_parquet_schema(written / "events" / "ocel_type=Create" / "data.parquet")
    ) == {"ocel_id", "ocel_time", "cost"}
    assert set(
        pl.read_parquet_schema(
            written / "events" / "ocel_type=Ship%20Order" / "data.parquet"
        )
    ) == {"ocel_id", "ocel_time", "carrier"}

    written_manifest = json.loads((written / "manifest.json").read_text())
    assert set(written_manifest) == {"format", "formatVersion", "createdAt"}
    assert written_manifest["formatVersion"] == 2
    assert written_manifest["createdAt"].endswith("Z")

    reopened = OCEL.open(written)
    assert reopened.events().collect().height == 2
    assert reopened.e2o().collect().height == 1

    with pytest.raises(FileExistsError):
        ocel.write(written)

    invalid = OCEL(
        events=ocel.events().drop("ocel_type"),
        objects=ocel.objects(),
        object_changes=ocel.object_changes(),
        e2o=ocel.e2o(),
        o2o=ocel.o2o(),
    )
    with pytest.raises(ValueError, match="ocel_type"):
        invalid.write(written, overwrite=True)
    assert OCEL.open(written).events().collect().height == 2

    ocel.write(written, overwrite=True)
