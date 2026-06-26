from datetime import datetime
from pathlib import Path

import polars as pl

from oceldb import OCEL
from oceldb.store import decode_type_name, encode_type_name


def _sample_ocel() -> OCEL:
    events = pl.DataFrame(
        {
            "ocel_id": ["e1", "e2"],
            "ocel_time": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
            "ocel_type": ["Place Order", "Pay Order"],
            "amount": [42.0, None],
        }
    ).lazy()
    objects = pl.DataFrame(
        {
            "ocel_id": ["o1"],
            "ocel_type": ["order"],
        }
    ).lazy()
    object_changes = pl.DataFrame(
        {
            "ocel_id": ["o1", "o1"],
            "ocel_time": [datetime(1970, 1, 1), datetime(2024, 1, 2)],
            "ocel_changed_field": [None, "status"],
            "status": ["created", "paid"],
            "ocel_type": ["order", "order"],
        }
    ).lazy()
    e2o = pl.DataFrame(
        {
            "ocel_event_id": ["e1", "e2"],
            "ocel_event_type": ["Place Order", "Pay Order"],
            "ocel_object_id": ["o1", "o1"],
            "ocel_object_type": ["order", "order"],
            "ocel_qualifier": [None, None],
        }
    ).lazy()
    o2o = pl.DataFrame(
        schema={
            "ocel_source_id": pl.String,
            "ocel_source_type": pl.String,
            "ocel_target_id": pl.String,
            "ocel_target_type": pl.String,
            "ocel_qualifier": pl.String,
        }
    ).lazy()
    return OCEL(events, objects, object_changes, o2o, e2o)


def test_type_name_encoding_round_trips() -> None:
    type_name = "invoice/item: paid"

    assert decode_type_name(encode_type_name(type_name)) == type_name


def test_ocel_write_read_and_type_filter(tmp_path: Path) -> None:
    target = tmp_path / "log"

    _sample_ocel().write(target)
    reopened = OCEL.read(target)

    pay_events = reopened.events("Pay Order").collect()

    assert pay_events.get_column("ocel_id").to_list() == ["e2"]
    assert "amount" not in pay_events.columns


def test_object_states_forward_fill() -> None:
    states = _sample_ocel().object_states("order").collect()

    assert states.get_column("status").to_list() == ["created", "paid"]
