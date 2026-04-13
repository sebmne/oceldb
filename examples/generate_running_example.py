from __future__ import annotations

import argparse
import random
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from oceldb.io import convert_sqlite

EXAMPLE_NAME = "atlas-fulfillment"
DEFAULT_ORDER_COUNT = 2_500
DEFAULT_SEED = 7

OBJECT_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "customer": (
        ("region", "TEXT"),
        ("segment", "TEXT"),
        ("tier", "TEXT"),
    ),
    "order": (
        ("channel", "TEXT"),
        ("country", "TEXT"),
        ("priority", "TEXT"),
        ("payment_method", "TEXT"),
        ("payment_terms_days", "INTEGER"),
        ("risk_bucket", "TEXT"),
        ("order_value", "REAL"),
        ("status", "TEXT"),
    ),
    "order_item": (
        ("sku", "TEXT"),
        ("category", "TEXT"),
        ("quantity", "INTEGER"),
        ("unit_price", "REAL"),
        ("fulfillment_mode", "TEXT"),
    ),
    "package": (
        ("carrier", "TEXT"),
        ("service_level", "TEXT"),
        ("package_weight", "REAL"),
        ("zone", "TEXT"),
    ),
    "shipment": (
        ("carrier", "TEXT"),
        ("lane", "TEXT"),
        ("is_international", "BOOLEAN"),
        ("had_customs_hold", "BOOLEAN"),
        ("attempt_count", "INTEGER"),
        ("final_status", "TEXT"),
    ),
    "invoice": (
        ("currency", "TEXT"),
        ("gross_amount", "REAL"),
        ("payment_terms_days", "INTEGER"),
        ("status", "TEXT"),
    ),
    "payment": (
        ("method", "TEXT"),
        ("amount", "REAL"),
        ("status", "TEXT"),
    ),
    "return_case": (
        ("reason", "TEXT"),
        ("resolution", "TEXT"),
        ("refundable_amount", "REAL"),
    ),
    "supplier_order": (
        ("supplier", "TEXT"),
        ("lead_time_days", "INTEGER"),
        ("status", "TEXT"),
    ),
}

EVENT_SCHEMAS: dict[str, tuple[tuple[str, str], ...]] = {
    "Place Order": (
        ("channel", "TEXT"),
        ("country", "TEXT"),
        ("priority", "TEXT"),
        ("item_count", "INTEGER"),
        ("order_value", "REAL"),
    ),
    "Start Fraud Review": (
        ("risk_bucket", "TEXT"),
        ("review_queue", "TEXT"),
    ),
    "Clear Fraud Review": (
        ("decision", "TEXT"),
        ("review_minutes", "INTEGER"),
    ),
    "Reject Order": (
        ("reason", "TEXT"),
    ),
    "Reserve Inventory": (
        ("warehouse", "TEXT"),
        ("reservation_status", "TEXT"),
    ),
    "Backorder Item": (
        ("shortage_reason", "TEXT"),
        ("expected_delay_days", "INTEGER"),
    ),
    "Create Supplier Order": (
        ("supplier", "TEXT"),
        ("lead_time_days", "INTEGER"),
    ),
    "Receive Supplier Delivery": (
        ("supplier", "TEXT"),
        ("received_quantity", "INTEGER"),
    ),
    "Pick Item": (
        ("warehouse", "TEXT"),
        ("picker_wave", "TEXT"),
    ),
    "Pack Package": (
        ("carrier", "TEXT"),
        ("package_weight", "REAL"),
        ("package_items", "INTEGER"),
    ),
    "Create Shipment": (
        ("carrier", "TEXT"),
        ("service_level", "TEXT"),
        ("package_count", "INTEGER"),
    ),
    "Customs Hold": (
        ("hold_reason", "TEXT"),
        ("extra_delay_days", "INTEGER"),
    ),
    "Customs Release": (
        ("clearance_channel", "TEXT"),
    ),
    "Dispatch Shipment": (
        ("hub", "TEXT"),
        ("lane", "TEXT"),
    ),
    "Delivery Attempt Failed": (
        ("reason", "TEXT"),
        ("attempt_no", "INTEGER"),
    ),
    "Deliver Shipment": (
        ("delivery_days", "REAL"),
        ("on_time", "BOOLEAN"),
    ),
    "Create Invoice": (
        ("payment_terms_days", "INTEGER"),
        ("gross_amount", "REAL"),
    ),
    "Receive Payment": (
        ("method", "TEXT"),
        ("amount", "REAL"),
        ("days_to_pay", "INTEGER"),
    ),
    "Send Payment Reminder": (
        ("reminder_level", "INTEGER"),
    ),
    "Request Return": (
        ("reason", "TEXT"),
        ("requested_quantity", "INTEGER"),
    ),
    "Approve Return": (
        ("resolution_path", "TEXT"),
    ),
    "Receive Return": (
        ("days_after_delivery", "INTEGER"),
    ),
    "Inspect Return": (
        ("disposition", "TEXT"),
        ("recoverable_value", "REAL"),
    ),
    "Restock Item": (
        ("warehouse", "TEXT"),
    ),
    "Scrap Item": (
        ("scrap_reason", "TEXT"),
    ),
    "Issue Refund": (
        ("refund_amount", "REAL"),
        ("refund_type", "TEXT"),
    ),
    "Close Order": (
        ("final_status", "TEXT"),
        ("cycle_days", "REAL"),
    ),
}

SEGMENTS = ["SMB", "Mid-Market", "Enterprise"]
TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["DACH", "Benelux", "Nordics", "North America", "Middle East"]
COUNTRIES = [
    ("Germany", False),
    ("Netherlands", False),
    ("Belgium", False),
    ("Sweden", False),
    ("United Kingdom", True),
    ("United States", True),
    ("Canada", True),
    ("United Arab Emirates", True),
]
CHANNELS = ["web", "mobile", "marketplace", "sales_rep"]
PRIORITIES = ["standard", "priority", "expedite"]
PAYMENT_METHODS = ["card", "wallet", "invoice", "wire"]
RISK_BUCKETS = ["low", "medium", "high", "critical"]
CATEGORIES = ["electronics", "fashion", "home", "industrial", "spare_parts"]
CARRIERS = ["DHL", "UPS", "FedEx", "DPD", "Maersk Air"]
SERVICE_LEVELS = ["economy", "standard", "express"]
WAREHOUSES = ["Aachen-DC", "Venlo-DC", "Warsaw-DC"]
SUPPLIERS = ["Alpha Parts", "Nordic Supply", "Mekatron", "Global Source"]
RETURN_REASONS = ["damaged", "wrong_item", "size_issue", "late_delivery", "quality_issue"]
SCRAP_REASONS = ["damaged_in_transit", "failed_quality_check", "used_item"]
CUSTOMS_REASONS = ["documentation_gap", "inspection", "duty_recalculation"]


@dataclass
class ObjectRecord:
    object_id: str
    object_type: str
    created_at: datetime
    attrs: dict[str, object] = field(default_factory=dict)


@dataclass
class EventRecord:
    event_id: str
    event_type: str
    happened_at: datetime
    attrs: dict[str, object] = field(default_factory=dict)


class StrictOCELBuilder:
    def __init__(self) -> None:
        self._object_counters: dict[str, int] = {}
        self._event_counter = 0
        self.objects: list[ObjectRecord] = []
        self.object_index: dict[str, ObjectRecord] = {}
        self.events: list[EventRecord] = []
        self.event_object_rows: list[tuple[str, str]] = []
        self.object_object_rows: set[tuple[str, str]] = set()

    def create_object(
        self,
        object_type: str,
        created_at: datetime,
        **attrs: object,
    ) -> str:
        object_id = self._next_object_id(object_type)
        record = ObjectRecord(
            object_id=object_id,
            object_type=object_type,
            created_at=created_at,
            attrs=self._validated_attrs(OBJECT_SCHEMAS[object_type], attrs),
        )
        self.objects.append(record)
        self.object_index[object_id] = record
        return object_id

    def update_object(self, object_id: str, **attrs: object) -> None:
        record = self.object_index[object_id]
        valid_names = {name for name, _ in OBJECT_SCHEMAS[record.object_type]}
        invalid = sorted(set(attrs) - valid_names)
        if invalid:
            raise ValueError(f"Unknown object attributes for {record.object_type!r}: {invalid}")
        record.attrs.update(attrs)

    def add_event(
        self,
        event_type: str,
        happened_at: datetime,
        object_ids: list[str],
        **attrs: object,
    ) -> str:
        self._event_counter += 1
        event_id = f"ev-{self._event_counter:08d}"
        self.events.append(
            EventRecord(
                event_id=event_id,
                event_type=event_type,
                happened_at=happened_at,
                attrs=self._validated_attrs(EVENT_SCHEMAS[event_type], attrs),
            )
        )
        for object_id in dict.fromkeys(object_ids):
            self.event_object_rows.append((event_id, object_id))
        return event_id

    def link_objects(self, source_id: str, target_id: str) -> None:
        if source_id == target_id:
            return
        ordered = tuple(sorted((source_id, target_id)))
        self.object_object_rows.add(ordered)

    def write_sqlite(self, target: Path) -> None:
        if target.exists():
            target.unlink()

        target.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(target) as con:
            con.execute("CREATE TABLE event_map_type (ocel_type TEXT, ocel_type_map TEXT)")
            con.execute("CREATE TABLE object_map_type (ocel_type TEXT, ocel_type_map TEXT)")
            con.execute("CREATE TABLE event (ocel_id TEXT, ocel_type TEXT)")
            con.execute("CREATE TABLE object (ocel_id TEXT, ocel_type TEXT)")
            con.execute(
                "CREATE TABLE event_object (ocel_event_id TEXT, ocel_object_id TEXT)"
            )
            con.execute(
                "CREATE TABLE object_object (ocel_source_id TEXT, ocel_target_id TEXT)"
            )

            event_map_rows = [
                (event_type, _slugify(event_type))
                for event_type in EVENT_SCHEMAS
            ]
            object_map_rows = [
                (object_type, _slugify(object_type))
                for object_type in OBJECT_SCHEMAS
            ]
            con.executemany("INSERT INTO event_map_type VALUES (?, ?)", event_map_rows)
            con.executemany("INSERT INTO object_map_type VALUES (?, ?)", object_map_rows)

            for event_type, schema in EVENT_SCHEMAS.items():
                table_name = f'event_{_slugify(event_type)}'
                self._create_payload_table(
                    con,
                    table_name,
                    base_columns=(
                        ("ocel_id", "TEXT"),
                        ("ocel_time", "TEXT"),
                    ),
                    custom_columns=schema,
                )

            for object_type, schema in OBJECT_SCHEMAS.items():
                table_name = f'object_{_slugify(object_type)}'
                self._create_payload_table(
                    con,
                    table_name,
                    base_columns=(
                        ("ocel_id", "TEXT"),
                        ("ocel_time", "TEXT"),
                        ("ocel_changed_field", "TEXT"),
                    ),
                    custom_columns=schema,
                )

            con.executemany(
                "INSERT INTO event VALUES (?, ?)",
                [(event.event_id, event.event_type) for event in self.events],
            )
            con.executemany(
                "INSERT INTO object VALUES (?, ?)",
                [(record.object_id, record.object_type) for record in self.objects],
            )
            con.executemany(
                "INSERT INTO event_object VALUES (?, ?)",
                self.event_object_rows,
            )
            con.executemany(
                "INSERT INTO object_object VALUES (?, ?)",
                sorted(self.object_object_rows),
            )

            for event_type, schema in EVENT_SCHEMAS.items():
                table_name = f'event_{_slugify(event_type)}'
                rows = [
                    (
                        event.event_id,
                        _timestamp(event.happened_at),
                        *[event.attrs.get(name) for name, _ in schema],
                    )
                    for event in self.events
                    if event.event_type == event_type
                ]
                self._insert_rows(
                    con,
                    table_name,
                    column_count=2 + len(schema),
                    rows=rows,
                )

            for object_type, schema in OBJECT_SCHEMAS.items():
                table_name = f'object_{_slugify(object_type)}'
                rows = [
                    (
                        record.object_id,
                        _timestamp(record.created_at),
                        None,
                        *[record.attrs.get(name) for name, _ in schema],
                    )
                    for record in self.objects
                    if record.object_type == object_type
                ]
                self._insert_rows(
                    con,
                    table_name,
                    column_count=3 + len(schema),
                    rows=rows,
                )

    @property
    def event_count(self) -> int:
        return len(self.events)

    @property
    def object_count(self) -> int:
        return len(self.objects)

    def _next_object_id(self, object_type: str) -> str:
        counter = self._object_counters.get(object_type, 0) + 1
        self._object_counters[object_type] = counter
        return f"{_slugify(object_type)}-{counter:06d}"

    @staticmethod
    def _validated_attrs(
        schema: tuple[tuple[str, str], ...],
        attrs: dict[str, object],
    ) -> dict[str, object]:
        valid_names = {name for name, _ in schema}
        invalid = sorted(set(attrs) - valid_names)
        if invalid:
            raise ValueError(f"Unknown attributes: {invalid}")
        return dict(attrs)

    @staticmethod
    def _create_payload_table(
        con: sqlite3.Connection,
        table_name: str,
        *,
        base_columns: tuple[tuple[str, str], ...],
        custom_columns: tuple[tuple[str, str], ...],
    ) -> None:
        column_sql = ", ".join(
            f'"{name}" {sql_type}'
            for name, sql_type in (*base_columns, *custom_columns)
        )
        con.execute(f'CREATE TABLE "{table_name}" ({column_sql})')

    @staticmethod
    def _insert_rows(
        con: sqlite3.Connection,
        table_name: str,
        *,
        column_count: int,
        rows: list[tuple[object, ...]],
    ) -> None:
        if not rows:
            return
        placeholders = ", ".join("?" for _ in range(column_count))
        con.executemany(
            f'INSERT INTO "{table_name}" VALUES ({placeholders})',
            rows,
        )


@dataclass
class ItemSpec:
    category: str
    quantity: int
    unit_price: float
    fulfillment_mode: str
    sku: str
    item_id: str | None = None
    ready_at: datetime | None = None


def build_running_example(order_count: int, seed: int) -> StrictOCELBuilder:
    rng = random.Random(seed)
    builder = StrictOCELBuilder()
    start = datetime(2024, 1, 1, 8, 0, 0)

    customer_ids = _create_customers(builder, rng, order_count, start)
    for index in range(order_count):
        _generate_order(builder, rng, customer_ids, start, index)

    return builder


def _create_customers(
    builder: StrictOCELBuilder,
    rng: random.Random,
    order_count: int,
    start: datetime,
) -> list[str]:
    customer_count = max(350, order_count // 3)
    customers: list[str] = []
    for _ in range(customer_count):
        created_at = start - timedelta(days=rng.randint(30, 240))
        customer_id = builder.create_object(
            "customer",
            created_at,
            region=_weighted_choice(rng, REGIONS, [30, 16, 12, 22, 8]),
            segment=_weighted_choice(rng, SEGMENTS, [50, 30, 20]),
            tier=_weighted_choice(rng, TIERS, [42, 30, 20, 8]),
        )
        customers.append(customer_id)
    return customers


def _generate_order(
    builder: StrictOCELBuilder,
    rng: random.Random,
    customer_ids: list[str],
    start: datetime,
    index: int,
) -> None:
    created_at = start + timedelta(minutes=index * rng.randint(12, 65))
    customer_id = rng.choice(customer_ids)
    channel = _weighted_choice(rng, CHANNELS, [45, 18, 22, 15])
    priority = _weighted_choice(rng, PRIORITIES, [62, 26, 12])
    payment_method = _weighted_choice(rng, PAYMENT_METHODS, [38, 10, 36, 16])
    risk_bucket = _weighted_choice(rng, RISK_BUCKETS, [56, 28, 12, 4])
    country, is_international = _weighted_choice(
        rng,
        COUNTRIES,
        [22, 14, 12, 8, 10, 18, 8, 8],
    )
    payment_terms_days = {"card": 0, "wallet": 0, "invoice": 30, "wire": 14}[payment_method]

    item_specs = _generate_item_specs(rng)
    order_value = round(
        sum(item.quantity * item.unit_price for item in item_specs),
        2,
    )

    order_id = builder.create_object(
        "order",
        created_at,
        channel=channel,
        country=country,
        priority=priority,
        payment_method=payment_method,
        payment_terms_days=payment_terms_days,
        risk_bucket=risk_bucket,
        order_value=order_value,
        status="open",
    )
    builder.link_objects(customer_id, order_id)

    item_ids: list[str] = []
    for spec in item_specs:
        item_id = builder.create_object(
            "order_item",
            created_at + timedelta(minutes=rng.randint(1, 25)),
            sku=spec.sku,
            category=spec.category,
            quantity=spec.quantity,
            unit_price=spec.unit_price,
            fulfillment_mode=spec.fulfillment_mode,
        )
        spec.item_id = item_id
        item_ids.append(item_id)
        builder.link_objects(order_id, item_id)

    builder.add_event(
        "Place Order",
        created_at,
        [order_id, customer_id, *item_ids],
        channel=channel,
        country=country,
        priority=priority,
        item_count=len(item_specs),
        order_value=order_value,
    )

    current_time = created_at + timedelta(minutes=rng.randint(10, 45))
    if _needs_fraud_review(rng, risk_bucket, channel, is_international):
        review_start = current_time
        builder.add_event(
            "Start Fraud Review",
            review_start,
            [order_id, customer_id],
            risk_bucket=risk_bucket,
            review_queue=_weighted_choice(
                rng,
                ["auto", "regional", "senior"],
                [50, 35, 15],
            ),
        )
        review_end = review_start + timedelta(minutes=rng.randint(40, 320))
        if _is_rejected(rng, risk_bucket):
            builder.add_event(
                "Reject Order",
                review_end,
                [order_id, customer_id],
                reason=_weighted_choice(
                    rng,
                    ["fraud_risk", "sanctions_match", "address_inconsistency"],
                    [55, 10, 35],
                ),
            )
            builder.add_event(
                "Close Order",
                review_end + timedelta(minutes=10),
                [order_id],
                final_status="rejected",
                cycle_days=round((review_end - created_at).total_seconds() / 86_400, 2),
            )
            builder.update_object(order_id, status="rejected")
            return

        builder.add_event(
            "Clear Fraud Review",
            review_end,
            [order_id, customer_id],
            decision="approved",
            review_minutes=int((review_end - review_start).total_seconds() // 60),
        )
        current_time = review_end + timedelta(minutes=rng.randint(10, 35))

    supplier_order_ids: list[str] = []
    warehouses = rng.sample(WAREHOUSES, k=min(len(WAREHOUSES), 2))
    for spec in item_specs:
        assert spec.item_id is not None
        warehouse = rng.choice(warehouses)
        reserve_time = current_time + timedelta(minutes=rng.randint(5, 90))
        reservation_status = "reserved"

        if spec.fulfillment_mode != "stock":
            reservation_status = "backordered"

        builder.add_event(
            "Reserve Inventory",
            reserve_time,
            [order_id, spec.item_id],
            warehouse=warehouse,
            reservation_status=reservation_status,
        )

        if spec.fulfillment_mode == "stock":
            spec.ready_at = reserve_time + timedelta(minutes=rng.randint(25, 180))
            continue

        delay_days = rng.randint(2, 12)
        supplier = rng.choice(SUPPLIERS)
        builder.add_event(
            "Backorder Item",
            reserve_time + timedelta(minutes=10),
            [order_id, spec.item_id],
            shortage_reason=_weighted_choice(
                rng,
                ["supplier_delay", "demand_spike", "quality_hold"],
                [45, 35, 20],
            ),
            expected_delay_days=delay_days,
        )
        supplier_order_id = builder.create_object(
            "supplier_order",
            reserve_time + timedelta(minutes=20),
            supplier=supplier,
            lead_time_days=delay_days,
            status="received",
        )
        supplier_order_ids.append(supplier_order_id)
        builder.link_objects(order_id, supplier_order_id)
        builder.link_objects(spec.item_id, supplier_order_id)
        builder.add_event(
            "Create Supplier Order",
            reserve_time + timedelta(minutes=20),
            [order_id, spec.item_id, supplier_order_id],
            supplier=supplier,
            lead_time_days=delay_days,
        )
        delivery_time = reserve_time + timedelta(days=delay_days, hours=rng.randint(2, 16))
        builder.add_event(
            "Receive Supplier Delivery",
            delivery_time,
            [order_id, spec.item_id, supplier_order_id],
            supplier=supplier,
            received_quantity=spec.quantity,
        )
        spec.ready_at = delivery_time + timedelta(minutes=rng.randint(50, 240))

    latest_ready = max(spec.ready_at for spec in item_specs if spec.ready_at is not None)
    assert latest_ready is not None

    package_groups = _assign_packages(rng, item_specs, is_international)
    package_ids: list[str] = []
    package_ready_times: dict[str, datetime] = {}
    for package_index, group in enumerate(package_groups):
        package_items = [spec.item_id for spec in group]
        package_weight = round(
            sum(spec.quantity * _category_weight(spec.category) for spec in group),
            2,
        )
        carrier = _choose_carrier(rng, is_international)
        service_level = _choose_service_level(rng, priority)
        package_time = latest_ready + timedelta(minutes=25 + package_index * rng.randint(10, 50))
        package_id = builder.create_object(
            "package",
            package_time,
            carrier=carrier,
            service_level=service_level,
            package_weight=package_weight,
            zone=_shipping_zone(country, priority),
        )
        package_ids.append(package_id)
        package_ready_times[package_id] = package_time
        builder.link_objects(order_id, package_id)
        for item_id in package_items:
            assert item_id is not None
            builder.link_objects(item_id, package_id)

        for spec in group:
            assert spec.item_id is not None
            assert spec.ready_at is not None
            pick_time = max(spec.ready_at, package_time - timedelta(minutes=rng.randint(20, 90)))
            builder.add_event(
                "Pick Item",
                pick_time,
                [order_id, spec.item_id, package_id],
                warehouse=rng.choice(warehouses),
                picker_wave=f"W{rng.randint(1, 12):02d}",
            )

        builder.add_event(
            "Pack Package",
            package_time,
            [order_id, package_id, *[item_id for item_id in package_items if item_id is not None]],
            carrier=carrier,
            package_weight=package_weight,
            package_items=len(group),
        )

    shipment_groups = _assign_shipments(rng, package_ids, is_international)
    shipment_ids: list[str] = []
    shipment_delivery_times: list[datetime] = []
    for shipment_index, package_group in enumerate(shipment_groups):
        shipment_time = max(package_ready_times[package_id] for package_id in package_group)
        shipment_time += timedelta(minutes=rng.randint(20, 180))

        carrier = builder.object_index[package_group[0]].attrs["carrier"]
        service_level = builder.object_index[package_group[0]].attrs["service_level"]
        had_customs_hold = is_international and rng.random() < 0.22
        attempt_count = 2 if rng.random() < 0.11 else 1
        lane = "Intercontinental-Air" if is_international else _weighted_choice(
            rng,
            ["DACH-Road", "Nordics-Road", "Benelux-Road"],
            [50, 20, 30],
        )

        shipment_id = builder.create_object(
            "shipment",
            shipment_time,
            carrier=carrier,
            lane=lane,
            is_international=is_international,
            had_customs_hold=had_customs_hold,
            attempt_count=attempt_count,
            final_status="delivered",
        )
        shipment_ids.append(shipment_id)
        builder.link_objects(order_id, shipment_id)
        for package_id in package_group:
            builder.link_objects(package_id, shipment_id)

        builder.add_event(
            "Create Shipment",
            shipment_time,
            [order_id, shipment_id, *package_group],
            carrier=str(carrier),
            service_level=str(service_level),
            package_count=len(package_group),
        )

        dispatch_time = shipment_time + timedelta(hours=rng.randint(2, 20))
        builder.add_event(
            "Dispatch Shipment",
            dispatch_time,
            [order_id, shipment_id, *package_group],
            hub=_weighted_choice(rng, ["Cologne", "Liege", "Frankfurt"], [45, 25, 30]),
            lane=lane,
        )

        delivery_start = dispatch_time
        if had_customs_hold:
            hold_days = rng.randint(1, 5)
            hold_time = dispatch_time + timedelta(days=rng.randint(1, 3))
            builder.add_event(
                "Customs Hold",
                hold_time,
                [order_id, shipment_id, *package_group],
                hold_reason=rng.choice(CUSTOMS_REASONS),
                extra_delay_days=hold_days,
            )
            release_time = hold_time + timedelta(days=hold_days, hours=rng.randint(4, 18))
            builder.add_event(
                "Customs Release",
                release_time,
                [order_id, shipment_id, *package_group],
                clearance_channel=_weighted_choice(
                    rng,
                    ["standard", "broker", "priority"],
                    [55, 30, 15],
                ),
            )
            delivery_start = release_time

        travel_days = rng.randint(1, 4) if not is_international else rng.randint(4, 10)
        delivered_at = delivery_start + timedelta(days=travel_days, hours=rng.randint(1, 20))
        if attempt_count > 1:
            failure_time = delivered_at - timedelta(hours=rng.randint(3, 12))
            builder.add_event(
                "Delivery Attempt Failed",
                failure_time,
                [order_id, shipment_id, *package_group],
                reason=_weighted_choice(
                    rng,
                    ["customer_absent", "address_issue", "capacity_overflow"],
                    [55, 30, 15],
                ),
                attempt_no=1,
            )
            delivered_at += timedelta(days=rng.randint(1, 3))

        promised_days = {"economy": 8, "standard": 5, "express": 3}[str(service_level)]
        builder.add_event(
            "Deliver Shipment",
            delivered_at,
            [order_id, shipment_id, *package_group],
            delivery_days=round((delivered_at - dispatch_time).total_seconds() / 86_400, 2),
            on_time=(delivered_at - created_at).days <= promised_days,
        )
        shipment_delivery_times.append(delivered_at)

    invoice_time = min(shipment_delivery_times) - timedelta(days=rng.randint(0, 2))
    invoice_id = builder.create_object(
        "invoice",
        invoice_time,
        currency="EUR",
        gross_amount=order_value,
        payment_terms_days=payment_terms_days,
        status="issued",
    )
    builder.link_objects(order_id, invoice_id)
    builder.add_event(
        "Create Invoice",
        invoice_time,
        [order_id, customer_id, invoice_id],
        payment_terms_days=payment_terms_days,
        gross_amount=order_value,
    )

    payment_ids: list[str] = []
    last_payment_time = _create_payments(
        builder,
        rng,
        order_id=order_id,
        invoice_id=invoice_id,
        payment_method=payment_method,
        created_at=created_at,
        delivered_at=max(shipment_delivery_times),
        order_value=order_value,
        payment_terms_days=payment_terms_days,
        payment_ids=payment_ids,
    )

    return_case_ids: list[str] = []
    last_return_time = last_payment_time
    for spec in item_specs:
        assert spec.item_id is not None
        if not _item_is_returned(rng, spec.category, is_international):
            continue

        return_created_at = max(shipment_delivery_times) + timedelta(days=rng.randint(2, 21))
        reason = rng.choice(RETURN_REASONS)
        refundable_amount = round(spec.quantity * spec.unit_price * rng.uniform(0.75, 1.0), 2)
        resolution = _weighted_choice(
            rng,
            ["refund", "partial_refund", "store_credit"],
            [70, 20, 10],
        )
        return_case_id = builder.create_object(
            "return_case",
            return_created_at,
            reason=reason,
            resolution=resolution,
            refundable_amount=refundable_amount,
        )
        return_case_ids.append(return_case_id)
        builder.link_objects(order_id, return_case_id)
        builder.link_objects(spec.item_id, return_case_id)

        request_time = return_created_at
        approve_time = request_time + timedelta(hours=rng.randint(2, 28))
        receive_time = approve_time + timedelta(days=rng.randint(2, 9))
        inspect_time = receive_time + timedelta(hours=rng.randint(4, 20))

        builder.add_event(
            "Request Return",
            request_time,
            [order_id, spec.item_id, return_case_id],
            reason=reason,
            requested_quantity=spec.quantity,
        )
        builder.add_event(
            "Approve Return",
            approve_time,
            [order_id, spec.item_id, return_case_id],
            resolution_path=resolution,
        )
        builder.add_event(
            "Receive Return",
            receive_time,
            [order_id, spec.item_id, return_case_id],
            days_after_delivery=(receive_time - max(shipment_delivery_times)).days,
        )

        disposition = _weighted_choice(
            rng,
            ["restock", "scrap"],
            [72, 28],
        )
        recoverable_value = round(refundable_amount * rng.uniform(0.35, 0.95), 2)
        builder.add_event(
            "Inspect Return",
            inspect_time,
            [order_id, spec.item_id, return_case_id],
            disposition=disposition,
            recoverable_value=recoverable_value,
        )

        if disposition == "restock":
            builder.add_event(
                "Restock Item",
                inspect_time + timedelta(hours=2),
                [order_id, spec.item_id, return_case_id],
                warehouse=rng.choice(WAREHOUSES),
            )
        else:
            builder.add_event(
                "Scrap Item",
                inspect_time + timedelta(hours=2),
                [order_id, spec.item_id, return_case_id],
                scrap_reason=rng.choice(SCRAP_REASONS),
            )

        refund_objects = [order_id, return_case_id]
        if payment_ids:
            builder.link_objects(payment_ids[0], return_case_id)
            refund_objects.append(payment_ids[0])

        refund_time = inspect_time + timedelta(days=rng.randint(0, 4))
        builder.add_event(
            "Issue Refund",
            refund_time,
            refund_objects,
            refund_amount=refundable_amount,
            refund_type=resolution,
        )
        last_return_time = max(last_return_time, refund_time)

    final_status = _final_order_status(return_case_ids)
    close_time = max(max(shipment_delivery_times), last_payment_time, last_return_time)
    close_time += timedelta(hours=rng.randint(2, 20))
    builder.add_event(
        "Close Order",
        close_time,
        [order_id],
        final_status=final_status,
        cycle_days=round((close_time - created_at).total_seconds() / 86_400, 2),
    )

    builder.update_object(order_id, status=final_status)
    builder.update_object(invoice_id, status="paid")


def _create_payments(
    builder: StrictOCELBuilder,
    rng: random.Random,
    *,
    order_id: str,
    invoice_id: str,
    payment_method: str,
    created_at: datetime,
    delivered_at: datetime,
    order_value: float,
    payment_terms_days: int,
    payment_ids: list[str],
) -> datetime:
    if payment_method in {"card", "wallet"}:
        paid_at = created_at + timedelta(hours=rng.randint(1, 18))
        payment_id = builder.create_object(
            "payment",
            paid_at,
            method=payment_method,
            amount=order_value,
            status="settled",
        )
        payment_ids.append(payment_id)
        builder.link_objects(order_id, payment_id)
        builder.link_objects(invoice_id, payment_id)
        builder.add_event(
            "Receive Payment",
            paid_at,
            [order_id, invoice_id, payment_id],
            method=payment_method,
            amount=order_value,
            days_to_pay=0,
        )
        return paid_at

    due_at = delivered_at + timedelta(days=payment_terms_days)
    send_reminder = rng.random() < (0.38 if payment_method == "invoice" else 0.18)
    if send_reminder:
        builder.add_event(
            "Send Payment Reminder",
            due_at + timedelta(days=rng.randint(1, 9)),
            [order_id, invoice_id],
            reminder_level=rng.randint(1, 2),
        )

    payment_splits = 2 if order_value > 1_600 and rng.random() < 0.25 else 1
    last_payment_time = due_at
    remaining = order_value
    for split_index in range(payment_splits):
        if split_index == payment_splits - 1:
            amount = round(remaining, 2)
        else:
            amount = round(order_value * rng.uniform(0.4, 0.65), 2)
            remaining = round(remaining - amount, 2)

        paid_at = due_at + timedelta(days=rng.randint(0, 20 if send_reminder else 8))
        payment_id = builder.create_object(
            "payment",
            paid_at,
            method=payment_method,
            amount=amount,
            status="settled",
        )
        payment_ids.append(payment_id)
        builder.link_objects(order_id, payment_id)
        builder.link_objects(invoice_id, payment_id)
        builder.add_event(
            "Receive Payment",
            paid_at,
            [order_id, invoice_id, payment_id],
            method=payment_method,
            amount=amount,
            days_to_pay=(paid_at - created_at).days,
        )
        last_payment_time = max(last_payment_time, paid_at)
        due_at = paid_at + timedelta(days=rng.randint(2, 9))

    return last_payment_time


def _generate_item_specs(rng: random.Random) -> list[ItemSpec]:
    item_count = _weighted_choice(rng, [1, 2, 3, 4, 5], [20, 28, 24, 18, 10])
    specs: list[ItemSpec] = []
    for _ in range(item_count):
        category = _weighted_choice(rng, CATEGORIES, [28, 20, 18, 16, 18])
        quantity = _weighted_choice(rng, [1, 2, 3, 4], [62, 24, 10, 4])
        fulfillment_mode = _weighted_choice(
            rng,
            ["stock", "backorder", "drop_ship"],
            [76, 18, 6],
        )
        unit_price = round(_price_for_category(rng, category), 2)
        specs.append(
            ItemSpec(
                category=category,
                quantity=quantity,
                unit_price=unit_price,
                fulfillment_mode=fulfillment_mode,
                sku=_build_sku(rng, category),
            )
        )
    return specs


def _assign_packages(
    rng: random.Random,
    item_specs: list[ItemSpec],
    is_international: bool,
) -> list[list[ItemSpec]]:
    package_count = 1
    if len(item_specs) >= 3 and rng.random() < 0.55:
        package_count += 1
    if len(item_specs) >= 4 and (is_international or rng.random() < 0.35):
        package_count += 1
    package_count = min(package_count, len(item_specs))

    shuffled = item_specs[:]
    rng.shuffle(shuffled)
    groups = [[] for _ in range(package_count)]
    for index, spec in enumerate(shuffled):
        groups[index % package_count].append(spec)
    return groups


def _assign_shipments(
    rng: random.Random,
    package_ids: list[str],
    is_international: bool,
) -> list[list[str]]:
    shipment_count = 1
    if len(package_ids) >= 3 and rng.random() < 0.42:
        shipment_count += 1
    if is_international and len(package_ids) >= 2 and rng.random() < 0.3:
        shipment_count += 1
    shipment_count = min(shipment_count, len(package_ids))

    shuffled = package_ids[:]
    rng.shuffle(shuffled)
    groups = [[] for _ in range(shipment_count)]
    for index, package_id in enumerate(shuffled):
        groups[index % shipment_count].append(package_id)
    return groups


def _needs_fraud_review(
    rng: random.Random,
    risk_bucket: str,
    channel: str,
    is_international: bool,
) -> bool:
    if risk_bucket == "critical":
        return True
    if risk_bucket == "high":
        return rng.random() < 0.8
    if channel == "marketplace" and is_international:
        return rng.random() < 0.35
    return rng.random() < 0.08


def _is_rejected(rng: random.Random, risk_bucket: str) -> bool:
    rejection_rate = {
        "low": 0.01,
        "medium": 0.03,
        "high": 0.08,
        "critical": 0.18,
    }[risk_bucket]
    return rng.random() < rejection_rate


def _item_is_returned(rng: random.Random, category: str, is_international: bool) -> bool:
    base_rate = {
        "electronics": 0.06,
        "fashion": 0.22,
        "home": 0.08,
        "industrial": 0.04,
        "spare_parts": 0.1,
    }[category]
    if is_international:
        base_rate += 0.03
    return rng.random() < base_rate


def _final_order_status(return_case_ids: list[str]) -> str:
    if not return_case_ids:
        return "delivered"
    if len(return_case_ids) == 1:
        return "returned_partial"
    return "returned_multi"


def _price_for_category(rng: random.Random, category: str) -> float:
    price_bands = {
        "electronics": (120, 950),
        "fashion": (25, 220),
        "home": (35, 420),
        "industrial": (160, 1_200),
        "spare_parts": (18, 280),
    }
    low, high = price_bands[category]
    return rng.uniform(low, high)


def _build_sku(rng: random.Random, category: str) -> str:
    prefix = {
        "electronics": "EL",
        "fashion": "FA",
        "home": "HO",
        "industrial": "IN",
        "spare_parts": "SP",
    }[category]
    return f"{prefix}-{rng.randint(1000, 9999)}-{rng.randint(10, 99)}"


def _category_weight(category: str) -> float:
    return {
        "electronics": 1.8,
        "fashion": 0.5,
        "home": 2.4,
        "industrial": 4.8,
        "spare_parts": 0.9,
    }[category]


def _choose_carrier(rng: random.Random, is_international: bool) -> str:
    if is_international:
        return _weighted_choice(rng, CARRIERS, [18, 18, 20, 8, 36])
    return _weighted_choice(rng, CARRIERS, [40, 20, 18, 20, 2])


def _choose_service_level(rng: random.Random, priority: str) -> str:
    if priority == "expedite":
        return _weighted_choice(rng, SERVICE_LEVELS, [4, 22, 74])
    if priority == "priority":
        return _weighted_choice(rng, SERVICE_LEVELS, [14, 48, 38])
    return _weighted_choice(rng, SERVICE_LEVELS, [36, 52, 12])


def _shipping_zone(country: str, priority: str) -> str:
    if country in {"Germany", "Netherlands", "Belgium"}:
        return "near_eu"
    if country in {"Sweden", "United Kingdom"}:
        return "regional"
    if priority == "expedite":
        return "air_priority"
    return "global"


def _weighted_choice[T](
    rng: random.Random,
    values: list[T],
    weights: list[int],
) -> T:
    return rng.choices(values, weights=weights, k=1)[0]


def _slugify(value: str) -> str:
    lowered = value.strip().lower().replace("&", "and")
    slug = re.sub(r"[^a-z0-9]+", "_", lowered).strip("_")
    return slug


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate the synthetic atlas-fulfillment OCEL running example.",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=DEFAULT_ORDER_COUNT,
        help=f"Number of orders to generate (default: {DEFAULT_ORDER_COUNT}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=Path("examples/data") / EXAMPLE_NAME,
        help="Output directory for the generated oceldb dataset.",
    )
    parser.add_argument(
        "--keep-sqlite",
        action="store_true",
        help="Keep the intermediate strict OCEL 2.0 SQLite file next to the target.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    builder = build_running_example(order_count=args.orders, seed=args.seed)
    target = args.target.expanduser().resolve()

    if args.keep_sqlite:
        sqlite_target = target.with_suffix(".sqlite")
        builder.write_sqlite(sqlite_target)
        written = convert_sqlite(sqlite_target, target, overwrite=True)
    else:
        with TemporaryDirectory(prefix="oceldb_running_example_") as tmpdir:
            sqlite_target = Path(tmpdir) / f"{EXAMPLE_NAME}.sqlite"
            builder.write_sqlite(sqlite_target)
            written = convert_sqlite(sqlite_target, target, overwrite=True)

    print(f"Generated {builder.event_count} events and {builder.object_count} objects.")
    print(f"Wrote running example to {written}")


if __name__ == "__main__":
    main()
