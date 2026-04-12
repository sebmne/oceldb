# Running Example

The `atlas-fulfillment` running example is a synthetic OCEL for a fulfillment
and reverse-logistics process. It is intentionally more complex than the tiny
test fixture and is meant to exercise the object-centric parts of the DSL.

The generated log contains these object types:

- `customer`
- `order`
- `order_item`
- `package`
- `shipment`
- `invoice`
- `payment`
- `return_case`
- `supplier_order`

The process includes these kinds of behavior:

- fraud review and order rejection
- backorders and supplier replenishment
- split packing and split shipping
- customs holds on international shipments
- failed delivery attempts
- invoicing and delayed payments with reminders
- returns, inspections, restocking, scrapping, and refunds

Generate the packaged running example:

```bash
uv run python examples/generate_running_example.py
```

Run the example analysis:

```bash
uv run python examples/analyze_running_example.py
```

The generated archive defaults to:

```text
examples/data/atlas-fulfillment.oceldb
```

If you want a larger or smaller version, change the order count:

```bash
uv run python examples/generate_running_example.py --orders 5000
uv run python examples/generate_running_example.py --orders 800
```
