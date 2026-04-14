# Object Lifecycle Design

## Status

This document defines the intended semantics and public API for a future
`oceldb.discovery.object_lifecycle(...)` helper.

It is a design contract. It does not describe an implemented feature yet.

## Goal

`object_lifecycle(...)` should describe how object state evolves over time.

The design must be:

- faithful to the canonical `object_change` table
- explicit about temporal semantics
- robust to sparse object history rows
- robust to multiple change rows with the same timestamp
- centered on OCEL object attributes rather than storage-level row mechanics
- specific enough that the result is interpretable on real logs

## Core Decision

Lifecycle is defined for one object type at a time and, by default, includes
all object attributes of that type.

Proposed public API:

```python
object_lifecycle(
    ocel: OCEL,
    object_type: str,
    *,
    attributes: tuple[str, ...] | None = None,
    include_null: bool = False,
    collapse_repeats: bool = True,
) -> ObjectLifecycle
```

Semantics:

- `attributes=None` means all custom object attributes available for the
  selected `object_type`
- `attributes=(...)` narrows the lifecycle to a selected attribute subset

## Why `attributes` Is Optional, Not Required

If the feature is called `object_lifecycle`, it should describe the lifecycle
of the object, not force users into a single-attribute projection as the
primary API.

Users should be able to write:

```python
object_lifecycle(ocel, "order")
```

and receive a lifecycle artifact over the full reconstructed object state.

Attribute narrowing is still useful, but it should be an optional refinement:

```python
object_lifecycle(ocel, "order", attributes=("status", "priority"))
```

## OCEL Terminology

The public API should use `attribute`, not `field`.

So the naming should be:

- `attributes`
- `changed_attributes`
- `LifecycleState.attributes`

The storage column `ocel_changed_field` may keep its existing internal name for
layout compatibility, but the public lifecycle API should use OCEL vocabulary.

## Input Semantics

The lifecycle is based on `object_change`, not on event participation.

The selected `attributes` must be custom object attributes from
`object_change` for the selected `object_type`.

The lifecycle is intentionally not defined over:

- `ocel_id`
- `ocel_type`
- `ocel_time`
- `ocel_changed_field`

If an attribute is unknown for the selected type, the function should raise a
clear error.

## Lifecycle Construction

For each object of the selected type:

1. Start from all rows in `object_change` for that object.
2. Group rows by `ocel_time`.
3. Reconstruct the post-timestamp object state using the same fill-forward
   semantics used by `object_states(...)`.
4. Project the selected `attributes`.
5. Optionally drop state snapshots that are entirely null if
   `include_null=False`.
6. Optionally collapse consecutive identical projected states if
   `collapse_repeats=True`.

The remaining ordered projected states form the lifecycle sequence for that
object.

## Why Timestamp Batches Matter

The storage model does not define a stable row order within the same object and
timestamp.

Therefore lifecycle should not depend on raw row order inside one timestamp.

Instead, the semantic unit is:

- "the reconstructed object state after all changes at timestamp `t`"

This avoids ambiguous micro-ordering and gives a stable, deterministic
interpretation.

If multiple updates to the same attribute happen at the same timestamp, only
the post-timestamp value is visible in the lifecycle.

That is an intentional limitation of the storage semantics.

## Role of `ocel_changed_field`

`ocel_changed_field` is useful metadata, but it should not define lifecycle
semantics by itself.

Lifecycle should be derived from reconstructed state snapshots, not from
blindly chaining raw change rows.

Reason:

- initialization rows may have `ocel_changed_field = NULL`
- unrelated attribute updates still matter for reconstructing the object state
  at a timestamp
- same-timestamp batching is easier to define on reconstructed states than on
  raw rows

`ocel_changed_field` can still be used later as an optimization hint, but it
should not be the semantic definition.

## Proposed Output Contract

```python
@dataclass(frozen=True)
class LifecycleState:
    attributes: tuple[tuple[str, object | None], ...]


@dataclass(frozen=True)
class LifecycleStateCount:
    state: LifecycleState
    count: int


@dataclass(frozen=True)
class LifecycleTransition:
    source: LifecycleState
    target: LifecycleState
    changed_attributes: tuple[str, ...]
    count: int
    mean_duration_seconds: float | None
    median_duration_seconds: float | None
    min_duration_seconds: float | None
    max_duration_seconds: float | None


@dataclass(frozen=True)
class ObjectLifecycle:
    object_type: str
    attributes: tuple[str, ...]
    include_null: bool
    collapse_repeats: bool
    object_count: int
    objects_with_changes: int
    objects_without_changes: int
    objects_with_lifecycle: int
    objects_without_lifecycle: int
    avg_steps_per_object: float | None
    median_steps_per_object: float | None
    min_steps_per_object: int | None
    max_steps_per_object: int | None
    states: tuple[LifecycleStateCount, ...]
    starts: tuple[LifecycleStateCount, ...]
    ends: tuple[LifecycleStateCount, ...]
    transitions: tuple[LifecycleTransition, ...]
```

## Output Semantics

### `LifecycleState`

A lifecycle state is the projected reconstructed object state at one retained
timestamp step.

The `attributes` tuple should be stable and ordered consistently, for example
by the selected `attributes` order.

### `states`

`states` counts how often a projected lifecycle state appears after applying
the configured null-handling and repeat-collapsing rules.

This is not raw row frequency.

It is frequency in the derived lifecycle representation.

### `starts`

`starts` counts the first lifecycle state per object.

### `ends`

`ends` counts the last lifecycle state per object.

### `transitions`

`transitions` counts adjacent lifecycle state changes inside the derived
sequence.

`changed_attributes` is the subset of selected attributes whose values differ
between source and target state.

Duration for a transition is measured as:

- timestamp of target lifecycle step
- minus timestamp of source lifecycle step

### `steps_per_object`

A lifecycle step is one retained lifecycle state in the derived sequence.

Examples:

- `[open]` has 1 step and 0 transitions
- `[open, packed, shipped]` has 3 steps and 2 transitions
- an empty lifecycle has 0 steps

## State Representation Tradeoff

Full-state lifecycle can create many distinct states.

That is acceptable as the default semantics, but the API should acknowledge
that users may sometimes want narrower views.

That is why `attributes=...` exists as an explicit narrowing parameter.

The design should not force a single attribute at the top level, but it should
allow users to reduce cardinality when needed.

## Null Handling

Default:

```python
include_null=False
```

Rationale:

- fully null projected states are often not analytically useful
- many logs use sparse updates
- including all-null states by default can dominate the artifact

When `include_null=False`:

- a projected state where all selected attributes are `NULL` is dropped from
  the lifecycle

When `include_null=True`:

- null-valued states are retained
- transitions involving null-valued states may appear

Important detail:

- `include_null=False` should not drop a state merely because one attribute is
  null
- it should only drop the state if all selected attributes are null

Otherwise sparse multi-attribute lifecycles would become unusable.

## Repeat Collapsing

Default:

```python
collapse_repeats=True
```

Rationale:

- lifecycle should usually reflect state changes, not repeated writes
- reconstructed snapshots can repeat identical projected states across
  timestamps because unrelated attributes changed outside the selected
  projection

Example:

- projected states: `[{"status": "open"}, {"status": "open"}, {"status": "packed"}]`
- with `collapse_repeats=True`: `[{"status": "open"}, {"status": "packed"}]`
- with `collapse_repeats=False`: all three states are retained

## Objects Without History

Objects from `object` with no rows in `object_change` must still count toward:

- `object_count`
- `objects_without_changes`

They do not contribute lifecycle steps unless a future design explicitly
introduces synthetic initial states. This design does not do that.

## Objects With Changes But No Lifecycle

An object may have change rows but still produce no lifecycle, for example:

- the selected projected states are entirely null and `include_null=False`
- all retained projected states collapse away after filtering

These objects count toward:

- `objects_with_changes`
- `objects_without_lifecycle`

## Example

Assume selected attributes `("status", "priority")` and reconstructed states:

```text
o1: (open, high) -> (packed, high) -> (shipped, high)
o2: (open, low)  -> (shipped, low)
o3: (open, low)
```

Then:

- `starts` contains `(open, high): 1` and `(open, low): 2`
- `ends` contains `(shipped, high): 1`, `(shipped, low): 1`, `(open, low): 1`
- `transitions` contains:
  - `(open, high) -> (packed, high)`
  - `(packed, high) -> (shipped, high)`
  - `(open, low) -> (shipped, low)`

with appropriate `changed_attributes`.

## Non-Goals

This design does not aim to provide:

- automatic phase discovery across all object attributes
- event-centric lifecycle summaries
- inferred business semantics beyond the selected attributes
- a mandatory single-attribute lifecycle API

Those may be added later as separate discovery artifacts or derived views.

## Relation to Other Surfaces

- `ocel.query.object_changes(...)` remains the raw history surface
- `ocel.query.object_states(...)` remains the row-oriented reconstructed state
  surface
- `oceldb.discovery.object_lifecycle(...)` becomes the higher-level mined
  lifecycle artifact over reconstructed object-state evolution

This keeps the DSL compositional and keeps lifecycle as an explicit discovery
layer.

## Likely Implementation Strategy

The implementation should:

1. validate `object_type`
2. resolve the selected `attributes`
3. build timestamp-batched reconstructed projected states per object
4. apply null filtering and repeat collapsing
5. derive per-object adjacent transitions
6. aggregate states, starts, ends, and transition durations

It should reuse the same state reconstruction semantics already used by
`object_states(...)` where possible.

## Open Questions

These should be decided only if real use cases demand them:

- Should the lifecycle artifact later expose a method like
  `lifecycle.project("status")` for attribute-specific views?
- Should there be a later companion artifact for phase discovery on top of
  lifecycle states?
- Should `include_null=True` remain boolean, or become a richer null policy?
- Should there be an option to include per-object lifecycle sequences for
  debugging?
- Should the lifecycle artifact later expose dwell-time statistics per state in
  addition to transition durations?
