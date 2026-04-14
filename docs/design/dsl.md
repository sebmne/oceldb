# DSL Design

## Status

This document defines the target architecture for the `oceldb` query DSL.

It is the design contract the implementation should converge to. The current
runtime already implements parts of this design, but some details described
here are still target behavior rather than completed behavior.

For the boundary between direct log inspection and mined analytical artifacts,
see [`inspection-vs-discovery.md`](./inspection-vs-discovery.md).

For the planned state-oriented lifecycle discovery artifact, see
[`object-lifecycle.md`](./object-lifecycle.md).

## Goals

The DSL should be:

- lazy
- Polars-like in feel
- explicit about OCEL semantics
- extendable without rewriting the planner
- strict about query phase legality
- strict about scope legality
- independent of schema generation

The DSL should help users express OCEL analysis naturally while keeping the log
on disk and preserving enough structure to materialize valid sublogs.

## Non-Goals

The DSL does not aim to provide:

- compile-time type safety in Python
- generated dataset-specific Python APIs
- public AST classes as part of the user-facing API
- arbitrary relational algebra beyond the needs of OCEL analysis
- backend independence as a primary goal

Runtime validation remains part of the design. The goal is to make invalid
queries fail early and clearly, not to make them impossible at Python parse
time.

## Core Principles

- Keep `OCEL` thin and stable.
- Keep the AST internal.
- Use free expression builders in a Polars-like style.
- Keep legality in query phases where possible.
- Keep scope validation in the compiler where necessary.
- Separate expression structure from logical planning.
- Make materialization legality an explicit property of the plan.
- Keep higher-level inspection and discovery helpers outside `OCEL` itself.

## Public Query Roots

The stable query entrypoint is `ocel.query`.

It should expose exactly these roots:

- `ocel.query.events(*event_types: str) -> EventRows`
- `ocel.query.objects(*object_types: str) -> ObjectRows`
- `ocel.query.object_changes(*object_types: str) -> ObjectChangeRows`
- `ocel.query.object_states(*object_types: str) -> ObjectStateSeed`
- `ocel.query.event_objects() -> EventObjectRows`
- `ocel.query.object_objects() -> ObjectObjectRows`

`object_states(...)` is intentionally a seed phase. Users must choose an
explicit temporal projection before the result becomes a row query.

## Public Query Types

The public query surface is phase-typed. The runtime does not need a separate
implementation class for every phase, but the public method surface must differ
by phase and capability.

The target public query types are:

- `EventRows`
- `ObjectRows`
- `ObjectChangeRows`
- `ObjectStateSeed`
- `ObjectStateRows`
- `EventObjectRows`
- `ObjectObjectRows`
- `SelectedRows`
- `GroupedRows`
- `AggregatedRows`

## Public DSL Builders

The DSL should prefer free builders over query-bound builders.

Core builders:

- `col(name: str)`
- `lit(value)`
- `count()`
- `count_distinct(expr)`
- `min_(expr)`
- `max_(expr)`
- `sum_(expr)`
- `avg(expr)`
- `asc(expr_or_name)`
- `desc(expr_or_name)`

Relation builders:

- `cooccurs_with(object_type: str)`
- `linked(object_type: str)`
- `has_event(event_type: str)`
- `has_object(object_type: str)`

The surrounding query root provides the scope in which these builders are
interpreted.

## Public Query Semantics

### Event Queries

`events(...)` returns event rows:

- one row per event
- event core columns
- typed custom event attributes

### Object Identity Queries

`objects(...)` returns object identities:

- one row per object identity
- `ocel_id`
- `ocel_type`
- no temporal object state columns

### Object Change Queries

`object_changes(...)` returns raw object history rows:

- one row per stored change row
- `ocel_id`
- `ocel_type`
- `ocel_time`
- `ocel_changed_field`
- typed object-history attributes

### Object State Queries

`object_states(...)` is a temporal projection seed.

- `latest()` returns one reconstructed snapshot row per object
- `as_of(value)` returns one reconstructed snapshot row per object at the given
  cutoff

Each `ObjectStateRows` row contains:

- `ocel_id`
- `ocel_type`
- `ocel_time`
- reconstructed object-state attributes

It does not expose `ocel_changed_field`.

Objects without any history row must still appear in `ObjectStateRows`, with
`ocel_time = NULL` and projected attributes `NULL`.

### Raw Relation Queries

`event_objects()` exposes the raw event-to-object relation table.

`object_objects()` exposes the raw object-to-object relation table.

These roots are intentionally low-level and correspond directly to the canonical
storage tables.

## Relation Semantics

High-level relation predicates are expressed through free builders and are valid
only in specific scopes.

### Object-Rooted Relation Predicates

These are valid only in object-rooted scopes:

- `cooccurs_with(object_type)`
- `linked(object_type)`
- `has_event(event_type)`

Semantics:

- `cooccurs_with(object_type)` means objects connected through shared events
- `linked(object_type)` means objects connected through `object_object`
- `has_event(event_type)` means events attached through `event_object`

### Event-Rooted Relation Predicates

`has_object(object_type)` is valid only in event-rooted scopes.

Semantics:

- it navigates from an event to co-occurring objects through `event_object`
- if a predicate is applied to the co-occurring object, that predicate should see
  object state at event time, not latest object state

This introduces an additional internal scope kind:

- `object_state_at_event`

That scope is not public, but it is required for correct validation and SQL
lowering of event-rooted object predicates such as:

```python
ocel.query.events("Ship Order").where(
    has_object("order").any(col("status") == "open")
)
```

## Public Method Matrix

### Row Queries

`EventRows`, `ObjectRows`, `ObjectChangeRows`, `ObjectStateRows`

- `where(*predicates: BoolExpr) -> Self`
- `with_columns(*exprs, **named_exprs) -> Self`
- `select(*exprs, **named_exprs) -> SelectedRows`
- `group_by(*exprs) -> GroupedRows`
- `sort(*exprs, descending: bool = False) -> Self`
- `unique() -> Self`
- `limit(n: int) -> Self`
- `collect() -> duckdb.DuckDBPyRelation`
- `count() -> int`
- `exists() -> bool`
- `scalar() -> Any`
- `to_sql() -> str`

### Materializable Row Queries

Only identity-preserving roots expose:

- `ids() -> list[str]`
- `to_ocel() -> OCEL`
- `write(target, overwrite=False) -> Path`

This applies to:

- `EventRows`
- `ObjectRows`
- `ObjectStateRows`

It does not apply to:

- `ObjectChangeRows`
- `SelectedRows`
- `AggregatedRows`
- raw relation queries

### Object State Seed

`ObjectStateSeed`

- `latest() -> ObjectStateRows`
- `as_of(value: date | datetime | str) -> ObjectStateRows`

### Raw Relation Queries

`EventObjectRows`, `ObjectObjectRows`

- `where(*predicates) -> Self`
- `with_columns(*exprs, **named_exprs) -> Self`
- `select(*exprs, **named_exprs) -> SelectedRows`
- `group_by(*exprs) -> GroupedRows`
- `sort(...) -> Self`
- `unique() -> Self`
- `limit(n: int) -> Self`
- `collect() -> duckdb.DuckDBPyRelation`
- `count() -> int`
- `exists() -> bool`
- `scalar() -> Any`
- `to_sql() -> str`

### Selected Rows

`SelectedRows`

- `where(*predicates) -> Self`
- `sort(...) -> Self`
- `unique() -> Self`
- `limit(n: int) -> Self`
- `collect() -> duckdb.DuckDBPyRelation`
- `count() -> int`
- `exists() -> bool`
- `scalar() -> Any`
- `to_sql() -> str`

`SelectedRows` deliberately does not expose `ids()`, `to_ocel()`, or `write()`.

### Grouped Rows

`GroupedRows`

- `agg(*exprs, **named_exprs) -> AggregatedRows`

### Aggregated Rows

`AggregatedRows`

- `having(*predicates: BoolExpr) -> Self`
- `select(*exprs, **named_exprs) -> SelectedRows`
- `sort(...) -> Self`
- `limit(n: int) -> Self`
- `collect() -> duckdb.DuckDBPyRelation`
- `count() -> int`
- `exists() -> bool`
- `scalar() -> Any`
- `to_sql() -> str`

`having(...)` is preferred over reusing `where(...)` for grouped results. It
makes grouped-query semantics explicit and leaves less ambiguity in the public
contract.

## Expression Model

The public expression categories are:

- `ScalarExpr`
- `BoolExpr`
- `AggregateExpr`
- `SortExpr`

Rules:

- `where(...)` accepts only boolean expressions
- `having(...)` accepts only boolean expressions over grouped outputs
- `with_columns(...)` accepts only scalar expressions
- `select(...)` accepts only scalar expressions
- `group_by(...)` accepts only scalar expressions
- `agg(...)` accepts only aggregate expressions
- aggregate expressions are not valid inside row-level `where(...)`
- relation predicates are valid only in their supported scopes

## Expression AST

The expression AST should be normalized around node families rather than one
Python class per individual SQL function.

Target node families:

- `ColumnRef(name)`
- `Literal(value)`
- `Alias(expr, name)`
- `Cast(expr, sql_type)`
- `Compare(op, left, right)`
- `UnaryPredicate(op, expr)`
- `Logical(op, operands)`
- `InList(expr, values)`
- `ScalarCall(name, args)`
- `AggregateCall(name, args, distinct=False)`
- `RelationSpec(kind, target_type, filters=())`
- `RelationExists(spec)`
- `RelationCount(spec)`
- `RelationAll(spec, predicate)`
- `SortKey(expr, descending=False)`

Benefits:

- easier extension for new scalar and aggregate functions
- less compiler boilerplate
- fewer visitor methods
- easier AST rewrites and validation passes

The AST remains internal.

## Logical Plan AST

The logical plan should be represented as an immutable tree, not as a public
query wrapper plus a flat tuple of operations.

Target plan nodes:

- `SourcePlan(source)`
- `FilterPlan(input, predicates)`
- `ExtendPlan(input, assignments)`
- `ProjectPlan(input, projections)`
- `GroupPlan(input, keys, aggregations)`
- `SortPlan(input, orderings)`
- `DistinctPlan(input)`
- `LimitPlan(input, n)`

Target source variants:

- `EventSource(types)`
- `ObjectSource(types)`
- `ObjectChangeSource(types)`
- `ObjectStateSource(types, mode, as_of=None)`
- `EventObjectSource()`
- `ObjectObjectSource()`

The plan tree is the internal contract consumed by validation, schema
inference, SQL rendering, and materialization planning.

## Internal Scope Model

Internal validation and rendering should distinguish at least these scope kinds:

- `event`
- `object`
- `object_change`
- `object_state`
- `event_object`
- `object_object`
- `object_state_at_event`

The scope model determines:

- which columns are available
- which relation builders are legal
- how relation predicates are rendered
- whether `ocel_type` filtering is legal
- whether later materialization remains valid

## Compiler Passes

The compiler should be split into passes rather than concentrated in a single
query wrapper module.

Recommended passes:

- schema inference
- validation
- output naming
- SQL lowering
- materialization planning

Each pass should operate on the immutable logical plan.

## Public Query Mixins

Mixins are appropriate for the public query surface because they express
capabilities directly in the Python API.

Recommended mixins:

- `ExecutionMixin`
- `WhereMixin`
- `HavingMixin`
- `SelectMixin`
- `WithColumnsMixin`
- `GroupByMixin`
- `SortMixin`
- `UniqueMixin`
- `LimitMixin`
- `MaterializeMixin`
- `ObjectStateProjectionMixin`

Use mixins only for the public fluent API. Do not use them to structure the AST
or compiler passes.

## Materialization Rules

Materialization legality should be derived from the logical plan, not inferred
informally from the public query wrapper type.

At minimum, the plan should track:

- whether identity rows are still preserved
- whether the preserved identity kind is `event` or `object`
- whether closure materialization is therefore legal

Current materialization semantics remain:

- event-rooted materialization keeps selected events, all linked objects, and
  `object_object` edges between included objects
- object-rooted materialization keeps selected logical objects even if they have
  no linked events, then adds linked events and participating objects

## Error Model

The DSL should fail early with explicit, scope-aware errors.

Examples:

- unknown column in current scope
- relation builder used in unsupported scope
- aggregate expression used in row-level `where(...)`
- attempt to materialize a non-identity-preserving plan
- missing temporal projection on `object_states(...)`

Errors should prefer actionable messages over generic type failures.

## Extension Rule

Every new DSL feature should fit the same pipeline:

1. add or extend the public builder or query method
2. add or extend internal AST nodes
3. extend scope and schema inference
4. extend validation
5. extend SQL lowering
6. extend output naming if needed
7. extend materialization planning only if identity semantics change

If a feature cannot be added cleanly through this pipeline, it likely does not
belong in the core DSL.

## Accepted Direction

The accepted direction for `oceldb` is:

- free DSL builders, not query-bound relation builders
- strict public query phases
- strict expression-kind validation
- no generated schema API
- no runtime `.c` namespace
- normalized internal AST
- immutable logical plan tree
- compiler passes over the plan

This keeps the DSL readable and Polars-like while still making OCEL-specific
semantics explicit and extensible.
