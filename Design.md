# oceldb DSL — Design Document

## Status

Proposal for the v2 DSL rewrite. Supersedes the current implementation in
`oceldb/ast/`, `oceldb/dsl/`, `oceldb/query/`, and `oceldb/sql/`.

## Goals

1. **Fewer lines, clearer responsibilities.** The current implementation is
   ~6900 lines across parallel visitor and match-based traversals. The target
   is ~1500 lines for the same functional surface.
2. **Static safety for the query protocol.** Illegal chains (e.g. calling
   `.collect()` before a temporal projection is chosen) are rejected at
   type-check time, not runtime.
3. **One-place changes for extension.** Adding a new expression node, plan
   node, or source should touch one file, not nine.
4. **Polars-like surface.** The user-facing API is fluent, immutable,
   method-chained, rooted in object-centric event log semantics.

## Non-goals

- Multiple backends. DuckDB only.
- Query optimization. DuckDB is the optimizer.
- Stable wire format for the IR. Internal, free to change.

## Core architectural decisions

### 1. Three layers

```
Surface        Expression builders + scope-typed Query classes
   ↓
IR             Expression AST + logical plan, both immutable
   ↓
Backend        Validator + compiler, implemented as visitors
```

The user writes only surface. The backend walks only IR. The IR never leaks
into user-facing signatures.

### 2. State machine for the query protocol

The fluent API is a finite state machine. Each state is a Python class that
exposes only the methods valid as the next step.

```
QueryRoot  ── .events / .objects ───────────────▶  MaterializableRowQuery
           ── .object_states ──────▶  ObjectStateSeed  ── .latest / .as_of ──▶  MaterializableRowQuery
           ── .flatten / .event_objects / .object_objects ──▶  RowQuery

MaterializableRowQuery
  ├─ .where / .with_columns / .sort / .limit / .rename  → Self (preserves materializability)
  ├─ .select / .unique                                  → RowQuery (downgrade)
  ├─ .group_by                                          → GroupedQuery
  ├─ .collect / .count / .exists / .to_sql / .scalar / .ids
  └─ .to_ocel / .write                                  (materialization boundary)

RowQuery
  ├─ .where / .with_columns / .sort / .limit / .rename / .select / .unique → Self
  ├─ .group_by                                           → GroupedQuery
  └─ .collect / .count / .exists / .to_sql / .scalar

GroupedQuery
  └─ .agg                                                → AggregatedQuery

AggregatedQuery
  ├─ .where / .sort / .limit / .rename / .select         → Self
  └─ .collect / .count / .exists / .to_sql / .scalar
```

The six states are: `QueryRoot`, `ObjectStateSeed`, `RowQuery`,
`MaterializableRowQuery`, `GroupedQuery`, `AggregatedQuery`. No other state is
reachable.

Why this shape:

- **Temporal projection is forced.** Object-state queries require choosing
  `.latest()` or `.as_of(...)` before anything else can happen. The seed has
  only those two methods; it has no `.where`, no `.collect`, no way to forget.
- **Aggregation is one-way.** After `.agg()` you can't re-group. This matches
  SQL semantics (GROUP BY collapses rows) and keeps the compiler's job
  linear.
- **Materialization is a capability, not a method.** Only scopes that
  correspond to canonical OCEL tables (events, objects, object-states) can
  produce a sublog. Event-occurrences and raw relation tables are join
  products, not entities. The type system enforces this.
- **Identity-breaking ops downgrade.** `.select` and `.unique` on a
  materializable query return a plain `RowQuery`. If the user wants to
  materialize after narrowing columns, they call `.to_ocel()` first and start
  a fresh query tree.

### 3. Expressions are a closed hierarchy; operations are open

The expression AST is a closed union of frozen dataclasses
(`Column`, `Literal`, `BinaryOp`, `Compare`, `Sum`, `Count`, ...). The
operations over that union (compile to SQL, validate, name outputs, check for
aggregates, check for window functions) are an open set, each implemented as
an `ExprVisitor` subclass.

This is the "expression problem" — closed types, open operations — solved by
the visitor pattern with a `generic_visit` default. Adding a new expression
type requires one dataclass. Adding a new operation requires one visitor
subclass. Neither forces changes to the other.

The visitor base:

```python
class ExprVisitor(Generic[T]):
    def visit(self, expr: Expr) -> T:
        method = getattr(self, f"visit_{type(expr).__name__}", None)
        return method(expr) if method is not None else self.generic_visit(expr)

    def generic_visit(self, expr: Expr) -> T:
        for child in expr.children():
            self.visit(child)
        return None  # type: ignore
```

Every AST node implements `children() -> Iterable[Expr]`. A visitor that
doesn't override `visit_NewNodeType` gets free recursion into that node's
children. This replaces the three parallel match-based traversals
(`_contains_aggregate`, `_contains_window`, `output_name`) in the current
code.

### 4. Sources are polymorphic

Each source variant (`EventSource`, `ObjectSource`, `ObjectStateSource`, ...)
is a frozen dataclass implementing an abstract `Source` base with two
methods:

```python
class Source(ABC):
    @abstractmethod
    def scope(self) -> ScopeKind: ...
    @abstractmethod
    def render(self) -> str: ...
```

This kills the central match-on-source-kind extractor functions (`source_kind`,
`selected_types`, `object_state_mode`) in the current code. Adding a new
source type — e.g. `EventPairSource` for event-to-event navigation — is a
single-file change.

### 5. Immutable state

Every method returns a new instance. `@dataclass(frozen=True)` everywhere. No
in-place mutation. The underlying DuckDB connection is the only mutable
resource and it lives on the `OCEL` handle, not inside query states.

## Module layout

```
oceldb/
  core/                    # OCEL handle, manifest (unchanged from v1)
  expr/
    nodes.py               # Expr hierarchy + ExprVisitor base
    builders.py            # col, lit, count, sum_, when, has_event, ...
    relations.py           # relation predicates (has_event, cooccurs_with, ...)
  plan/
    nodes.py               # Plan hierarchy + PlanVisitor base
    sources.py             # Source protocol + variants
  compile/
    expr.py                # ExprCompiler (ExprVisitor → SQL)
    plan.py                # PlanCompiler (PlanVisitor → SQL)
    context.py             # CompileContext for nested scopes
  query/
    scope.py               # ScopeKind + materializability rules
    states.py              # Six FSM classes, methods, transitions
  io/                      # read / write / convert (unchanged)
  inspect/                 # structural summaries (unchanged)
  discovery/               # OC-DFG, lifecycle mining (unchanged)
```

Everything under `expr/`, `plan/`, `compile/`, `query/` is new. The other
packages are untouched.

## Method catalog by state

Methods are listed once on the state where they live. A method with the same
name on multiple states is the same contract, different return type.

### `QueryRoot` (entry)

| Method | Returns | Purpose |
|--------|---------|---------|
| `.events(*types)` | `MaterializableRowQuery` | Event-grained query |
| `.objects(*types)` | `MaterializableRowQuery` | Identity-grained object query |
| `.object_states(*types)` | `ObjectStateSeed` | Forces temporal projection next |
| `.object_changes(*types)` | `RowQuery` | Raw sparse history rows |
| `.flatten(*types)` | `RowQuery` | Event-object incidence for sequences |
| `.event_objects()` | `RowQuery` | Raw E2O edges |
| `.object_objects()` | `RowQuery` | Raw O2O edges |

### `ObjectStateSeed` (temporal gate)

| Method | Returns | Purpose |
|--------|---------|---------|
| `.latest()` | `MaterializableRowQuery` | Forward-fill state per object |
| `.as_of(t)` | `MaterializableRowQuery` | Point-in-time state per object |

Nothing else. No `.where`, no terminal methods.

### `MaterializableRowQuery`

Shape-preserving (returns `Self`, stays materializable):

| Method | Purpose |
|--------|---------|
| `.where(*predicates)` | Boolean filter (AND of args) |
| `.with_columns(*exprs, **named)` | Add computed columns |
| `.sort(*exprs, descending=False)` | ORDER BY |
| `.limit(n)` | LIMIT |
| `.rename(mapping, **named)` | Rename columns (reserved names forbidden) |

Downgrading (returns `RowQuery`):

| Method | Why it downgrades |
|--------|-------------------|
| `.select(*exprs, **named)` | Might drop identity columns |
| `.unique()` | Can collapse distinct identities with same attrs |

Grouping transition:

| Method | Returns |
|--------|---------|
| `.group_by(*exprs)` | `GroupedQuery` |

Materialization-only terminals:

| Method | Purpose |
|--------|---------|
| `.to_ocel()` | Build a new OCEL handle from the query |
| `.write(target, *, overwrite=False)` | Persist as canonical dataset dir |
| `.ids()` | List of `ocel_id` values |

Shared terminals (inherited from `TerminalQuery`): `.collect()`, `.count()`,
`.exists()`, `.to_sql()`, `.scalar()`.

### `RowQuery`

Same method surface as `MaterializableRowQuery` minus the three
materialization terminals. All shape-preserving operations return `Self`;
`.select` and `.unique` return `RowQuery` (no downgrade needed — already
non-materializable).

### `GroupedQuery`

| Method | Returns |
|--------|---------|
| `.agg(*exprs, **named)` | `AggregatedQuery` |

Nothing else. No chaining, no terminals. The user must aggregate.

### `AggregatedQuery`

| Method | Returns | Purpose |
|--------|---------|---------|
| `.where(*predicates)` | `Self` | Compiles to HAVING if predicate references aggregate alias |
| `.sort` / `.limit` / `.rename` / `.select` | `Self` | Post-aggregation shape ops |

No `.group_by` (no re-grouping), no `.with_columns` (would conflict with
aggregation semantics), no `.unique` (every row is already one per group).

Terminals: `.collect()`, `.count()`, `.exists()`, `.to_sql()`, `.scalar()`.

### Shared terminals (on `TerminalQuery`)

| Method | Returns | Purpose |
|--------|---------|---------|
| `.collect()` | `DuckDBPyRelation` | Execute and return DuckDB relation |
| `.count()` | `int` | Wrapped `COUNT(*)` |
| `.exists()` | `bool` | Wrapped `EXISTS(...)` |
| `.to_sql()` | `str` | Compiled SQL without executing |
| `.scalar()` | `Any` | First column of first row |

## Expression surface

### Column references

`col("name")` for any expression. Positional string arguments in `.select`,
`.sort`, `.group_by` are coerced to `col(...)` automatically.

### Literals

`lit(value)` for explicit literals. Comparisons (`col("x") > 5`) auto-lift
Python scalars, so explicit `lit` is usually unnecessary.

### Operators

Python operators on `ScalarExpr` build AST nodes:

- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
- Logical (on `BoolExpr`): `&`, `|`, `~`

`__bool__` raises intentionally — `col("a") and col("b")` is a Python `and`,
not a logical AND. Users must use `&`.

### Methods vs functions

Rule: if an operation has a natural left-hand side, it's a method on `Expr`.
Otherwise it's a module-level function.

Methods: `.alias`, `.is_null`, `.not_null`, `.is_in`, `.cast`, `.str.upper`,
`.dt.year`, etc.

Functions: `lit`, `coalesce`, `when`, `count`, `sum_`, `avg`, `min_`, `max_`,
`has_event`, `cooccurs_with`, etc.

No duplicates. The current code has both `expr.abs()` and `abs_(expr)`; the
new code has only `.abs()`.

### Aggregates

`count()`, `sum_(expr)`, `avg(expr)`, `min_(expr)`, `max_(expr)`. All
subclass `AggregateExpr`, which subclasses `ScalarExpr`. Only legal inside
`.agg(...)`; `.where` raises if it finds one.

### Relation predicates

OCEL-specific. Each returns a builder that terminates in `.exists()`,
`.count()`, `.any(pred)`, or `.all(pred)`:

```python
objects("order").where(has_event("Pay Order").exists())
objects("order").where(cooccurs_with("item").count() >= 3)
events().where(has_object("customer").exists())
objects("order").where(linked("package").outgoing().max_hops(2).exists())
```

Scope legality is enforced at validation time. `has_event`/`cooccurs_with`/
`linked` are object-scoped. `has_object` is event-scoped.

## Implementation choices worth recording

### Expression nodes and methods are merged

Ibis separates "operations" (AST) from "expressions" (user-facing wrappers
with methods). oceldb does not — `Column` is both the AST node and the place
`.is_null()` lives. This saves complexity at the cost of making it harder to
add a second backend later. Given oceldb is DuckDB-only, merged is correct.

### No explicit IR lowering pass

Polars has two plans: `DslPlan` (what users build) and `IR` (what the
optimizer walks). oceldb does not. DuckDB is the optimizer; the single
`Plan` tree goes straight to SQL. If a future version adds backend-agnostic
optimizations, lowering becomes worthwhile.

### Reserved columns are rejected at construction

`ocel_id`, `ocel_type`, `ocel_time` cannot be renamed or shadowed via
`.with_columns` on a materializable query. Rather than downgrading silently,
we raise. The error message points to `.select` as the escape hatch.

### Validation vs compilation

Two visitors, two passes. The validator runs during method calls and at
`collect()` time, checking that column references resolve, scopes are
compatible, and aggregate/window expressions appear only in legal contexts.
The compiler runs only when the user asks for SQL. The separation keeps
error messages close to the user's code.

## Migration

The rewrite is internal. The user-facing API is a strict subset of the
current surface plus the type-level guarantees — no existing valid query
breaks. The invalid queries that now raise at runtime will raise at
type-check time. Methods removed from their old locations (e.g. `abs_` as a
function) will also exist as methods, so migration is typically a
search-and-replace.

Test suite runs unchanged; any test that relied on runtime-only error
messages gets an updated error string.

## Open questions

1. **`AggregatedQuery.with_columns`** — Polars has it; current oceldb
   doesn't. Post-aggregation derived columns over aggregate aliases are
   sometimes useful (`total=agg_sum`, `ratio=col("total")/col("n")`). Decide.
2. **Window functions** — Scope unclear. On `RowQuery` they make sense
   inside `.with_columns` (`row_number().over(...)`). On
   `AggregatedQuery` they arguably do too. Current code only allows them in
   `.with_columns`. Decide.
3. **`scalar()` on non-scalar queries** — Should it fail loudly if more than
   one column or more than one row? Current code silently returns the first
   cell.
4. **Relation `.where` scope** — Filters passed to `has_event("X").where(...)`
   live in the *event* scope, not the outer object scope. The type system
   can't easily express this. Document clearly; consider a
   scope-parameterized builder later.

## References

- Ibis design — separation of operations and expressions: <http://blog.ibis-project.org/design-types-operations/>
- Polars DslPlan / IR split: <https://deepwiki.com/pola-rs/polars/2.3-lazyframe>
- Python `ast.NodeVisitor` — canonical default-recursion visitor:
  <https://docs.python.org/3/library/ast.html#ast.NodeVisitor>
- Martin Fowler on fluent interfaces:
  <https://martinfowler.com/bliki/FluentInterface.html>
- Fowler, "Domain-Specific Languages" (2010), chapter on internal DSL state
  machines.
