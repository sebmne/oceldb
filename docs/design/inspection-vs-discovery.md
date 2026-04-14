# Inspection vs Discovery

## Status

This document defines the intended boundary between the `oceldb.inspect` and
`oceldb.discovery` modules.

It is a placement rule for future features, not just a description of the
current implementation.

## Goal

`oceldb` should not grow a vague "analysis" namespace.

Users should be able to predict where a feature belongs:

- `oceldb.inspect` answers "what is in this log?"
- `oceldb.discovery` answers "what structure can we derive from this log?"

That distinction keeps the library easier to learn and prevents a single module
from accumulating unrelated helpers.

## High-Level Rule

Put a function in `inspect` when it exposes direct structural facts about the
current dataset.

Put a function in `discovery` when it computes a mined, modeled, or
higher-order analytical artifact from the dataset.

## Inspection

Inspection is for direct descriptive access to the stored OCEL.

Typical properties of inspection functions:

- they describe the current dataset as it exists
- they stay close to canonical tables and manifest information
- they do not introduce a process model or graph model
- they do not depend on user-chosen mining semantics
- they are mainly about inventory, profile, and structural overview

Typical examples:

- `overview(ocel)`
- `event_types(ocel)`
- `object_types(ocel)`
- `event_type_counts(ocel)`
- `object_type_counts(ocel)`
- `event_attributes(ocel, ...)`
- `object_attributes(ocel, ...)`
- `attributes(ocel)`
- `event_object_stats(ocel)`

`event_object_stats` belongs to inspection even though it is aggregated,
because it is still a direct profile of the canonical relation tables rather
than a mined process artifact.

## Discovery

Discovery is for derived analytical structures.

Typical properties of discovery functions:

- they build a higher-level artifact from the log
- they introduce modeling choices
- they may depend on traversal, projection, or mining semantics
- the result is usually a graph, model, pattern set, or summarized behavioral
  structure

Typical examples:

- `ocdfg(ocel, ...)`
- `object_lifecycle(ocel, object_type=..., field=...)`

Likely future examples:

- projected directly-follows graphs
- lifecycle summaries with model-level semantics
- bottleneck discovery
- pattern mining helpers
- conformance-oriented discovered reference structures

See [`object-lifecycle.md`](./object-lifecycle.md) for the lifecycle design.

## Practical Placement Test

When adding a new function, answer these questions in order:

1. Is the result mostly a direct fact about tables, attributes, counts, or the
   overall profile of the current log?
   Then it belongs in `inspect`.
2. Is the result a derived behavioral structure, graph, model, or mined
   abstraction?
   Then it belongs in `discovery`.
3. Does the function require choosing semantics such as path traversal,
   aggregation strategy, behavioral adjacency, or mining configuration?
   Bias toward `discovery`.

If the answer is still unclear, use this tie-breaker:

- if users would naturally verify the result by mentally looking at the raw
  stored tables, it is probably `inspect`
- if users would naturally treat the result as an interpretation of the log, it
  is probably `discovery`

## Relation to the Query DSL

`ocel.query` remains the compositional analysis surface.

The intended layering is:

- `OCEL` is the stable dataset handle
- `ocel.query` is the lazy row-oriented DSL
- `oceldb.inspect` is a pure-function convenience layer for direct log facts
- `oceldb.discovery` is a pure-function convenience layer for mined artifacts

Inspection and discovery functions should accept `OCEL` explicitly:

```python
from oceldb.discovery import ocdfg
from oceldb.inspect import overview

overview(ocel)
ocdfg(ocel, "order")
```

This keeps `OCEL` thin and prevents namespace growth on the core handle.

## Naming Guidance

Prefer plain nouns or verb phrases that describe the resulting artifact:

- `overview`
- `event_types`
- `object_attributes`
- `ocdfg`

Avoid names that hide the distinction behind a generic "analyze" or
"inspect/discover everything" facade.

## Non-Goals

This split does not mean:

- `inspect` must avoid all aggregation
- `discovery` must be complex
- every future analytical helper needs a new top-level module

The distinction is semantic, not based on implementation complexity.

## Design Consequence

Whenever a new high-level helper is proposed, the first design question should
be:

"Is this exposing a direct fact of the stored log, or is it discovering a
derived analytical artifact?"

That answer determines whether the feature belongs in `oceldb.inspect` or
`oceldb.discovery`.
