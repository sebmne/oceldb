"""Plan → SQL compilation."""

from oceldb.compile.context import CompileContext, quote_ident
from oceldb.compile.expr import render_compare_value, render_expr, render_order_expr
from oceldb.compile.plan import compile_query
from oceldb.compile.schema import (
    NodeAnalysis,
    analyze_node,
    derive_output_columns,
    output_name,
    query_output_columns,
)
from oceldb.compile.sources import (
    render_object_change_batches_source,
    render_object_state_source,
    render_object_state_source_at_event,
    render_scope_source,
    render_source,
)
from oceldb.compile.validate import (
    ValidationVisitor,
    contains_aggregate,
    contains_window,
    validate_expr,
    validate_query,
    validate_sort_expr,
)

__all__ = [
    "CompileContext",
    "NodeAnalysis",
    "ValidationVisitor",
    "analyze_node",
    "compile_query",
    "contains_aggregate",
    "contains_window",
    "derive_output_columns",
    "output_name",
    "query_output_columns",
    "quote_ident",
    "render_compare_value",
    "render_expr",
    "render_object_change_batches_source",
    "render_object_state_source",
    "render_object_state_source_at_event",
    "render_order_expr",
    "render_scope_source",
    "render_source",
    "validate_expr",
    "validate_query",
    "validate_sort_expr",
]
