"""Relation-predicate subquery rendering.

``has_event`` / ``has_object`` / ``cooccurs_with`` / ``linked`` all expand to
correlated subqueries against the current scope's ``ocel_id``. The expression
visitor asks this module for the candidate ``CompileContext`` first, renders
the optional predicate against it, and then hands the rendered string back
here so the subquery can be assembled.
"""

from __future__ import annotations

from oceldb.compile.context import CompileContext
from oceldb.compile.sources import render_scope_source
from oceldb.plan.scope import ScopeKind
from oceldb.expr.nodes import (
    LinkedDirection,
    RelationKind,
    RelationTarget,
)


def candidate_ctx_for(target: RelationTarget, ctx: CompileContext) -> CompileContext:
    """Return the ``CompileContext`` in which the relation predicate is rendered."""
    candidate_kind = relation_candidate_kind(target.kind, ctx.kind)
    candidate_alias = relation_candidate_alias(target.kind)
    candidate_event_alias = ctx.alias if target.kind == "has_object" else ctx.event_alias
    return ctx.with_alias(
        candidate_alias,
        kind=candidate_kind,
        event_alias=candidate_event_alias,
    )


def render_relation_subquery(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    *,
    select_sql: str,
    candidate_predicate_sql: str | None,
    negate_candidate_predicate: bool = False,
) -> str:
    extra: list[str] = []
    if candidate_predicate_sql is not None:
        fragment = (
            f"NOT ({candidate_predicate_sql})"
            if negate_candidate_predicate
            else candidate_predicate_sql
        )
        extra.append(fragment)

    if target.kind == "cooccurs_with":
        _require_object_scope(ctx.kind, "cooccurs_with")
        return _render_cooccurs_with(target, ctx, candidate_ctx, select_sql, extra)
    if target.kind == "linked":
        _require_object_scope(ctx.kind, "linked")
        return _render_linked(target, ctx, candidate_ctx, select_sql, extra)
    if target.kind == "has_event":
        _require_object_scope(ctx.kind, "has_event")
        return _render_has_event(target, ctx, candidate_ctx, select_sql, extra)
    if target.kind == "has_object":
        if ctx.kind != "event":
            raise ValueError("has_object(...) is only valid in event-rooted scope")
        return _render_has_object(target, ctx, candidate_ctx, select_sql, extra)
    raise TypeError(f"Unsupported relation kind: {target.kind!r}")


def _require_object_scope(kind: str, relation_name: str) -> None:
    if kind not in {"object", "object_state"}:
        raise ValueError(f"{relation_name}(...) is only valid in object-rooted scope")


def _render_cooccurs_with(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    predicates: list[str] = [
        f'eo1."ocel_object_id" = {ctx.alias}."ocel_id"',
        'eo1."ocel_event_id" = eo2."ocel_event_id"',
        f'eo2."ocel_object_id" = {candidate_ctx.alias}."ocel_id"',
        f'{candidate_ctx.alias}."ocel_type" = {_render_string_literal(target.type_name)}',
    ]
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo1
        JOIN {ctx.table("event_object")} eo2
          ON eo1."ocel_event_id" = eo2."ocel_event_id"
        JOIN {render_scope_source(candidate_ctx)}
          ON eo2."ocel_object_id" = {candidate_ctx.alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_linked(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    predicates: list[str] = [
        f'{candidate_ctx.alias}."ocel_type" = {_render_string_literal(target.type_name)}',
    ]
    predicates.extend(extra_predicates)

    if target.hop_limit is None:
        return _render_linked_unbounded(
            target, ctx, candidate_ctx, select_sql, predicates
        )

    anchor_node = f'{ctx.alias}."ocel_id"'
    anchor_next = _linked_neighbor_sql(target.direction, anchor_node)
    anchor_match = _linked_match_sql(target.direction, anchor_node)

    recursive_node = 'lp."ocel_id"'
    recursive_next = _linked_neighbor_sql(target.direction, recursive_node)
    recursive_match = _linked_match_sql(target.direction, recursive_node)
    hop_limit_sql = f'AND lp."depth" < {target.hop_limit}'

    return f"""
        WITH RECURSIVE linked_paths("ocel_id", "depth", "path") AS (
            SELECT
                {anchor_next} AS "ocel_id",
                1 AS "depth",
                ',' || {anchor_node} || ',' || {anchor_next} || ',' AS "path"
            FROM {ctx.table("object_object")} oo
            WHERE {anchor_match}
              AND {anchor_next} <> {anchor_node}

            UNION ALL

            SELECT
                {recursive_next} AS "ocel_id",
                lp."depth" + 1 AS "depth",
                lp."path" || {recursive_next} || ',' AS "path"
            FROM linked_paths lp
            JOIN {ctx.table("object_object")} oo
              ON {recursive_match}
            WHERE POSITION(',' || {recursive_next} || ',' IN lp."path") = 0
              {hop_limit_sql}
        )
        SELECT {select_sql}
        FROM (
            SELECT DISTINCT lp."ocel_id"
            FROM linked_paths lp
        ) linked_ids
        JOIN {render_scope_source(candidate_ctx)}
          ON linked_ids."ocel_id" = {candidate_ctx.alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_linked_unbounded(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    select_sql: str,
    predicates: list[str],
) -> str:
    anchor_node = f'{ctx.alias}."ocel_id"'
    linked_edges_sql = _linked_edges_sql(target.direction, ctx.table("object_object"))

    return f"""
        WITH RECURSIVE
        linked_edges("source_id", "target_id") AS (
            {linked_edges_sql}
        ),
        linked_nodes("ocel_id") AS (
            SELECT le."target_id"
            FROM linked_edges le
            WHERE le."source_id" = {anchor_node}
              AND le."target_id" <> {anchor_node}

            UNION

            SELECT le."target_id"
            FROM linked_nodes ln
            JOIN linked_edges le
              ON le."source_id" = ln."ocel_id"
            WHERE le."target_id" <> {anchor_node}
        )
        SELECT {select_sql}
        FROM linked_nodes linked_ids
        JOIN {render_scope_source(candidate_ctx)}
          ON linked_ids."ocel_id" = {candidate_ctx.alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_has_event(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    predicates: list[str] = [
        f'eo."ocel_object_id" = {ctx.alias}."ocel_id"',
        f'{candidate_ctx.alias}."ocel_type" = {_render_string_literal(target.type_name)}',
    ]
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo
        JOIN {ctx.table("event")} {candidate_ctx.alias}
          ON eo."ocel_event_id" = {candidate_ctx.alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def _render_has_object(
    target: RelationTarget,
    ctx: CompileContext,
    candidate_ctx: CompileContext,
    select_sql: str,
    extra_predicates: list[str],
) -> str:
    predicates: list[str] = [
        f'eo."ocel_event_id" = {ctx.alias}."ocel_id"',
        f'{candidate_ctx.alias}."ocel_type" = {_render_string_literal(target.type_name)}',
    ]
    predicates.extend(extra_predicates)

    return f"""
        SELECT {select_sql}
        FROM {ctx.table("event_object")} eo
        JOIN {render_scope_source(candidate_ctx)}
          ON eo."ocel_object_id" = {candidate_ctx.alias}."ocel_id"
        WHERE {" AND ".join(predicates)}
    """


def relation_candidate_kind(kind: RelationKind, current_kind: ScopeKind) -> ScopeKind:
    if kind in {"cooccurs_with", "linked"}:
        return "object_state" if current_kind == "object_state" else "object"
    if kind == "has_event":
        return "event"
    if kind == "has_object":
        return "object_state_at_event"
    raise TypeError(f"Unsupported relation kind: {kind!r}")


def relation_candidate_alias(kind: RelationKind) -> str:
    if kind == "cooccurs_with":
        return "ro"
    if kind == "linked":
        return "lo"
    if kind == "has_event":
        return "he"
    if kind == "has_object":
        return "ho"
    raise TypeError(f"Unsupported relation kind: {kind!r}")


def _linked_match_sql(direction: LinkedDirection, node_sql: str) -> str:
    if direction == "any":
        return f'(oo."ocel_source_id" = {node_sql} OR oo."ocel_target_id" = {node_sql})'
    if direction == "outgoing":
        return f'oo."ocel_source_id" = {node_sql}'
    if direction == "incoming":
        return f'oo."ocel_target_id" = {node_sql}'
    raise TypeError(f"Unsupported linked direction: {direction!r}")


def _linked_neighbor_sql(direction: LinkedDirection, node_sql: str) -> str:
    if direction == "any":
        return (
            f'CASE WHEN oo."ocel_source_id" = {node_sql} '
            f'THEN oo."ocel_target_id" ELSE oo."ocel_source_id" END'
        )
    if direction == "outgoing":
        return 'oo."ocel_target_id"'
    if direction == "incoming":
        return 'oo."ocel_source_id"'
    raise TypeError(f"Unsupported linked direction: {direction!r}")


def _linked_edges_sql(direction: LinkedDirection, object_object_sql: str) -> str:
    if direction == "any":
        return f"""
            SELECT
                oo."ocel_source_id" AS "source_id",
                oo."ocel_target_id" AS "target_id"
            FROM {object_object_sql} oo
            UNION
            SELECT
                oo."ocel_target_id" AS "source_id",
                oo."ocel_source_id" AS "target_id"
            FROM {object_object_sql} oo
        """
    if direction == "outgoing":
        return f"""
            SELECT
                oo."ocel_source_id" AS "source_id",
                oo."ocel_target_id" AS "target_id"
            FROM {object_object_sql} oo
        """
    if direction == "incoming":
        return f"""
            SELECT
                oo."ocel_target_id" AS "source_id",
                oo."ocel_source_id" AS "target_id"
            FROM {object_object_sql} oo
        """
    raise TypeError(f"Unsupported linked direction: {direction!r}")


def _render_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


__all__ = [
    "candidate_ctx_for",
    "relation_candidate_alias",
    "relation_candidate_kind",
    "render_relation_subquery",
]
