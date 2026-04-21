"""Compilation context threaded through expression and plan rendering.

A ``CompileContext`` captures the transient information needed to emit SQL for
a single scope: the current alias, the scope kind, and the ambient metadata
required to render manifest-dependent sources (object state columns, event
aliases for lateral joins).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime

from oceldb.plan.scope import ScopeKind


@dataclass(frozen=True)
class CompileContext:
    alias: str
    kind: ScopeKind
    object_change_columns: tuple[str, ...] = ()
    object_state_mode: tuple[str, date | datetime | None] | None = None
    event_alias: str | None = None

    def with_alias(
        self,
        alias: str,
        *,
        kind: ScopeKind | None = None,
        event_alias: str | None = None,
    ) -> "CompileContext":
        return replace(
            self,
            alias=alias,
            kind=kind if kind is not None else self.kind,
            event_alias=event_alias if event_alias is not None else self.event_alias,
        )

    def table(self, name: str) -> str:
        return quote_ident(name)


def quote_ident(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


__all__ = ["CompileContext", "quote_ident"]
