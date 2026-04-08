from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ScopeKind = Literal["event", "object"]


@dataclass(frozen=True)
class CompileContext:
    """
    Compilation context for rendering expressions and queries into SQL.

    Attributes:
        alias: SQL alias of the current root scope.
        schema: Active OCEL schema.
        kind: Current scope kind.
    """

    alias: str
    schema: str
    kind: ScopeKind

    def table(self, name: str) -> str:
        """
        Return the fully-qualified table name in the current schema.
        """
        return f"{self.schema}.{name}"

    def with_alias(
        self, alias: str, *, kind: ScopeKind | None = None
    ) -> "CompileContext":
        """
        Return a child context with a different SQL alias.
        """
        return CompileContext(
            alias=alias,
            schema=self.schema,
            kind=self.kind if kind is None else kind,
        )
