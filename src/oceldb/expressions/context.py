from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal

ScopeKind = Literal["object", "event"]


@dataclass(frozen=True)
class Context:
    """
    Immutable compilation context for expression-to-SQL compilation.

    A Context describes the *current* compilation scope:
        - `alias`: the SQL alias of the current row space
        - `schema`: the DuckDB schema backing the current OCEL
        - `kind`: whether the current row space is object- or event-shaped

    Contexts are derived immutably via `.child(...)`, similar in spirit to Go's
    context style, but specialized for SQL compilation rather than runtime
    control flow.
    """

    alias: str
    schema: str
    kind: ScopeKind
    _alias_index: int = 0

    def table(self, name: str) -> str:
        """
        Return the fully qualified table name inside the current schema.
        """
        return f"{self.schema}.{name}"

    def child(
        self,
        prefix: str,
        *,
        kind: ScopeKind | None = None,
    ) -> "Context":
        """
        Derive a child compilation context with a fresh SQL alias.

        Args:
            prefix: Prefix for the generated alias, e.g. "oi", "e", "oo".
            kind: Optional new scope kind. If omitted, the current kind is kept.

        Returns:
            A new Context with:
                - a fresh alias like "oi1", "e2", ...
                - the same schema
                - the given or inherited kind
                - an incremented alias counter

        Example:
            ctx = Context(alias="o", schema="ocel_123", kind="object")
            child = ctx.child("oi", kind="object")
            # child.alias == "oi1"
        """
        next_index = self._alias_index + 1
        return Context(
            alias=f"{prefix}{next_index}",
            schema=self.schema,
            kind=self.kind if kind is None else kind,
            _alias_index=next_index,
        )

    def with_alias(self, alias: str) -> "Context":
        """
        Return a copy with a manually chosen alias.

        Use this sparingly. In most cases, `.child(...)` is preferable because it
        guarantees fresh aliases.
        """
        return replace(self, alias=alias)

    def with_kind(self, kind: ScopeKind) -> "Context":
        """
        Return a copy with a different scope kind.
        """
        return replace(self, kind=kind)

    def require_kind(self, expected: ScopeKind) -> None:
        """
        Validate that this context has the expected scope kind.

        Raises:
            TypeError: if the current context kind does not match.
        """
        if self.kind != expected:
            raise TypeError(
                f"Expression requires a {expected!r} context, got {self.kind!r}."
            )

    @classmethod
    def for_objects(cls, schema: str, alias: str = "o") -> "Context":
        """
        Create a root object-scoped context.
        """
        return cls(alias=alias, schema=schema, kind="object")

    @classmethod
    def for_events(cls, schema: str, alias: str = "e") -> "Context":
        """
        Create a root event-scoped context.
        """
        return cls(alias=alias, schema=schema, kind="event")
