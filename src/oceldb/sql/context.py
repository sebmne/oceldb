from dataclasses import dataclass
from typing import Literal

ExprScopeKind = Literal["event", "object", "event_object", "object_object"]


@dataclass(frozen=True)
class CompileContext:
    alias: str
    schema: str
    kind: ExprScopeKind

    def table(self, name: str) -> str:
        return f"{self.schema}.{name}"

    def with_alias(
        self,
        alias: str,
        *,
        kind: ExprScopeKind | None = None,
    ) -> "CompileContext":
        return CompileContext(
            alias=alias,
            schema=self.schema,
            kind=self.kind if kind is None else kind,
        )
