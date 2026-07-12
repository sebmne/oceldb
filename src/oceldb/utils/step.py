"""Decorator that makes filter functions usable both directly and as pipe steps."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    overload,
)

if TYPE_CHECKING:
    from oceldb.ocel import OCEL

_P = ParamSpec("_P")
_R = TypeVar("_R")
_R_co = TypeVar("_R_co", covariant=True)


class StepFunction(Protocol[_P, _R_co]):
    """A function callable directly or curried into an OCEL pipeline step."""

    @overload
    def __call__(self, ocel: OCEL, /, *args: _P.args, **kwargs: _P.kwargs) -> _R_co: ...

    @overload
    def __call__(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> Callable[[OCEL], _R_co]: ...


def step(
    fn: Callable[Concatenate[OCEL, _P], _R],
) -> StepFunction[_P, _R]:
    """Wrap *fn* so it can be called with or without an OCEL as the first arg.

    When called with an ``OCEL`` as the first positional argument the function
    runs immediately. When called without one it returns a partial that can be
    applied through the ``ocel >> step`` pipe operator (``OCEL.__rshift__``).

    The returned protocol preserves both call styles without requiring local
    overload declarations on every decorated function.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> "_R | Callable[[OCEL], _R]":
        from oceldb.ocel import OCEL

        if args and isinstance(args[0], OCEL):
            return fn(*args, **kwargs)  # pyright: ignore[reportCallIssue]

        def step(ocel: "OCEL") -> _R:
            return fn(ocel, *args, **kwargs)  # pyright: ignore[reportCallIssue]

        return step

    return cast("StepFunction[_P, _R]", wrapper)
