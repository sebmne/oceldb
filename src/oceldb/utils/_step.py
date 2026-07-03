"""Decorator that makes filter functions usable both directly and as pipe steps."""

import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from oceldb.ocel import OCEL

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _step(fn: Callable[_P, _R]) -> Callable[..., "_R | Callable[[OCEL], _R]"]:
    """Wrap *fn* so it can be called with or without an OCEL as the first arg.

    When called with an ``OCEL`` as the first positional argument the function
    runs immediately. When called without one it returns a partial that can be
    applied through the ``ocel >> step`` pipe operator (``OCEL.__rshift__``).

    Call-site types come from the ``@overload`` declarations on each decorated
    function; this wrapper only supplies the runtime dispatch.
    """

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> "_R | Callable[[OCEL], _R]":
        from oceldb.ocel import OCEL

        if args and isinstance(args[0], OCEL):
            return fn(*args, **kwargs)  # pyright: ignore[reportCallIssue]

        def step(ocel: "OCEL") -> _R:
            return fn(ocel, *args, **kwargs)  # pyright: ignore[reportCallIssue]

        return step

    return wrapper
