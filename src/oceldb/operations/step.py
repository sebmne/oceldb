"""The ``@step`` decorator giving operations their dual direct-call/``>>`` interface."""

import functools
from collections.abc import Callable
from typing import (
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    overload,
)

from oceldb.ocel import OCEL

_P = ParamSpec("_P")
_R = TypeVar("_R")
_R_co = TypeVar("_R_co", covariant=True)


class StepFunction(Protocol[_P, _R_co]):
    """A function callable directly or curried into an OCEL pipeline step."""

    @overload
    def __call__(self, ocel: OCEL, *args: _P.args, **kwargs: _P.kwargs) -> _R_co: ...

    @overload
    def __call__(
        self, *args: _P.args, **kwargs: _P.kwargs
    ) -> Callable[[OCEL], _R_co]: ...


def step(
    fn: Callable[Concatenate[OCEL, _P], _R],
) -> StepFunction[_P, _R]:
    """Allow *fn* to run directly or produce an ``OCEL.__rshift__`` step."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> "_R | Callable[[OCEL], _R]":
        if args and isinstance(args[0], OCEL):
            if "ocel" in kwargs:
                raise TypeError(f"{fn.__name__}() got multiple values for 'ocel'")
            return fn(*args, **kwargs)  # pyright: ignore[reportCallIssue]
        if "ocel" in kwargs:
            source = kwargs.pop("ocel")
            if not isinstance(source, OCEL):
                raise TypeError("ocel must be an OCEL.")
            return fn(source, *args, **kwargs)  # pyright: ignore[reportCallIssue]

        def apply(ocel: "OCEL") -> _R:
            return fn(ocel, *args, **kwargs)  # pyright: ignore[reportCallIssue]

        return apply

    return cast("StepFunction[_P, _R]", wrapper)
