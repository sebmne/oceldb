"""Decorator that makes filter functions usable both directly and as pipe steps."""

import functools


def _step(fn):
    """Wrap *fn* so it can be called with or without an OCEL as the first arg.

    When called with an ``OCEL`` as the first positional argument the function
    runs immediately. When called without one it returns a partial that can be
    passed to ``OCEL.__or__``.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        from oceldb.ocel import OCEL

        if args and isinstance(args[0], OCEL):
            return fn(*args, **kwargs)

        def step(ocel):
            return fn(ocel, *args, **kwargs)

        return step

    return wrapper
