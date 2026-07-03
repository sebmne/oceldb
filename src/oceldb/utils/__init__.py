"""Internal utilities."""

from collections.abc import Iterable


def to_list(value: str | Iterable[str]) -> list[str]:
    """Normalize a single name or an iterable of names to a list.

    A bare string is wrapped in a one-element list rather than being split into
    characters; any other iterable is materialized with :class:`list`.
    """
    return [value] if isinstance(value, str) else list(value)


__all__ = ["to_list"]
