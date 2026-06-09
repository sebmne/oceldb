"""Type labels used in persisted metadata."""

_TYPE_ALIASES = {
    "bool": "bool",
    "boolean": "bool",
    "double": "float",
    "float": "float",
    "int": "int",
    "integer": "int",
    "real": "float",
    "str": "string",
    "string": "string",
    "text": "string",
    "timestamp": "datetime",
    "time": "datetime",
    "datetime": "datetime",
    "varchar": "string",
}


def manifest_type(type_name: str) -> str:
    """Return the normalized public manifest label for *type_name*."""
    normalized = type_name.strip().lower()
    try:
        return _TYPE_ALIASES[normalized]
    except KeyError as exc:
        raise ValueError(f"Unsupported attribute type {type_name!r}.") from exc


def manifest_attributes(attributes: dict[str, str]) -> dict[str, str]:
    """Normalize an attribute schema for manifest storage/display."""
    return {name: manifest_type(type_name) for name, type_name in attributes.items()}
