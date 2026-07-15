"""Per-type attribute directories, declared at IO boundaries or observed.

A :class:`TypeDirectory` answers "which attribute columns belong to which
event or object type". It is seeded from declared exchange or manifest
schemas when a log is opened or read, and probed from observed non-null
values otherwise. Transformations never update a directory; they drop it and
the next access recomputes the truth from the data.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

import polars as pl

from oceldb import schema as s
from oceldb.schema import AttributeType, OCELSchema

EVENT_FIXED = frozenset({s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE})
CHANGE_FIXED = frozenset({s.OCEL_ID, s.OCEL_TIME, s.OCEL_TYPE, s.OCEL_CHANGED_FIELD})

TypeColumns = Mapping[str, Mapping[str, pl.DataType]]


@dataclass(frozen=True)
class TypeDirectory:
    """Attribute columns and dtypes per event and object type."""

    event_types: TypeColumns
    object_types: TypeColumns

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_types", _freeze(self.event_types))
        object.__setattr__(self, "object_types", _freeze(self.object_types))

    @classmethod
    def from_schema(cls, schema: OCELSchema) -> "TypeDirectory":
        """Seed a directory from declared IO attribute types."""
        return cls(
            event_types=_declared_columns(schema.event_types),
            object_types=_declared_columns(schema.object_types),
        )

    @classmethod
    def probe(
        cls,
        *,
        events: pl.LazyFrame,
        objects: pl.LazyFrame,
        object_changes: pl.LazyFrame,
    ) -> "TypeDirectory":
        """Observe populated attributes per type in one collection pass.

        Event attributes come from the events table, object attributes from
        the sparse change rows. Object types without changes are still listed
        through their identity rows.
        """
        event_query, event_attrs, event_schema = _presence_query(events, EVENT_FIXED)
        change_query, change_attrs, change_schema = _presence_query(
            object_changes, CHANGE_FIXED
        )
        identity_query = objects.select(s.OCEL_TYPE).unique()
        event_df, change_df, identity_df = pl.collect_all(
            [event_query, change_query, identity_query]
        )
        object_types = _observed_columns(change_df, change_attrs, change_schema)
        for value in identity_df.get_column(s.OCEL_TYPE).drop_nulls().to_list():
            object_types.setdefault(str(value), {})
        return cls(
            event_types=_observed_columns(event_df, event_attrs, event_schema),
            object_types=object_types,
        )

    def to_schema(self) -> OCELSchema:
        """Express the directory as declared IO attribute types."""
        return OCELSchema(
            event_types=_as_attribute_types(self.event_types),
            object_types=_as_attribute_types(self.object_types),
        )

    def event_attributes(self, types: Sequence[str]) -> list[str]:
        """Return the stable attribute union for the selected event types."""
        return _attribute_union(self.event_types, types)

    def object_attributes(self, types: Sequence[str]) -> list[str]:
        """Return the stable attribute union for the selected object types."""
        return _attribute_union(self.object_types, types)


def _presence_query(
    frame: pl.LazyFrame, fixed: frozenset[str]
) -> tuple[pl.LazyFrame, list[str], pl.Schema]:
    """Build the one-row-per-type non-null presence aggregation."""
    schema = frame.collect_schema()
    attributes = [name for name in schema.names() if name not in fixed]
    query = frame.group_by(s.OCEL_TYPE).agg(
        pl.col(name).is_not_null().any() for name in attributes
    )
    return query, attributes, schema


def _observed_columns(
    presence: pl.DataFrame, attributes: list[str], schema: pl.Schema
) -> dict[str, dict[str, pl.DataType]]:
    result: dict[str, dict[str, pl.DataType]] = {}
    for row in presence.iter_rows(named=True):
        type_name = row[s.OCEL_TYPE]
        if type_name is None:
            continue
        result[str(type_name)] = {
            name: schema[name] for name in attributes if row[name]
        }
    return result


def _declared_columns(
    types: Mapping[str, Mapping[str, AttributeType]],
) -> dict[str, dict[str, pl.DataType]]:
    return {
        type_name: {
            name: attr_type.polars_dtype() for name, attr_type in attributes.items()
        }
        for type_name, attributes in types.items()
    }


def _as_attribute_types(
    types: TypeColumns,
) -> dict[str, dict[str, AttributeType]]:
    return {
        type_name: {
            name: AttributeType.from_polars(dtype) for name, dtype in attributes.items()
        }
        for type_name, attributes in types.items()
    }


def _attribute_union(types: TypeColumns, selected: Sequence[str]) -> list[str]:
    return list(
        dict.fromkeys(
            attribute
            for type_name in selected
            for attribute in types.get(type_name, {})
        )
    )


def _freeze(types: TypeColumns) -> TypeColumns:
    return MappingProxyType(
        {
            str(type_name): MappingProxyType(dict(attributes))
            for type_name, attributes in types.items()
        }
    )
