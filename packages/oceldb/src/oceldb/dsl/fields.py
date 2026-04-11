from typing import Any, Optional

from oceldb.ast.field import FieldExpr
from oceldb.dsl._utils import python_type_to_sql_type


def field(name: str, cast: Optional[type[Any]] = None) -> FieldExpr:
    """
    Build an expression for a fixed OCEL field in the current scope.
    """
    return FieldExpr(name=name, cast=python_type_to_sql_type(cast))


def id_() -> FieldExpr:
    """
    Shorthand for the OCEL id field.
    """
    return field("ocel_id")


def type_() -> FieldExpr:
    """
    Shorthand for the OCEL type field.
    """
    return field("ocel_type")


def time_() -> FieldExpr:
    """
    Shorthand for the OCEL time field.
    """
    return field("ocel_time")


def changed_field() -> FieldExpr:
    """
    Shorthand for the object changed-field column.
    """
    return field("ocel_changed_field")


def event_id() -> FieldExpr:
    """
    Shorthand for the event id column in event-object relations.
    """
    return field("ocel_event_id")


def object_id() -> FieldExpr:
    """
    Shorthand for the object id column in event-object relations.
    """
    return field("ocel_object_id")


def source_id() -> FieldExpr:
    """
    Shorthand for the object source id column in object-object relations.
    """
    return field("ocel_source_id")


def target_id() -> FieldExpr:
    """
    Shorthand for the object target id column in object-object relations.
    """
    return field("ocel_target_id")
