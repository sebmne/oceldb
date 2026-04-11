from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional


def python_type_to_sql_type(tp: Optional[type[Any]]) -> Optional[str]:
    if tp is None:
        return None

    mapping: dict[type[Any], str] = {
        int: "BIGINT",
        float: "DOUBLE",
        str: "VARCHAR",
        bool: "BOOLEAN",
        datetime: "TIMESTAMP",
        date: "DATE",
        Decimal: "DOUBLE",
    }

    if tp in mapping:
        return mapping[tp]

    raise TypeError(
        f"Unsupported cast type {tp!r}. "
        "Use a Python type like int/float/datetime or a raw SQL type string."
    )
