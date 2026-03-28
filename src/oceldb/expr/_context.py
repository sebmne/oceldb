"""CompilationContext — schema-aware context for compiling expressions to SQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from oceldb.types import Domain

if TYPE_CHECKING:
    from oceldb.ocel import Ocel


class CompilationContext:
    """Provides schema information needed during expression compilation.

    Built once per :meth:`ViewBuilder.create` call.  Researchers writing
    custom :class:`Expr` subclasses use this to reference tables::

        e2o = ctx.table_ref("event_object")
        unified = ctx.unified_sql(Domain.EVENT)
    """

    def __init__(self, ocel: Ocel) -> None:
        """Build a compilation context by introspecting the OCEL schema.

        Reads the ``*_map_type`` tables and ``DESCRIBE`` output for every
        per-type table to discover available columns up front.

        Args:
            ocel: The :class:`~oceldb.ocel.Ocel` instance whose schema to inspect.
        """
        self._ocel = ocel
        self._prefix = ocel._schema_prefix

        self._type_maps: dict[Domain, dict[str, str]] = {
            Domain.EVENT: {},
            Domain.OBJECT: {},
        }
        self._columns: dict[Domain, set[str]] = {
            Domain.EVENT: set(),
            Domain.OBJECT: set(),
        }
        self._load_schema()

    # -- Public interface ------------------------------------------------------

    def table_ref(self, table: str) -> str:
        """Return a fully qualified reference to a table in the parent schema.

        Args:
            table: Unqualified table name (e.g. ``"event_object"``).

        Returns:
            Schema-qualified name like ``"ocel_db.main.event_object"``.
        """
        return f"{self._prefix}.{table}"

    def unified_sql(self, domain: Domain) -> str:
        """Build a parenthesized subquery that unifies all per-type tables.

        The result is a ``UNION ALL BY NAME`` of every per-type table for
        *domain*, joined with the core table to include the ``ocel_type``
        column. This allows filters to query across types in a single scan.

        Args:
            domain: :attr:`~oceldb.types.Domain.EVENT` or
                :attr:`~oceldb.types.Domain.OBJECT`.

        Returns:
            A parenthesized SQL subquery suitable for use as a table expression.
        """
        type_map = self.type_map(domain)
        parts = [
            f'SELECT * FROM {self._prefix}."{domain.value}_{suffix}"'
            for suffix in type_map.values()
        ]
        union = " UNION ALL BY NAME ".join(parts)
        core = f"{self._prefix}.{domain.value}"
        return (
            f"(SELECT c.ocel_type, u.* FROM ({union}) u JOIN {core} c USING (ocel_id))"
        )

    def available_columns(self, domain: Domain) -> set[str]:
        """Return all column names available for filtering on *domain*.

        Includes columns from every per-type table as well as the core
        columns (``ocel_id``, ``ocel_type``).

        Args:
            domain: The entity domain to inspect.

        Returns:
            A set of column name strings.
        """
        return self._columns[domain]

    def validate_column(self, domain: Domain, column: str) -> None:
        """Validate that *column* exists in *domain*'s schema.

        Args:
            domain: The entity domain.
            column: The column name to check.

        Raises:
            ValueError: If *column* is not found, with a message listing the
                available columns.
        """
        available = self.available_columns(domain)
        if column not in available:
            raise ValueError(
                f"No {domain.value} type has attribute {column!r}. "
                f"Available: {sorted(available - {'ocel_id'})}"
            )

    def type_map(self, domain: Domain) -> dict[str, str]:
        """Return the mapping of OCEL type names to per-type table suffixes.

        Args:
            domain: The entity domain.

        Returns:
            A dict like ``{"Create Order": "Create Order"}`` where values
            are the suffixes used in table names (``event_Create Order``).
        """
        return self._type_maps[domain]

    # -- Internals -------------------------------------------------------------

    def _load_schema(self) -> None:
        for domain in Domain:
            rows = self._ocel.sql(
                f"SELECT ocel_type, ocel_type_map FROM {domain.value}_map_type"
            ).fetchall()
            for ocel_type, suffix in rows:
                self._type_maps[domain][ocel_type] = suffix
                table = f"{domain.value}_{suffix}"
                cols = self._ocel.sql(f'DESCRIBE "{table}"').fetchall()
                for col_name, *_ in cols:
                    self._columns[domain].add(col_name)
            # Core columns are always available
            self._columns[domain].update({"ocel_id", "ocel_type"})
