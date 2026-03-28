"""ViewBuilder — compile filter expressions into a materialized OCEL view.

The view materialization pipeline:

1. Separate conditions by :class:`~oceldb.types.Domain` (event vs. object).
2. Compile each group into a SQL ``WHERE`` clause.
3. Compute surviving event and object IDs.
4. Cross-propagate through the event-to-object (E2O) table so that
   event filters prune unreferenced objects and vice versa.
5. Create a DuckDB schema with filtered ``VIEW`` definitions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from oceldb.expr._context import CompilationContext
from oceldb.expr._expr import Expr
from oceldb.types import Domain

if TYPE_CHECKING:
    from oceldb.ocel import Ocel


class ViewBuilder:
    """Fluent builder for filtered OCEL views.

    Chain ``.where()`` calls to add conditions, then call ``.create()``::

        view = (ocel.view()
            .where(event.type == "Create Order")
            .where(event.total_price > 100)
            .create())
    """

    __slots__ = ("_ocel", "_conditions")

    def __init__(self, ocel: Ocel, conditions: tuple[Expr, ...] = ()) -> None:
        """Initialize a view builder.

        Args:
            ocel: The source log to filter.
            conditions: Pre-existing conditions (used internally by
                :meth:`where` to accumulate filters immutably).
        """
        self._ocel = ocel
        self._conditions = conditions

    def where(self, *conditions: Expr) -> ViewBuilder:
        """Add one or more filter conditions (AND semantics).

        Returns a new :class:`ViewBuilder` — the original is unchanged,
        so calls can be chained::

            builder = ocel.view().where(expr_a).where(expr_b)

        Args:
            *conditions: One or more :class:`~oceldb.expr._expr.Expr` instances.
                Multiple arguments within a single call are AND-joined.

        Returns:
            A new :class:`ViewBuilder` with the added conditions.
        """
        return ViewBuilder(self._ocel, self._conditions + conditions)

    def create(self) -> Ocel:
        """Materialize the filtered view as a new :class:`Ocel` instance.

        Compiles all accumulated conditions, computes surviving event and
        object IDs with cross-propagation through E2O, and creates an
        in-memory DuckDB schema containing filtered ``VIEW`` definitions
        for every OCEL table.

        The returned :class:`Ocel` shares the parent's connection and should
        be used as a context manager to clean up the view schema::

            with ocel.view().where(expr).create() as view:
                view.events().fetchall()

        Returns:
            A view-backed :class:`Ocel` whose tables reflect the filters.
        """
        from oceldb.ocel import Ocel

        ctx = CompilationContext(self._ocel)

        # Separate by domain
        event_exprs: list[Expr] = []
        object_exprs: list[Expr] = []
        for cond in self._conditions:
            if cond.domain is Domain.EVENT:
                event_exprs.append(cond)
            else:
                object_exprs.append(cond)

        prefix = self._ocel._schema_prefix

        # Compile WHERE conditions
        event_where = _compile_where(event_exprs, ctx)
        object_where = _compile_where(object_exprs, ctx)

        # Build surviving-ID subqueries
        if event_where:
            unified_e = ctx.unified_sql(Domain.EVENT)
            event_ids = f"SELECT ocel_id FROM {unified_e} WHERE {event_where}"
        else:
            event_ids = f"SELECT ocel_id FROM {prefix}.event"

        if object_where:
            unified_o = ctx.unified_sql(Domain.OBJECT)
            object_ids = f"SELECT ocel_id FROM {unified_o} WHERE {object_where}"
        else:
            object_ids = f"SELECT ocel_id FROM {prefix}.object"

        # Cross-propagation through E2O
        e2o = f"{prefix}.event_object"

        if event_where and not object_where:
            final_event_ids = event_ids
            final_object_ids = (
                f"SELECT DISTINCT ocel_object_id AS ocel_id FROM {e2o} "
                f"WHERE ocel_event_id IN ({event_ids})"
            )
        elif object_where and not event_where:
            final_object_ids = object_ids
            final_event_ids = (
                f"SELECT DISTINCT ocel_event_id AS ocel_id FROM {e2o} "
                f"WHERE ocel_object_id IN ({object_ids})"
            )
        elif event_where and object_where:
            final_event_ids = (
                f"({event_ids}) INTERSECT "
                f"(SELECT DISTINCT ocel_event_id AS ocel_id FROM {e2o} "
                f"WHERE ocel_object_id IN ({object_ids}))"
            )
            final_object_ids = (
                f"({object_ids}) INTERSECT "
                f"(SELECT DISTINCT ocel_object_id AS ocel_id FROM {e2o} "
                f"WHERE ocel_event_id IN ({event_ids}))"
            )
        else:
            final_event_ids = f"SELECT ocel_id FROM {prefix}.event"
            final_object_ids = f"SELECT ocel_id FROM {prefix}.object"

        # Create child schema with filtered views
        schema = f"memory.oceldb_view_{uuid4().hex[:8]}"
        con = self._ocel._con

        con.execute(f"CREATE SCHEMA {schema}")
        try:
            self._materialize(
                con, schema, prefix, ctx, final_event_ids, final_object_ids
            )
        except Exception:
            con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            raise

        return Ocel(
            con=con,
            path=self._ocel._path,
            schema_prefix=schema,
            owns_connection=False,
        )

    @staticmethod
    def _materialize(
        con,
        schema: str,
        prefix: str,
        ctx: CompilationContext,
        final_event_ids: str,
        final_object_ids: str,
    ) -> None:
        """Create filtered ``VIEW`` definitions in the child schema.

        Builds views for core tables (``event``, ``object``), relationship
        tables (``event_object``, ``object_object``), map tables, and every
        per-type table — each filtered to only include surviving IDs.

        Args:
            con: The DuckDB connection.
            schema: Fully qualified child schema name.
            prefix: Fully qualified parent schema prefix.
            ctx: Compilation context for type-map introspection.
            final_event_ids: SQL subquery yielding surviving event IDs.
            final_object_ids: SQL subquery yielding surviving object IDs.
        """

        def run(sql: str) -> None:
            con.execute(sql)

        # Core tables
        run(
            f"CREATE VIEW {schema}.event AS "
            f"SELECT * FROM {prefix}.event "
            f"WHERE ocel_id IN ({final_event_ids})"
        )
        run(
            f"CREATE VIEW {schema}.object AS "
            f"SELECT * FROM {prefix}.object "
            f"WHERE ocel_id IN ({final_object_ids})"
        )

        # Relationship tables
        run(
            f"CREATE VIEW {schema}.event_object AS "
            f"SELECT * FROM {prefix}.event_object "
            f"WHERE ocel_event_id IN ({final_event_ids}) "
            f"AND ocel_object_id IN ({final_object_ids})"
        )
        run(
            f"CREATE VIEW {schema}.object_object AS "
            f"SELECT * FROM {prefix}.object_object "
            f"WHERE ocel_source_id IN ({final_object_ids}) "
            f"AND ocel_target_id IN ({final_object_ids})"
        )

        # Map tables (pass through)
        run(
            f"CREATE VIEW {schema}.event_map_type AS "
            f"SELECT * FROM {prefix}.event_map_type"
        )
        run(
            f"CREATE VIEW {schema}.object_map_type AS "
            f"SELECT * FROM {prefix}.object_map_type"
        )

        # Per-type tables
        for domain, final_ids in [
            (Domain.EVENT, final_event_ids),
            (Domain.OBJECT, final_object_ids),
        ]:
            for suffix in ctx.type_map(domain).values():
                table = f"{domain.value}_{suffix}"
                run(
                    f'CREATE VIEW {schema}."{table}" AS '
                    f'SELECT * FROM {prefix}."{table}" '
                    f"WHERE ocel_id IN ({final_ids})"
                )


def _compile_where(exprs: list[Expr], ctx: CompilationContext) -> str | None:
    """Compile and AND-join a list of expressions into a single SQL condition.

    Args:
        exprs: Expressions to compile. May be empty.
        ctx: Schema context passed to each expression's :meth:`~Expr.compile`.

    Returns:
        A SQL boolean string, or ``None`` if *exprs* is empty.
    """
    if not exprs:
        return None
    parts = [e.compile(ctx) for e in exprs]
    if len(parts) == 1:
        return parts[0]
    return " AND ".join(f"({p})" for p in parts)
