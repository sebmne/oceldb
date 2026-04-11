"""Integration tests for TableQuery — execute against a real DuckDB."""

from __future__ import annotations

import pytest

from oceldb.dsl import (
    asc,
    attr,
    avg,
    count,
    count_distinct,
    desc,
    event_id,
    field,
    id_,
    max_,
    min_,
    object_id,
    source_id,
    sum_,
    target_id,
    time_,
    type_,
)


# ── Basic table access ──────────────────────────────────────────────────


class TestTableAccess:
    def test_events_count(self, ocel):
        assert ocel.tables.events().count() == 5

    def test_objects_count(self, ocel):
        assert ocel.tables.objects().count() == 3

    def test_event_objects_count(self, ocel):
        assert ocel.tables.event_objects().count() == 6

    def test_object_objects_count(self, ocel):
        assert ocel.tables.object_objects().count() == 2

    def test_exists_true(self, ocel):
        assert ocel.tables.events().exists() is True

    def test_to_sql_returns_string(self, ocel):
        sql = ocel.tables.events().to_sql()
        assert isinstance(sql, str)
        assert "event" in sql.lower()


# ── Select ───────────────────────────────────────────────────────────────


class TestTableSelect:
    def test_select_field(self, ocel):
        rel = ocel.tables.events().select(type_().as_("t")).relation()
        rows = rel.fetchall()
        assert len(rows) == 5

    def test_select_multiple(self, ocel):
        rel = (
            ocel.tables.events()
            .select(id_().as_("id"), type_().as_("type"))
            .relation()
        )
        row = rel.fetchone()
        assert len(row) == 2

    def test_distinct(self, ocel):
        rel = ocel.tables.events().select(type_()).distinct().relation()
        rows = rel.fetchall()
        assert len(rows) == 2  # "Create Order" and "Pay Order"


# ── Aggregation ──────────────────────────────────────────────────────────


class TestTableAggregation:
    def test_global_count(self, ocel):
        result = ocel.tables.events().agg(count().as_("n")).scalar()
        assert result == 5

    def test_count_distinct(self, ocel):
        result = ocel.tables.events().count_distinct(type_())
        assert result == 2

    def test_group_by_count(self, ocel):
        # group_by without select: only agg columns appear in result
        rel = (
            ocel.tables.events()
            .group_by(type_())
            .agg(count().as_("n"))
            .relation()
        )
        rows = rel.fetchall()
        assert len(rows) == 2
        assert sorted(r[0] for r in rows) == [2, 3]

    def test_min_max(self, ocel):
        rel = (
            ocel.tables.events()
            .agg(
                min_(time_()).as_("earliest"),
                max_(time_()).as_("latest"),
            )
            .relation()
        )
        row = rel.fetchone()
        assert row[0] < row[1]


# ── Ordering ─────────────────────────────────────────────────────────────


class TestTableOrdering:
    def test_order_asc(self, ocel):
        rel = (
            ocel.tables.events()
            .select(id_().as_("id"), time_().as_("t"))
            .order_by(asc(time_()))
            .relation()
        )
        rows = rel.fetchall()
        assert rows[0][0] == "e1"

    def test_order_desc(self, ocel):
        rel = (
            ocel.tables.events()
            .select(id_().as_("id"), time_().as_("t"))
            .order_by(desc(time_()))
            .relation()
        )
        rows = rel.fetchall()
        assert rows[0][0] == "e5"


# ── Limit ────────────────────────────────────────────────────────────────


class TestTableLimit:
    def test_limit(self, ocel):
        rel = ocel.tables.events().select(id_()).limit(2).relation()
        rows = rel.fetchall()
        assert len(rows) == 2

    def test_limit_zero(self, ocel):
        rel = ocel.tables.events().select(id_()).limit(0).relation()
        rows = rel.fetchall()
        assert len(rows) == 0

    def test_negative_limit_raises(self, ocel):
        with pytest.raises(ValueError, match="non-negative"):
            ocel.tables.events().limit(-1)


# ── Validation ───────────────────────────────────────────────────────────


class TestTableValidation:
    def test_select_agg_without_group_by_raises(self, ocel):
        with pytest.raises(ValueError, match="group_by"):
            (
                ocel.tables.events()
                .select(type_())
                .agg(count().as_("n"))
                .to_sql()
            )

    def test_select_with_group_by_validation_bug(self, ocel):
        # BUG: FieldExpr uses eq=False, making it unhashable.
        # _validate tries set(query.groupings) which fails with TypeError.
        with pytest.raises(TypeError, match="unhashable"):
            (
                ocel.tables.events()
                .select(type_(), id_())
                .group_by(type_())
                .agg(count().as_("n"))
                .to_sql()
            )


# ── Immutability ─────────────────────────────────────────────────────────


class TestTableImmutability:
    def test_select_does_not_mutate(self, ocel):
        q1 = ocel.tables.events()
        q2 = q1.select(type_())
        assert q1.selections == ()
        assert len(q2.selections) == 1

    def test_chain_produces_new_query(self, ocel):
        q1 = ocel.tables.events()
        q2 = q1.select(type_()).distinct()
        assert q1.is_distinct is False
        assert q2.is_distinct is True
        assert q1 is not q2

    def test_limit_does_not_mutate(self, ocel):
        q1 = ocel.tables.events()
        q2 = q1.limit(5)
        assert q1.limit_n is None
        assert q2.limit_n == 5


# ── Event-Object and Object-Object tables ───────────────────────────────


class TestRelationTables:
    def test_event_object_fields(self, ocel):
        rel = (
            ocel.tables.event_objects()
            .select(event_id().as_("eid"), object_id().as_("oid"))
            .limit(1)
            .relation()
        )
        row = rel.fetchone()
        assert len(row) == 2

    def test_object_object_fields(self, ocel):
        rel = (
            ocel.tables.object_objects()
            .select(source_id().as_("src"), target_id().as_("tgt"))
            .relation()
        )
        rows = rel.fetchall()
        assert len(rows) == 2

    def test_group_event_objects(self, ocel):
        # group_by without select: only agg column in result
        rel = (
            ocel.tables.event_objects()
            .group_by(object_id())
            .agg(count().as_("n"))
            .order_by(desc(count()))
            .relation()
        )
        rows = rel.fetchall()
        # o1 has 3 event relations (most), so first row is 3
        assert rows[0][0] == 3
