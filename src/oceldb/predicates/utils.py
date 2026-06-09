from typing import TYPE_CHECKING

from oceldb.expr import Predicate, Table, col, union

if TYPE_CHECKING:
    from oceldb.ocel import OCEL


class CountPredicate:
    """Lazy count comparison expression."""

    def __init__(self, counts: Table) -> None:
        self._counts = counts

    def __ge__(self, n: int) -> Predicate:
        return col("ocel_id").isin(
            self._counts.filter(self._counts["n"] >= n)["ocel_id"]
        )

    def __gt__(self, n: int) -> Predicate:
        return col("ocel_id").isin(
            self._counts.filter(self._counts["n"] > n)["ocel_id"]
        )

    def __le__(self, n: int) -> Predicate:
        return ~col("ocel_id").isin(
            self._counts.filter(self._counts["n"] > n)["ocel_id"]
        )

    def __lt__(self, n: int) -> Predicate:
        return ~col("ocel_id").isin(
            self._counts.filter(self._counts["n"] >= n)["ocel_id"]
        )

    def __eq__(self, n: object) -> Predicate:  # type: ignore[override]
        if isinstance(n, int) and n == 0:
            return ~col("ocel_id").isin(self._counts["ocel_id"])
        return col("ocel_id").isin(
            self._counts.filter(self._counts["n"] == n)["ocel_id"]
        )

    def __ne__(self, n: object) -> Predicate:  # type: ignore[override]
        return ~self.__eq__(n)

    def __hash__(self) -> int:
        return id(self)


def o2o_reachable_bfs(
    o2o: Table, object_type: str, direction: str, max_hops: int
) -> Table:
    layers: list[Table] = []
    if direction in ("forward", "both"):
        layers.append(
            o2o.filter(o2o["ocel_target_type"] == object_type).select(
                ocel_id=o2o["ocel_source_id"]
            )
        )
    if direction in ("backward", "both"):
        layers.append(
            o2o.filter(o2o["ocel_source_type"] == object_type).select(
                ocel_id=o2o["ocel_target_id"]
            )
        )
    reachable = union(*layers).distinct()

    for _ in range(max_hops - 1):
        expansion: list[Table] = []
        if direction in ("forward", "both"):
            expansion.append(
                o2o.filter(o2o["ocel_target_id"].isin(reachable["ocel_id"])).select(
                    ocel_id=o2o["ocel_source_id"]
                )
            )
        if direction in ("backward", "both"):
            expansion.append(
                o2o.filter(o2o["ocel_source_id"].isin(reachable["ocel_id"])).select(
                    ocel_id=o2o["ocel_target_id"]
                )
            )
        reachable = union(reachable, *expansion).distinct()

    return reachable


def o2o_reachable_recursive(ocel: OCEL, object_type: str, direction: str) -> Table:
    t = object_type.replace("'", "''")
    if direction == "forward":
        anchor = (
            f"SELECT ocel_source_id AS ocel_id FROM object_object"
            f" WHERE ocel_target_type = '{t}'"
        )
        step = (
            "SELECT oo.ocel_source_id AS ocel_id"
            " FROM object_object oo JOIN reachable r ON oo.ocel_target_id = r.ocel_id"
        )
    elif direction == "backward":
        anchor = (
            f"SELECT ocel_target_id AS ocel_id FROM object_object"
            f" WHERE ocel_source_type = '{t}'"
        )
        step = (
            "SELECT oo.ocel_target_id AS ocel_id"
            " FROM object_object oo JOIN reachable r ON oo.ocel_source_id = r.ocel_id"
        )
    else:
        anchor = (
            f"SELECT ocel_source_id AS ocel_id FROM object_object WHERE ocel_target_type = '{t}'"
            f" UNION"
            f" SELECT ocel_target_id AS ocel_id FROM object_object WHERE ocel_source_type = '{t}'"
        )
        step = (
            "SELECT oo.ocel_source_id AS ocel_id"
            " FROM object_object oo JOIN reachable r ON oo.ocel_target_id = r.ocel_id"
            " UNION"
            " SELECT oo.ocel_target_id AS ocel_id"
            " FROM object_object oo JOIN reachable r ON oo.ocel_source_id = r.ocel_id"
        )
    sql = (
        f"WITH RECURSIVE reachable(ocel_id) AS ({anchor} UNION {step})"
        " SELECT ocel_id FROM reachable"
    )
    return Table(ocel.con.sql(sql))  # pyright: ignore[reportUnknownArgumentType, reportUnknownMemberType]
