"""Case-centric directly-follows graph discovery."""

from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

from oceldb.case_centric.types import CaseCentricEventLog


@dataclass(frozen=True)
class DirectlyFollowsGraph:
    """Directly-follows graph with trace variants for recursive discovery.

    Filtering uses a relative ``threshold`` in ``[0, 1]``:

    - An activity ``a`` is retained if
      ``count(a) ≥ threshold × max_activity_count``.
    - An edge ``(a, b)`` is retained if
      ``count(a, b) ≥ threshold × max_outgoing_count(a)``.

    A threshold of ``0.0`` keeps everything; ``1.0`` keeps only the single
    most frequent activity and the single strongest outgoing edge per source.
    The default ``0.0`` matches the behaviour of the original inductive miner
    (no filtering).
    """

    activities: frozenset[str]
    start_counts: Counter[str]
    end_counts: Counter[str]
    edge_counts: Counter[tuple[str, str]]
    variants: Counter[tuple[str, ...]]
    threshold: float = 0.0

    @property
    def has_empty_traces(self) -> bool:
        """Return whether the graph contains projected empty traces."""
        return self.variants[()] > 0

    def without_empty_traces(self) -> "DirectlyFollowsGraph":
        """Return a graph with empty traces removed."""
        variants = Counter(self.variants)
        variants.pop((), None)
        return dfg_from_variants(variants, threshold=self.threshold)

    def project(self, activities: Iterable[str]) -> "DirectlyFollowsGraph":
        """Project trace variants to *activities* and rebuild the DFG."""
        selected = set(activities)
        variants: Counter[tuple[str, ...]] = Counter()
        for variant, count in self.variants.items():
            projected = tuple(activity for activity in variant if activity in selected)
            variants[projected] += count
        return dfg_from_variants(variants, threshold=self.threshold)


def discover_dfg(
    case_log: CaseCentricEventLog,
    *,
    case_id: str = "case:concept:name",
    activity: str = "concept:name",
    timestamp: str = "time:timestamp",
    event_id: str = "ocel_event_id",
    threshold: float = 0.0,
) -> DirectlyFollowsGraph:
    """Discover a directly-follows graph from a case-centric event log.

    ``threshold`` is a relative noise-filtering parameter in ``[0, 1]``.
    See :class:`DirectlyFollowsGraph` for the exact filtering rules.
    """
    _check_threshold(threshold)

    rows = (
        case_log.select(case_id, activity, timestamp, event_id)
        .order_by(case_id, timestamp, event_id)
        .execute()
    )
    traces: defaultdict[str, list[str]] = defaultdict(list)
    for row in rows.iter_rows(named=True):
        traces[cast(str, row[case_id])].append(cast(str, row[activity]))

    return dfg_from_traces(traces.values(), threshold=threshold)


def dfg_from_traces(
    traces: Iterable[Sequence[str]], *, threshold: float = 0.0
) -> DirectlyFollowsGraph:
    """Build a directly-follows graph from activity traces."""
    _check_threshold(threshold)
    variants: Counter[tuple[str, ...]] = Counter(tuple(trace) for trace in traces)
    return dfg_from_variants(variants, threshold=threshold)


def dfg_from_variants(
    variants: Counter[tuple[str, ...]], *, threshold: float = 0.0
) -> DirectlyFollowsGraph:
    """Build a directly-follows graph from counted trace variants.

    Activities and edges below the relative ``threshold`` are removed before
    the DFG is constructed; see :class:`DirectlyFollowsGraph` for the rules.
    """
    _check_threshold(threshold)

    # --- activity counts (unfiltered) ---
    activity_counts: Counter[str] = Counter()
    for variant, count in variants.items():
        for act in variant:
            activity_counts[act] += count

    # --- filter activities relative to the most frequent one ---
    max_activity = max(activity_counts.values(), default=0)
    min_activity = threshold * max_activity
    kept_activities = {
        act for act, count in activity_counts.items() if count >= min_activity
    }

    # project variants to kept activities
    filtered_variants: Counter[tuple[str, ...]] = Counter()
    for variant, count in variants.items():
        projected = tuple(act for act in variant if act in kept_activities)
        filtered_variants[projected] += count

    # --- rebuild activity/edge/start/end counts from filtered variants ---
    start_counts: Counter[str] = Counter()
    end_counts: Counter[str] = Counter()
    raw_edge_counts: Counter[tuple[str, str]] = Counter()
    activities: set[str] = set()

    for variant, count in filtered_variants.items():
        if not variant:
            continue
        activities.update(variant)
        start_counts[variant[0]] += count
        end_counts[variant[-1]] += count
        for edge in zip(variant, variant[1:]):
            raw_edge_counts[edge] += count

    # --- filter edges relative to the strongest outgoing edge per source ---
    max_outgoing: Counter[str] = Counter()
    for (src, _), count in raw_edge_counts.items():
        if count > max_outgoing[src]:
            max_outgoing[src] = count

    edge_counts: Counter[tuple[str, str]] = Counter(
        {
            edge: count
            for edge, count in raw_edge_counts.items()
            if count >= threshold * max_outgoing[edge[0]]
        }
    )

    return DirectlyFollowsGraph(
        activities=frozenset(activities),
        start_counts=start_counts,
        end_counts=end_counts,
        edge_counts=edge_counts,
        variants=filtered_variants,
        threshold=threshold,
    )


def _check_threshold(threshold: float) -> None:
    if not (0.0 <= threshold <= 1.0):
        raise ValueError(f"threshold must be in [0, 1], got {threshold!r}.")
