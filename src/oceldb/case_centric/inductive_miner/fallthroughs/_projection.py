"""Shared projection used by the (strict) tau-loop fallthroughs."""

from collections import Counter

from oceldb.case_centric.inductive_miner.dfg import (
    DirectlyFollowsGraph,
    dfg_from_variants,
)


def tau_loop_projection(
    dfg: DirectlyFollowsGraph, *, require_previous_end: bool
) -> DirectlyFollowsGraph | None:
    """Cut traces wherever a start activity restarts the process.

    Returns the projected DFG, or None if no extra cuts were introduced.
    When ``require_previous_end`` is true only restarts that follow an end
    activity are honoured — the *strict* variant.
    """
    starts = set(dfg.start_counts)
    if not starts:
        return None

    variants: Counter[tuple[str, ...]] = Counter()
    ends = set(dfg.end_counts)
    original = dfg.total_traces
    projected = 0
    for variant, count in dfg.variants.items():
        if not variant:
            continue
        segment_start = 0
        for index in range(1, len(variant)):
            is_restart = variant[index] in starts
            follows_end = variant[index - 1] in ends
            if is_restart and (follows_end or not require_previous_end):
                variants[variant[segment_start:index]] += count
                projected += count
                segment_start = index
        variants[variant[segment_start:]] += count
        projected += count

    if projected <= original:
        return None
    return dfg_from_variants(variants, threshold=dfg.threshold)
