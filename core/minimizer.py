"""Reduce a long trace to the smallest subsequence reproducing a cycle.

Reduction operates on *pairing groups* so that acquire/release and
condition wait/wake semantics are never torn apart.
"""
from __future__ import annotations

from .engine import HIGH, Engine
from .graph import find_cycles
from .models import Event

HARD_ISSUES = {
    "over_release",
    "orphan_acquire",
    "unmatched_wake",
    "conflicting_grant",
    "malformed",
    "self_deadlock",
    "wait_without_lock",
}


def build_groups(events: list[Event]) -> list[list[int]]:
    by_index = {event.seq: index for index, event in enumerate(events)}
    groups: dict[int, list[int]] = {}

    def group_for(seq: int) -> int:
        index = by_index[seq]
        groups.setdefault(index, [index])
        return index

    def merge(seq_a: int, seq_b: int) -> int:
        ga = group_for(seq_a)
        gb = group_for(seq_b)
        if ga == gb:
            return ga
        groups[ga].extend(groups.pop(gb))
        return ga

    # Pair acquire request -> success -> releases, LIFO per (thread, lock).
    acquire_stack: dict[tuple[str, int, str | None], list[int]] = {}
    pending: dict[int, int] = {}  # event seq -> request seq
    for event in events:
        if event.kind == "acquire_req":
            req_g = group_for(event.seq)
            acquire_stack.setdefault(
                (event.thread, event.gen, event.lock), []
            ).append(req_g)
        elif event.kind == "acquire_ok":
            stack = acquire_stack.get((event.thread, event.gen, event.lock), [])
            if stack:
                req_g = stack[-1]
                groups[req_g].append(by_index[event.seq])
                groups.pop(by_index[event.seq], None)
                pending[event.seq] = req_g
            # Orphan grants remain their own singleton.
        elif event.kind == "release":
            stack = acquire_stack.get((event.thread, event.gen, event.lock), [])
            if stack:
                req_g = stack.pop()
                groups[req_g].append(by_index[event.seq])
                groups.pop(by_index[event.seq], None)
            # Over-releases stay singletons (they are issues themselves).

    # Condition wait -> notify -> wake chain per (thread, cond).
    cond_stack: dict[tuple[str, int, str | None], list[int]] = {}
    for event in events:
        if event.kind == "cond_wait":
            cond_stack.setdefault(
                (event.thread, event.gen, event.cond), []
            ).append(group_for(event.seq))
        elif event.kind in {"cond_notify", "cond_notify_all"}:
            group_for(event.seq)
            for key, stack in cond_stack.items():
                if key[1] == event.gen and key[2] == event.cond and stack:
                    merge_g = stack[0]
                    groups[merge_g].append(by_index[event.seq])
                    groups.pop(by_index[event.seq], None)
                    if event.kind == "cond_notify":
                        break
        elif event.kind == "cond_wake":
            stack = cond_stack.get((event.thread, event.gen, event.cond), [])
            if stack:
                merge_g = stack.pop(0)
                groups[merge_g].append(by_index[event.seq])
                groups.pop(by_index[event.seq], None)

    # Timeout/cancel joins the acquire or cond-wait group it revokes.
    for event in events:
        if event.kind != "cancel":
            continue
        stack_key = (event.thread, event.gen, event.lock)
        stack = acquire_stack.get(stack_key, [])
        joined = False
        if stack:
            groups[stack.pop()].append(by_index[event.seq])
            groups.pop(by_index[event.seq], None)
            joined = True
        if not joined:
            cstack = cond_stack.get((event.thread, event.gen, event.cond), [])
            if cstack:
                groups[cstack.pop(0)].append(by_index[event.seq])
                groups.pop(by_index[event.seq], None)

    raw = sorted(groups.values(), key=lambda members: min(members))
    # Ensure every index is represented.
    covered = {index for members in raw for index in members}
    for index in range(len(events)):
        if index not in covered:
            raw.append([index])
    return [sorted(members) for members in raw]


def cycle_signature(cycle: dict) -> frozenset:
    return frozenset(
        (part["thread"], part["lock"], part["role"], part.get("mode"))
        for part in cycle["participants"]
    )


def _reproduces(
    events: list[Event],
    keep: list[int],
    target: frozenset,
    lock_types: dict[str, str] | None,
) -> bool:
    subset = [events[index] for index in sorted(keep)]
    engine = Engine(lock_types=lock_types)
    engine.reset(subset)
    snapshot = engine.snapshot(include_missing=False)
    for issue in snapshot["issues"]:
        if issue["kind"] in HARD_ISSUES and issue["severity"] == HIGH:
            return False
    return any(cycle_signature(cycle) == target for cycle in snapshot["cycles"])


def minimize(
    events: list[Event],
    target_cycle: dict,
    lock_types: dict[str, str] | None = None,
) -> list[Event]:
    """Classic ddmin over pairing groups, preserving canonical order."""
    target = cycle_signature(target_cycle)
    groups = build_groups(events)
    kept = list(range(len(groups)))

    if not _reproduces(
        events,
        [index for group_index in kept for index in groups[group_index]],
        target,
        lock_types,
    ):
        return list(events)

    granularity = 2
    while len(kept) >= granularity:
        size = max(1, len(kept) // granularity)
        reduced = False
        start = 0
        while start < len(kept):
            chunk = kept[start : start + size]
            trial = [index for index in kept if index not in chunk]
            event_indices = [
                event_index
                for group_index in trial
                for event_index in groups[group_index]
            ]
            if _reproduces(events, event_indices, target, lock_types):
                kept = trial
                granularity = 2
                reduced = True
                break
            start += size
        if not reduced:
            if granularity >= len(kept):
                break
            granularity = min(len(kept), granularity * 2)

    final_indices = sorted(
        event_index
        for group_index in kept
        for event_index in groups[group_index]
    )
    return [events[index] for index in final_indices]
