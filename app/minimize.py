"""Reduce a trace to the smallest fragment that still reproduces a cycle.

Pairing semantics are preserved by grouping events into atomic *units*:

* acquisition unit: acquire_req + acquired + every release that balances
  the granted depth (releases never travel without their acquire);
* cancelled request: acquire_req + acquire_timeout (or a thread_end that
  cancels it);
* condition unit: cond_wait plus its paired request, the notify that wakes
  it and/or its timeout; it depends on the notifier's guard-acquisition
  unit, so condition wait/notify semantics survive reduction;
* thread_start / thread_end are standalone units.

Greedy removal drops units together with their dependency closure while
the target cycle signature is still reproduced.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from .events import Event, order_key
from .replay import analyze, _lk


def build_units(events: List[Event]
                ) -> Tuple[List[Set[int]], Dict[int, Set[int]]]:
    """Return (units of replay-order positions, unit dependency edges)."""
    units: List[Set[int]] = []
    deps: Dict[int, Set[int]] = {}
    owner_unit: Dict[int, int] = {}

    def new_unit(*positions: int) -> int:
        unit = set(positions)
        units.append(unit)
        uid = len(units) - 1
        deps.setdefault(uid, set())
        for pos in positions:
            owner_unit[pos] = uid
        return uid

    def add_to(unit_id: int, pos: int) -> None:
        units[unit_id].add(pos)
        owner_unit[pos] = unit_id

    # (thread, scoped lock) -> stack of granted unit ids (reentry depth)
    held_stack: Dict[Tuple[str, str], List[int]] = {}
    # (thread, scoped lock) -> pending request unit id
    pending: Dict[Tuple[str, str], int] = {}
    # cond -> waiter unit ids in arrival order
    cond_waiters: Dict[str, List[int]] = {}
    # (thread, cond) -> waiter unit id
    cond_by_thread: Dict[Tuple[str, str], int] = {}

    for pos, e in enumerate(events):
        if e.type in ("thread_start", "thread_end"):
            uid_ = new_unit(pos)
            if e.type == "thread_end":
                for (thread, _key), request_unit in list(pending.items()):
                    if thread == e.thread:
                        add_to(request_unit, pos)
            continue

        key = _lk(e.lock, e.gen)
        tkey = (e.thread, key)

        if e.type == "acquire_req":
            pending[tkey] = new_unit(pos)

        elif e.type == "acquired":
            unit_id = pending.pop(tkey, None)
            if unit_id is None:
                unit_id = new_unit(pos)
            else:
                add_to(unit_id, pos)
            held_stack.setdefault(tkey, []).append(unit_id)

        elif e.type == "release":
            stack = held_stack.get(tkey, [])
            if stack:
                add_to(stack[-1], pos)
                stack.pop()
            else:
                new_unit(pos)

        elif e.type == "cond_wait":
            cond = e.cond or (e.lock + "#cv")
            unit_id = pending.pop(tkey, None)
            if unit_id is None:
                unit_id = new_unit(pos)
            add_to(unit_id, pos)
            # the wait releases the guard: its releases live in that unit too
            stack = held_stack.get(tkey, [])
            if stack:
                guard_unit = stack[-1]
                deps[unit_id].add(guard_unit)
            cond_by_thread[(e.thread, cond)] = unit_id
            cond_waiters.setdefault(cond, []).append(unit_id)
            pending[tkey] = unit_id

        elif e.type in ("cond_notify", "cond_notify_all"):
            cond = e.cond or (e.lock + "#cv")
            waiters = list(cond_waiters.get(cond, []))
            targets = waiters if e.type == "cond_notify_all" else waiters[:1]
            if not targets:
                new_unit(pos)
            else:
                add_to(targets[0], pos)
                nkey = (e.thread, _lk(e.lock, e.gen))
                notifier_stack = held_stack.get(nkey, [])
                if notifier_stack:
                    deps[targets[0]].add(notifier_stack[-1])

        elif e.type == "acquire_timeout":
            unit_id = pending.pop(tkey, None)
            if unit_id is None:
                new_unit(pos)
            else:
                add_to(unit_id, pos)
            cond = e.cond or (e.lock + "#cv")
            waiter = cond_by_thread.pop((e.thread, cond), None)
            if waiter is not None:
                cond_waiters.get(cond, []).remove(waiter)

    return units, deps


def minimize(events: List[Event], lock_types: Optional[Dict[str, str]] = None,
             target_ts: Optional[float] = None
             ) -> Tuple[List[Event], float, Dict[str, Any]]:
    """Greedily remove pairing units while the same cycle is reproduced."""
    ordered = sorted(events, key=order_key)
    types = dict(lock_types or {})

    def signature_at(ts: float) -> Optional[Tuple[str, ...]]:
        snap = analyze(ordered, types, at_ts=ts, complete=False)
        return tuple(snap["cycles"][0]["threads"]) if snap["cycles"] else None

    if target_ts is None:
        full = analyze(ordered, types, complete=False)
        if not full["cycles"]:
            return ordered, ordered[-1].ts if ordered else 0.0, \
                {"reason": "no cycle to minimize"}
        target = tuple(full["cycles"][0]["threads"])
        target_ts = full["cycles"][0]["wait_for_edges"][-1]["wait_event"]["ts"]
    else:
        sig = signature_at(target_ts)
        if sig is None:
            return ordered, target_ts, {"reason": "no cycle at target_ts"}
        target = sig

    units, deps = build_units(ordered)

    def closure(chosen: Set[int]) -> Set[int]:
        result: Set[int] = set()
        stack = list(chosen)
        while stack:
            uid_ = stack.pop()
            if uid_ in result:
                continue
            result.add(uid_)
            stack.extend(deps.get(uid_, set()) - result)
        return result

    def reproduces(chosen: Set[int]) -> Optional[float]:
        kept = closure(chosen)
        positions: Set[int] = set()
        for uid_ in kept:
            positions |= units[uid_]
        subset = [ordered[p] for p in sorted(positions)]
        if not subset:
            return None
        ts = max(target_ts, max(ev.ts for ev in subset))
        snap = analyze(subset, types, at_ts=ts, complete=False)
        sigs = {tuple(c["threads"]) for c in snap["cycles"]}
        return ts if target in sigs else None

    kept = set(range(len(units)))
    repro_ts = target_ts
    changed = True
    while changed:
        changed = False
        for uid_ in sorted(kept):
            trial = kept - {uid_}
            ts = reproduces(trial)
            if ts is not None:
                kept = trial
                repro_ts = ts
                changed = True

    positions: Set[int] = set()
    for uid_ in closure(kept):
        positions |= units[uid_]
    subset = [ordered[p] for p in sorted(positions)]
    info = {
        "target_signature": list(target),
        "original_events": len(ordered),
        "minimized_events": len(subset),
        "removed": len(ordered) - len(subset),
        "repro_ts": repro_ts,
        "units": len(units),
    }
    return subset, repro_ts, info
