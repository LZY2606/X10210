"""Delta-debugging minimizer.

Reduces a long trace to the smallest ordered event prefix/subset that still
reproduces a given cycle signature.  Before testing a subset it is closed
under pairing rules so per-thread acquire/release balance, request/cancel
pairing and condition-wait/notify semantics are preserved (no synthetic
release is ever introduced - closure only *adds* observed events).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

from .models import (
    Event, THREAD_START, THREAD_END, REQUEST, GRANT, RELEASE,
    COND_WAIT, COND_NOTIFY, COND_GRANT, CANCEL,
)
from .engine import ReplayEngine, ReplayResult


def _signatures_present(result: ReplayResult) -> Set[str]:
    sigs: Set[str] = set()
    for cycles in result.cycles_at.values():
        for c in cycles:
            sigs.add(c["signature"])
    return sigs


def reproduces(events: List[Event], signature: str,
               lock_types: Optional[Dict[str, str]] = None) -> bool:
    result = ReplayEngine(lock_types=lock_types).run(events)
    return signature in _signatures_present(result)


def _closure(events: List[Event], selected: Set[int]) -> List[Event]:
    """Expand an event-id selection to a semantically legal subset."""
    by_id: Dict[int, Event] = {}
    groups: Dict[Tuple, List[Event]] = defaultdict(list)
    thread_events: Dict[Tuple[str, int], List[Event]] = defaultdict(list)
    cond_groups: Dict[Tuple, List[Event]] = defaultdict(list)
    for ev in events:
        by_id[ev.id] = ev
        thread_events[(ev.thread, ev.gen)].append(ev)
        if ev.lock:
            groups[(ev.gen, ev.thread, ev.lock)].append(ev)
        if ev.kind in (COND_WAIT, COND_GRANT, COND_NOTIFY) and ev.cond:
            cond_groups[(ev.gen, ev.cond)].append(ev)
    for lst in list(groups.values()) + list(thread_events.values()) + \
               list(cond_groups.values()):
        lst.sort(key=lambda e: e.sort_key())

    keep: Set[int] = set(selected)
    changed = True
    while changed:
        changed = False

        def add(ev: Event) -> bool:
            if ev.id not in keep:
                keep.add(ev.id)
                return True
            return False

        # thread lifecycle
        for (t, gen), lst in thread_events.items():
            if any(e.id in keep for e in lst):
                starts = [e for e in lst if e.kind == THREAD_START]
                for e in starts[:1]:
                    changed |= add(e)

        # per (gen,thread,lock): prefix closure keeps request/grant/release
        # stacks balanced and keeps cancels next to their requests.
        # cond events are handled separately so a cond event never drags in
        # unrelated lock-acquire history.
        for key, lst in groups.items():
            lock_lst = [e for e in lst if e.kind != COND_WAIT]
            chosen = [i for i, e in enumerate(lock_lst) if e.id in keep]
            if not chosen:
                continue
            upto = max(chosen)
            for e in lock_lst[: upto + 1]:
                changed |= add(e)

        # condition spans: wait must stay with its grant/cancel; notifies on
        # the same condition variable between them are part of the semantics.
        for (gen, cond), lst in cond_groups.items():
            waits = [e for e in lst if e.kind == COND_WAIT and e.id in keep]
            grants = [e for e in lst
                      if e.kind == COND_GRANT and e.id in keep]
            for w in waits:
                changed |= add(w)
                later = [e for e in thread_events[(w.thread, gen)]
                         if e.sort_key() >= w.sort_key()
                         and e.kind in (COND_GRANT, CANCEL)]
                terminator = later[0] if later else None
                if terminator is not None:
                    changed |= add(terminator)
                end = terminator.ts if terminator else w.ts
                for n in lst:
                    if n.kind == COND_NOTIFY and w.ts <= n.ts <= end:
                        changed |= add(n)
            for g in grants:
                prior = [e for e in thread_events[(g.thread, gen)]
                         if e.sort_key() <= g.sort_key()
                         and e.kind == COND_WAIT and e.cond == cond]
                if prior:
                    changed |= add(prior[-1])
                    for n in lst:
                        if n.kind == COND_NOTIFY and                                 prior[-1].ts <= n.ts <= g.ts:
                            changed |= add(n)

        # a cancel kept for a thread needs its surrounding pending request
        for (t, gen), lst in thread_events.items():
            for i, e in enumerate(lst):
                if e.kind == CANCEL and e.id in keep:
                    prior = [x for x in lst[: i + 1]
                             if x.kind in (REQUEST, COND_WAIT)]
                    if prior:
                        changed |= add(prior[-1])

    return [e for e in events if e.id in keep]


def minimize(events: List[Event], signature: str,
             lock_types: Optional[Dict[str, str]] = None) -> dict:
    """ddmin over logical-order events with semantic closure."""
    ordered = sorted(events, key=lambda e: e.sort_key())
    if not reproduces(ordered, signature, lock_types):
        return {"signature": signature, "found": False, "events": [],
                "original_size": len(ordered), "minimal_size": 0,
                "iterations": 0}

    ids = [e.id for e in ordered]
    by_id = {e.id: e for e in ordered}
    current: Set[int] = set(ids)
    iterations = 0

    def test(keep: Set[int]) -> bool:
        closed = _closure(ordered, keep)
        iterations_inc[0] += 1
        return reproduces(closed, signature, lock_types)

    iterations_inc = [0]
    n = 2
    while len(current) >= 2:
        cur_ids = sorted(current, key=lambda i: by_id[i].sort_key())
        chunk = max(1, len(cur_ids) // n)
        progress = False
        i = 0
        while i < len(cur_ids):
            block = set(cur_ids[i: i + chunk])
            candidate = current - block
            if candidate and test(candidate):
                current = candidate
                progress = True
                n = max(n - 1, 2)
                break
            i += chunk
        if progress:
            continue
        if n >= len(current):
            break
        n = min(n * 2, len(current))
    iterations = iterations_inc[0]

    minimal = [by_id[i] for i in
               sorted(current, key=lambda i: by_id[i].sort_key())]
    result = ReplayEngine(lock_types=lock_types).run(minimal)
    first_frame = result.signature_first_frame.get(signature)
    cycle = None
    if first_frame is not None:
        for c in result.cycles_at.get(first_frame, []):
            if c["signature"] == signature:
                cycle = c
                break
    return {
        "signature": signature,
        "found": True,
        "events": [e.to_dict() for e in minimal],
        "event_ids": [e.id for e in minimal],
        "original_size": len(ordered),
        "minimal_size": len(minimal),
        "iterations": iterations,
        "first_frame": first_frame,
        "cycle": cycle,
    }
