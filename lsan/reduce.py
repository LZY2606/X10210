"""Minimize a trace to the smallest subsequence still reproducing a cycle.

Events are first joined into *units* mirroring the replay semantics:

* request <-> acquired <-> releases (including reentrant nesting levels)
* cond_wait <-> its reacquired event <-> cond_notify that woke it
* timeout <-> the request it cancels

Only whole units are ever removed, which preserves per-thread pairing and
condition-variable semantics. A greedy delta-debug loop drops every unit that
can be removed while the *same* cycle (same threads/locks and the same
originating request events) still appears at some point in the replay.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .model import Engine


class DSU:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _build_units(events: List[Dict[str, Any]]) -> List[frozenset]:
    n = len(events)
    dsu = DSU(n)
    pending: Dict[str, List[int]] = {}          # lock -> request/condwait idx
    # (thread, lock) -> flat stack of acquisition orig indices (one per level)
    holds: Dict[Tuple[str, str], List[int]] = {}
    tickets: Dict[int, List[int]] = {}          # condwait idx -> saved acqs
    condq: Dict[str, List[int]] = {}            # cond -> condwait idx
    cond_lock: Dict[int, str] = {}

    for i, e in enumerate(events):
        t, th, lk = e["type"], e.get("thread"), e.get("lock")
        if t == "request":
            pending.setdefault(lk, []).append(i)
        elif t == "acquired":
            q = pending.setdefault(lk, [])
            j = next((k for k, idx in enumerate(q)
                      if events[idx].get("thread") == th), None)
            if j is None:
                continue
            req_idx = q.pop(j)
            dsu.union(i, req_idx)
            if req_idx in tickets:               # condition reacquire
                saved = tickets.pop(req_idx)
                for a in saved:
                    dsu.union(i, a)
                holds.setdefault((th, lk), []).extend(saved + [i])
            else:
                holds.setdefault((th, lk), []).append(i)
        elif t == "release":
            stk = holds.get((th, lk))
            if stk:
                dsu.union(i, stk.pop())
        elif t in ("cond_wait", "cond_timedwait"):
            cond = e.get("cond") or lk
            stk = holds.setdefault((th, lk), [])
            saved = list(stk)
            stk.clear()
            tickets[i] = saved
            for a in saved:
                dsu.union(i, a)
            pending.setdefault(lk, []).append(i)
            condq.setdefault(cond, []).append(i)
            cond_lock[i] = cond
        elif t in ("cond_notify", "cond_notify_all"):
            cond = e.get("cond") or lk
            ws = condq.get(cond, [])
            take = 1 if t == "cond_notify" else len(ws)
            for _ in range(take):
                if ws:
                    dsu.union(i, ws.pop(0))
        elif t == "timeout":
            q = pending.get(lk, [])
            j = next((k for k, idx in enumerate(q)
                      if events[idx].get("thread") == th), None)
            if j is not None:
                dsu.union(i, q.pop(j))
        # thread_start / thread_end remain singleton units

    groups: Dict[int, List[int]] = {}
    for i in range(n):
        groups.setdefault(dsu.find(i), []).append(i)
    return [frozenset(v) for v in groups.values()]


def _wait_sig(cycle: Dict[str, Any]) -> Tuple[Tuple[str, str, str, int], ...]:
    out = []
    for step in cycle["chain"]:
        if step["kind"] == "wait":
            out.append((step["thread"], step["lock"], step["mode"],
                        step["request_event_index"]))
    return tuple(sorted(out))


def _clean(e: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in e.items() if not k.startswith("_")}


def reduce_trace(events: List[Dict[str, Any]],
                 lock_types: Optional[Dict[str, str]] = None,
                 cycle_id: Optional[str] = None) -> Dict[str, Any]:
    tagged = []
    for i, e in enumerate(events):
        ee = dict(e)
        ee["_orig"] = i
        tagged.append(ee)
    base = Engine(tagged, lock_types=lock_types)
    final = base.snapshot()
    if not final["cycles"]:
        return {"events": [_clean(e) for e in tagged], "removed": 0,
                "cycle": None, "at_index": None,
                "reason": "no cycle detected in the trace"}
    cycle = next((c for c in final["cycles"] if c["id"] == cycle_id),
                 final["cycles"][0])
    target = _wait_sig(cycle)
    # target refers to raw original positions already
    units = _build_units(base.raw)
    kept = set(range(len(base.raw)))
    found_at = None
    changed = True
    while changed:
        changed = False
        live = [u for u in (x & kept for x in units) if u]
        # small/irrelevant units first; deterministic within each pass
        live.sort(key=lambda u: (len(u), -min(u)))
        for u in live:
            trial = sorted(kept - u)
            sub = [base.raw[i] for i in trial]
            try:
                eng = Engine(sub, lock_types=lock_types)
            except Exception:
                continue
            present, at = _cycle_present_orig(eng, target)
            if present:
                kept = set(trial)
                found_at = at
                changed = True
                break

    ordered = [_clean(base.raw[i]) for i in sorted(kept)]
    return {"events": ordered, "removed": len(base.raw) - len(kept),
            "cycle": cycle, "at_index": found_at}


def _cycle_present_orig(engine: Engine,
                        target: Tuple[Tuple[str, str, str, int], ...]
                        ) -> Tuple[bool, Optional[int]]:
    for k in range(1, len(engine.states) + 1):
        snap = engine.snapshot(k)
        for c in snap["cycles"]:
            sig = set()
            for step in c["chain"]:
                if step["kind"] == "wait":
                    req_idx = step["request_event_index"]
                    orig = engine.raw[req_idx].get("_orig", req_idx)
                    sig.add((step["thread"], step["lock"],
                             step["mode"], orig))
            if sig == set(target):
                return True, k
    return False, None
