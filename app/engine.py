"""Deterministic lock-order replay engine.

The engine consumes normalized thread/lock events ordered by collection
generation and arrival sequence. It never fabricates compensating events:
missing or out-of-order observations are recorded as anomalies and affected
graph edges are marked with a confidence level.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

VALID_TYPES = ("mutex", "recursive", "rwlock")


def new_state() -> dict[str, Any]:
    return {
        "gen": None,
        "locks": {},
        "threads": {},
        "waiters": {},
        "next_waiter": 1,
        "next_ep": 1,
        "anomalies": [],
        "ep_by_event": {},
        "links": [],
        "processed": 0,
        "last_event_id": None,
        "last_ts": None,
    }


def ordered_events(events: Iterable[dict]) -> list[dict]:
    """Order by generation (ranked by first appearance) then seq/ts/id."""
    evs = list(events)
    gen_rank: dict[str, int] = {}
    for ev in sorted(evs, key=_arrival_key):
        if ev["gen"] not in gen_rank:
            gen_rank[ev["gen"]] = len(gen_rank)
    evs.sort(key=lambda e: (gen_rank[e["gen"]], e["seq"], e["ts"], e["id"]))
    return evs


def _arrival_key(ev: dict):
    return (ev.get("arrival_seq", ev["seq"]), ev["id"])


def replay(
    events: Iterable[dict],
    declared_types: dict[str, str] | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    declared = declared_types or {}
    st = state or new_state()
    for ev in ordered_events(events):
        if st["gen"] is not None and ev["gen"] != st["gen"]:
            _switch_gen(st, ev["gen"])
        elif st["gen"] is None:
            st["gen"] = ev["gen"]
        _expire(st, ev["ts"], before=ev)
        _dispatch(st, ev, declared)
        _grant_loop(st)
        st["processed"] += 1
        st["last_event_id"] = ev["id"]
        st["last_ts"] = ev["ts"]
    return st


def _switch_gen(st: dict[str, Any], gen: str) -> None:
    # The same lock address reused after a process restart is a different lock:
    # all runtime state resets, while observed anomalies/episodes are retained
    # for cross-generation provenance.
    st["gen"] = gen
    st["locks"] = {}
    st["waiters"] = {}
    st["next_waiter"] = 1


# ---------------------------------------------------------------- lock model


def _ensure_lock(st, lock_id, mode, declared):
    lock = st["locks"].get(lock_id)
    if lock is None:
        declared_type = declared.get(lock_id)
        inferred = "rwlock" if mode in ("S", "X") else "mutex"
        ltype = declared_type or inferred
        confidence = "high" if declared_type else ("high" if mode in ("S", "X") else "low")
        lock = {
            "id": lock_id,
            "type": ltype,
            "declared": bool(declared_type),
            "type_confidence": confidence,
            "holders": [],
            "queue": [],
        }
        st["locks"][lock_id] = lock
        if not declared_type and mode == "E":
            _anomaly(
                st, lock_id, "ambiguous_lock_type",
                f"lock {lock_id} seen only with exclusive access; mutex assumed, "
                "recursive is also a legal reconstruction",
                severity="info", confidence="low", event_id=None,
                candidates=[{"lock": lock_id, "declared_type": t}
                            for t in ("mutex", "recursive")],
            )
    elif declared.get(lock_id) and lock["type"] != declared[lock_id]:
        _anomaly(
            st, lock_id, "lock_type_conflict",
            f"declared type {declared[lock_id]} conflicts with inferred {lock['type']}",
            severity="warning", confidence="medium", event_id=None,
        )
    return lock


def _can_grant(lock: dict, w: dict) -> bool:
    holders = lock["holders"]
    if not holders:
        return True
    own = [h for h in holders if h["tid"] == w["tid"]]
    if own:
        if len(holders) != len(own):
            return False
        if lock["type"] == "recursive":
            return w["mode"] == "E"
        if lock["type"] == "rwlock":
            if w["mode"] == "S" and own[0]["mode"] == "S":
                return True
            if w["mode"] == "X" and len(holders) == 1 and own[0] == holders[0]:
                # Shared -> exclusive upgrade while it is the only reader.
                return own[0]["mode"] == "S"
            return False
        # An exclusive lock re-entered by its owner behaves recursively.
        return True
    if lock["type"] == "rwlock" and w["mode"] == "S":
        return all(h["mode"] == "S" for h in holders)
    return False


def _grant(st, lock, w, ok_event=None) -> None:
    existing = next((h for h in lock["holders"] if h["tid"] == w["tid"]), None)
    if existing is not None:
        existing["count"] += 1
        if lock["type"] == "rwlock" and w["mode"] == "X" and existing["mode"] == "S":
            existing["mode"] = "X"
            existing["upgraded"] = True
        existing["reentrant"] = existing["count"] > 1 or existing.get("upgraded")
        if ok_event is not None:
            existing["acquire_event_id"] = ok_event["id"]
            existing["stack"] = ok_event.get("stack") or existing.get("stack")
            existing["confidence"] = _conf(existing.get("confidence"), "high")
    else:
        lock["holders"].append({
            "tid": w["tid"],
            "mode": w["mode"],
            "count": 1,
            "acquire_event_id": (ok_event or {}).get("id", w["event_id"]),
            "stack": (ok_event or {}).get("stack") or w.get("stack"),
            "confidence": "high" if ok_event is not None else w.get("confidence", "medium"),
            "ep": w["ep"],
            "abandoned": False,
            "upgraded": False,
        })
    if w["wid"] in lock["queue"]:
        lock["queue"].remove(w["wid"])
    w["status"] = "granted"
    w["granted_event_id"] = (ok_event or {}).get("id")


def _grant_loop(st) -> None:
    progress = True
    while progress:
        progress = False
        for lock in list(st["locks"].values()):
            # Strict FIFO: a blocked head waiter blocks everybody behind it.
            for wid in list(lock["queue"]):
                w = st["waiters"].get(wid)
                if w is None or w["status"] != "waiting":
                    if wid in lock["queue"]:
                        lock["queue"].remove(wid)
                    continue
                if _can_grant(lock, w):
                    _grant(st, lock, w)
                    progress = True
                break


def _expire(st, ts: int, before: dict | None = None) -> None:
    for w in list(st["waiters"].values()):
        if w["status"] in ("cond_wait", "waiting") and w.get("deadline") is not None:
            # A notify at the same timestamp (earlier seq) is dispatched first,
            # so expiry uses a strict comparison.
            if w["deadline"] < ts:
                kind = "condition timeout" if w["status"] == "cond_wait" else "acquire timeout"
                w["status"] = "timeout"
                lock = st["locks"].get(w["lock"])
                if lock and w["wid"] in lock["queue"]:
                    lock["queue"].remove(w["wid"])
                _anomaly(
                    st, w["lock"], "wait_timeout",
                    f"thread {w['tid']} timed out waiting for lock {w['lock']} ({kind})",
                    severity="info", confidence="high",
                    event_id=(before or {}).get("id"), tid=w["tid"],
                )


def _anomaly(st, lock_id, code, message, *, severity="warning",
             confidence="medium", event_id=None, tid=None, candidates=None):
    st["anomalies"].append({
        "gen": st["gen"],
        "code": code,
        "lock": lock_id,
        "thread": tid,
        "severity": severity,
        "confidence": confidence,
        "event_id": event_id,
        "message": message,
        "candidates": candidates or [],
    })


def _conf(a: str | None, b: str) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    return a if order.get(a or "high", 2) <= order[b] else b


# --------------------------------------------------------------- event handling


def _new_episode(st, ev, lock_id) -> int:
    ep = st["next_ep"]
    st["next_ep"] += 1
    st["ep_by_event"][ev["id"]] = ep
    return ep


def _dispatch(st, ev, declared) -> None:
    kind = ev["kind"]
    fn = {
        "thread_start": _thread_start,
        "thread_end": _thread_end,
        "acquire": _acquire,
        "acquire_ok": _acquire_ok,
        "acquire_cancel": _acquire_cancel,
        "release": _release,
        "cond_wait": _cond_wait,
        "cond_notify": _cond_notify,
    }.get(kind)
    if fn is None:
        _anomaly(st, ev.get("lock"), "unknown_event",
                 f"unknown event kind {kind}", severity="error",
                 confidence="high", event_id=ev["id"])
        return
    fn(st, ev, declared)


def _thread_start(st, ev, declared) -> None:
    tid = ev["thread"]
    if tid in st["threads"]:
        _anomaly(st, None, "thread_restart",
                 f"thread {tid} started more than once in a generation",
                 event_id=ev["id"], tid=tid)
    st["threads"][tid] = {
        "id": tid,
        "alive": True,
        "start_event_id": ev["id"],
        "end_event_id": None,
    }


def _thread_end(st, ev, declared) -> None:
    tid = ev["thread"]
    th = st["threads"].setdefault(tid, {"id": tid, "alive": False})
    th["alive"] = False
    th["end_event_id"] = ev["id"]
    # Thread termination revokes every still-pending wait.
    for w in st["waiters"].values():
        if w["tid"] == tid and w["status"] in ("waiting", "cond_wait"):
            w["status"] = "thread_exit"
            lock = st["locks"].get(w["lock"])
            if lock and w["wid"] in lock["queue"]:
                lock["queue"].remove(w["wid"])
    # Locks still held by a dead thread are not released by the engine; they
    # become abandoned evidence (a real failure, never a synthetic release).
    for lock in st["locks"].values():
        for h in lock["holders"]:
            if h["tid"] == tid and not h["abandoned"]:
                h["abandoned"] = True
                _anomaly(
                    st, lock["id"], "lock_abandoned",
                    f"thread {tid} ended while holding lock {lock['id']}",
                    severity="error", confidence="high",
                    event_id=ev["id"], tid=tid,
                )


def _acquire(st, ev, declared) -> None:
    tid, lock_id, mode = ev["thread"], ev["lock"], ev.get("mode", "E")
    lock = _ensure_lock(st, lock_id, mode, declared)
    if lock["type"] == "recursive" and mode != "E":
        _anomaly(st, lock_id, "mode_conflict",
                 "shared mode requested on a recursive mutex",
                 event_id=ev["id"], tid=tid)
    th = st["threads"].setdefault(tid, {"id": tid, "alive": True})
    th["alive"] = True
    ep = _new_episode(st, ev, lock_id)
    wid = st["next_waiter"]
    st["next_waiter"] += 1
    timeout_ms = ev.get("timeout_ms")
    deadline = ev["ts"] + timeout_ms if timeout_ms is not None else None
    w = {
        "wid": wid,
        "tid": tid,
        "lock": lock_id,
        "mode": "X" if mode == "E" else "S",
        "request_event_id": ev["id"],
        "event_id": ev["id"],
        "stack": ev.get("stack"),
        "ep": ep,
        "status": "waiting",
        "deadline": deadline,
        "origin": "acquire",
        "confidence": "high",
    }
    st["waiters"][wid] = w
    lock["queue"].append(wid)


def _find_waiter(st, ev, *, cond=False):
    tid, lock_id = ev["thread"], ev["lock"]
    statuses = ("cond_wait",) if cond else ("waiting",)
    candidates = [
        w for w in st["waiters"].values()
        if w["tid"] == tid and w["lock"] == lock_id and w["status"] in statuses
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda w: (w["request_event_id"], w["wid"]))


def _acquire_ok(st, ev, declared) -> None:
    tid, lock_id = ev["thread"], ev["lock"]
    mode = ev.get("mode") or "E"
    lock = _ensure_lock(st, lock_id, mode, declared)
    w = _find_waiter(st, ev)
    if w is None:
        # Success without a known request: usually an out-of-order or dropped
        # acquire. Record the holder but keep the edge low-confidence instead
        # of inventing a request.
        ep = _new_episode(st, ev, lock_id)
        w = {
            "wid": st["next_waiter"],
            "tid": tid, "lock": lock_id,
            "mode": "X" if mode == "E" else "S",
            "request_event_id": None, "event_id": ev["id"],
            "stack": ev.get("stack"), "ep": ep,
            "status": "waiting", "deadline": None,
            "origin": "inferred_request", "confidence": "low",
        }
        st["next_waiter"] += 1
        st["waiters"][w["wid"]] = w
        _anomaly(
            st, lock_id, "acquire_without_request",
            f"thread {tid} acquired lock {lock_id} with no pending request",
            severity="warning", confidence="low",
            event_id=ev["id"], tid=tid,
            candidates=[{"kind": "insert_acquire", "after": None, "event": {
                "kind": "acquire", "thread": tid, "lock": lock_id,
                "mode": mode, "ts": ev["ts"], "gen": ev["gen"]}}],
        )
    if w["wid"] not in lock["queue"]:
        lock["queue"].append(w["wid"])
    if w.get("request_event_id") is not None:
        st["links"].append((w["request_event_id"], ev["id"], "request-acquire"))
    _grant(st, lock, w, ok_event=ev)


def _acquire_cancel(st, ev, declared) -> None:
    tid, lock_id = ev["thread"], ev["lock"]
    w = _find_waiter(st, ev)
    if w is None:
        _anomaly(st, lock_id, "cancel_without_wait",
                 f"cancel by {tid} for {lock_id} matches no pending request",
                 event_id=ev["id"], tid=tid)
        return
    w["status"] = "cancelled"
    lock = st["locks"].get(lock_id)
    if lock and w["wid"] in lock["queue"]:
        lock["queue"].remove(w["wid"])
    w["cancel_event_id"] = ev["id"]


def _release(st, ev, declared) -> None:
    tid, lock_id = ev["thread"], ev["lock"]
    lock = st["locks"].get(lock_id)
    holder = None
    if lock:
        holder = next((h for h in lock["holders"] if h["tid"] == tid), None)
    if holder is None:
        _anomaly(
            st, lock_id, "release_without_hold",
            f"thread {tid} released lock {lock_id} it did not hold",
            severity="error", confidence="high",
            event_id=ev["id"], tid=tid,
            candidates=[{"kind": "missing_acquire", "thread": tid,
                         "lock": lock_id, "gen": ev["gen"], "ts": ev["ts"]}],
        )
        # Never fabricate an acquire/hold; nothing stateful is changed.
        return
    holder["count"] -= 1
    holder["release_confidence"] = "high"
    if holder["count"] <= 0:
        st["ep_by_event"].setdefault(ev["id"], holder.get("ep"))
        st["links"].append((holder["acquire_event_id"], ev["id"], "acquire-release"))
        lock["holders"].remove(holder)


def _cond_wait(st, ev, declared) -> None:
    tid, lock_id, cond = ev["thread"], ev["lock"], ev.get("cond", "?")
    lock = st["locks"].get(lock_id)
    holder = next((h for h in (lock["holders"] if lock else [])
                   if h["tid"] == tid), None)
    if holder is None:
        _anomaly(st, lock_id, "condwait_without_hold",
                 f"thread {tid} waited on {cond} without holding {lock_id}",
                 event_id=ev["id"], tid=tid)
        return
    # cond_wait atomically drops the held lock ...
    released_count = holder["count"]
    released_mode = holder["mode"]
    lock["holders"].remove(holder)
    st["ep_by_event"].setdefault(ev["id"], holder.get("ep"))
    st["links"].append((holder["acquire_event_id"], ev["id"], "acquire-condwait"))
    # ... and parks the thread on the condition.
    ep = _new_episode(st, ev, lock_id)
    wid = st["next_waiter"]
    st["next_waiter"] += 1
    timeout_ms = ev.get("timeout_ms")
    w = {
        "wid": wid, "tid": tid, "lock": lock_id,
        "mode": released_mode,
        "request_event_id": ev["id"], "event_id": ev["id"],
        "stack": ev.get("stack"), "ep": ep,
        "status": "cond_wait", "deadline": (ev["ts"] + timeout_ms
                                            if timeout_ms is not None else None),
        "origin": "cond_wait", "cond": cond,
        "resume_count": released_count,
        "confidence": "high",
    }
    st["waiters"][wid] = w


def _cond_notify(st, ev, declared) -> None:
    cond = ev.get("cond", "?")
    waiters = [w for w in st["waiters"].values()
               if w.get("cond") == cond and w["status"] == "cond_wait"]
    waiters.sort(key=lambda w: (w["request_event_id"], w["wid"]))
    all_waiters = ev.get("all", False)
    chosen = waiters if all_waiters else waiters[:1]
    for w in chosen:
        # Wake from the condition and re-enter lock contention. FIFO order is
        # keyed by the original cond_wait timestamp.
        w["status"] = "waiting"
        w["origin"] = "cond_reacquire"
        w["notify_event_id"] = ev["id"]
        st["links"].append((ev["id"], w["request_event_id"], "notify-waiter"))
        lock = st["locks"].get(w["lock"])
        if lock is not None and w["wid"] not in lock["queue"]:
            # cond_wait precedes later acquire requests; insert at queue front
            # in request order, preserving FIFO fairness.
            lock["queue"].insert(0, w["wid"])


# ------------------------------------------------------------- graph analysis


def blocked_edges(st: dict[str, Any]) -> list[dict]:
    """Wait-for edges derived from the current runtime state.

    Every edge carries the event ids that produced it so the UI can jump from
    a graph edge straight to the acquire/notify/hold evidence.
    """
    edges: list[dict] = []
    for lock in st["locks"].values():
        holders = [h for h in lock["holders"] if not h.get("abandoned")]
        queue = [st["waiters"][wid] for wid in lock["queue"]
                 if st["waiters"].get(wid) and st["waiters"][wid]["status"] == "waiting"]
        for pos, w in enumerate(queue):
            blockers: list[dict] = []
            # Earlier FIFO waiters block this waiter even without holders.
            ahead = queue[:pos]
            for other in ahead:
                blockers.append({
                    "tid": other["tid"],
                    "kind": "queue_ahead",
                    "event_id": other["request_event_id"],
                })
            blocked_by_holders = not _can_grant(lock, w)
            for h in holders:
                if h["tid"] == w["tid"] and not blocked_by_holders:
                    # Reentrant/upgrade acquisition by the owner: not a wait.
                    continue
                if h["tid"] == w["tid"] and not ahead:
                    continue
                blockers.append({
                    "tid": h["tid"],
                    "kind": "holder",
                    "event_id": h["acquire_event_id"],
                    "confidence": h.get("confidence", "medium"),
                })
            for b in blockers:
                if b["tid"] == w["tid"]:
                    continue
                edges.append({
                    "from_thread": w["tid"],
                    "to_thread": b["tid"],
                    "lock": lock["id"],
                    "mode": w["mode"],
                    "wait_event_id": w["request_event_id"],
                    "wait_stack": w.get("stack"),
                    "block_event_id": b["event_id"],
                    "via": b["kind"],
                    "origin": w.get("origin", "acquire"),
                    "cond": w.get("cond"),
                    "notify_event_id": w.get("notify_event_id"),
                    "confidence": _conf(w.get("confidence", "high"),
                                        b.get("confidence", "high")),
                })
    return edges


def cond_edges(st: dict[str, Any]) -> list[dict]:
    out = []
    for w in st["waiters"].values():
        if w["status"] == "cond_wait":
            out.append({
                "from_thread": w["tid"],
                "cond": w.get("cond"),
                "lock": w["lock"],
                "mode": w["mode"],
                "wait_event_id": w["request_event_id"],
                "wait_stack": w.get("stack"),
                "deadline": w.get("deadline"),
                "confidence": w.get("confidence", "high"),
            })
    return out


def detect_cycles(edges: list[dict]) -> list[dict]:
    """Enumerate simple cycles of the thread wait-for graph.

    Each returned cycle carries, per step, the waiting thread, target thread,
    lock and both stacks, so a deadlock report is concrete evidence rather
    than a bare list of lock names.
    """
    adj: dict[str, list[dict]] = defaultdict(list)
    nodes: set[str] = set()
    for e in edges:
        adj[e["from_thread"]].append(e)
        nodes.add(e["from_thread"])
        nodes.add(e["to_thread"])

    seen: set[frozenset] = set()
    cycles: list[dict] = []
    color: dict[str, int] = {}

    def dfs(start: str, node: str, path: list[dict], visited_edges: set):
        for e in adj.get(node, []):
            ekey = (e["from_thread"], e["to_thread"], e["lock"], e["wait_event_id"])
            if ekey in visited_edges:
                continue
            nxt = e["to_thread"]
            if nxt == start and path:
                steps = path + [e]
                fp = _cycle_fingerprint(steps)
                if fp in seen:
                    continue
                seen.add(fp)
                cycles.append(_cycle_report(steps))
                continue
            if color.get(nxt, 0) == 1:
                continue
            color[nxt] = 1
            visited_edges.add(ekey)
            dfs(start, nxt, path + [e], visited_edges)
            visited_edges.discard(ekey)
            color[nxt] = 0

    for n in sorted(nodes):
        color = {n: 1}
        dfs(n, n, [], set())
    cycles.sort(key=lambda c: (len(c["steps"]), c["fingerprint"]))
    return cycles


def _cycle_fingerprint(steps: list[dict]) -> str:
    parts = sorted(
        f"{s['from_thread']}->{s['to_thread']}@{s['lock']}#{s['wait_event_id']}"
        for s in steps
    )
    return "|".join(parts)


def _cycle_report(steps: list[dict]) -> dict:
    return {
        "fingerprint": _cycle_fingerprint(steps),
        "threads": [s["from_thread"] for s in steps],
        "locks": [s["lock"] for s in steps],
        "steps": [
            {
                "thread": s["from_thread"],
                "waits_for_thread": s["to_thread"],
                "lock": s["lock"],
                "wait_mode": s["mode"],
                "wait_event_id": s["wait_event_id"],
                "wait_stack": s.get("wait_stack"),
                "hold_event_id": s["block_event_id"],
                "via": s["via"],
                "origin": s.get("origin"),
                "cond": s.get("cond"),
                "notify_event_id": s.get("notify_event_id"),
                "confidence": s.get("confidence", "high"),
            }
            for s in steps
        ],
    }


def hold_view(st: dict[str, Any]) -> list[dict]:
    out = []
    for lock in st["locks"].values():
        for h in lock["holders"]:
            out.append({
                "lock": lock["id"],
                "lock_type": lock["type"],
                "thread": h["tid"],
                "mode": h["mode"],
                "count": h["count"],
                "reentrant": bool(h.get("reentrant")),
                "upgraded": bool(h.get("upgraded")),
                "abandoned": bool(h.get("abandoned")),
                "event_id": h["acquire_event_id"],
                "stack": h.get("stack"),
                "confidence": h.get("confidence", "medium"),
            })
    return out


def lock_type_view(st: dict[str, Any]) -> list[dict]:
    return [
        {
            "lock": lock["id"],
            "type": lock["type"],
            "declared": lock["declared"],
            "confidence": lock["type_confidence"],
        }
        for lock in st["locks"].values()
    ]


# ------------------------------------------------------------------ snapshots


def events_through(events: list[dict], ts: int | None,
                   event_id: int | None = None) -> list[dict]:
    ordered = ordered_events(events)
    if event_id is not None:
        return [e for e in ordered
                if (e["seq"], e["id"]) <= _id_key(ordered, event_id)]
    if ts is None:
        return ordered
    return [e for e in ordered if e["ts"] <= ts]


def _id_key(ordered: list[dict], event_id: int):
    for e in ordered:
        if e["id"] == event_id:
            return e["seq"], e["id"]
    return 10**18, event_id


def analyze(events: list[dict], declared_types: dict[str, str] | None = None,
            ts: int | None = None, event_id: int | None = None,
            state: dict[str, Any] | None = None) -> dict:
    window = events_through(events, ts, event_id)
    st = replay(window, declared_types, state)
    edges = blocked_edges(st)
    conds = cond_edges(st)
    cycles = detect_cycles(edges)
    return {
        "state": st,
        "hold_graph": hold_view(st),
        "wait_graph": edges,
        "cond_graph": conds,
        "cycles": cycles,
        "anomalies": st["anomalies"],
        "lock_types": lock_type_view(st),
        "events_included": len(window),
        "cutoff_event_id": window[-1]["id"] if window else None,
        "cutoff_ts": window[-1]["ts"] if window else None,
    }


def has_cycle(report: dict, fingerprint: str | None = None) -> bool:
    if not report["cycles"]:
        return False
    if fingerprint is None:
        return True
    return any(c["fingerprint"] == fingerprint for c in report["cycles"])


# ---------------------------------------------------------- minimal reduction


class _DSU:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _pair_components(events: list[dict]) -> dict[int, int]:
    """Group events that must coexist for per-thread / condition semantics.

    Pairing units come straight from a replay: acquire->success,
    acquire->release, cond_wait with its preceding acquire and notifying
    event all share one component, so reduction never splits a pairing.
    """
    dsu = _DSU([e["id"] for e in events])
    st = replay(events)
    for a, b, _kind in st["links"]:
        if a is not None and a in dsu.parent and b in dsu.parent:
            dsu.union(a, b)
    return {eid: dsu.find(eid) for eid in dsu.parent}


# ---------------------------------------------------------- minimal reduction


class _DSU:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _pair_components(events: list[dict]) -> dict[int, int]:
    """Group events that must coexist for per-thread / condition semantics."""
    dsu = _DSU([e["id"] for e in events])

    # 1) Acquire attempt -> its success/cancel and matched release (per
    #    thread+lock episode) are one indivisible pairing unit.
    active: dict[tuple[str, str], list[int]] = defaultdict(list)
    pending: dict[int, int] = {}  # waiter request event id -> component anchor
    for e in ordered_events(events):
        key = (e["thread"], e.get("lock"))
        if e["kind"] in ("acquire", "cond_wait"):
            anchor = e["id"]
            active[key].append(anchor)
            pending[id(e)] = anchor  # placeholder, replaced below
        elif e["kind"] == "acquire_ok":
            anchor = active[key][-1] if active[key] else e["id"]
            dsu.union(anchor, e["id"])
        elif e["kind"] in ("acquire_cancel",):
            anchor = active[key][-1] if active[key] else e["id"]
            dsu.union(anchor, e["id"])
        elif e["kind"] == "release":
            if active[key]:
                dsu.union(active[key].pop(0), e["id"])
            else:
                dsu.union(e["id"], e["id"])

    # 2) cond_wait must stay with the notification that wakes it (the reacquire
    #    success rides the same episode).
    waiting_on: dict[tuple[str, str | None], int] = {}
    for e in ordered_events(events):
        if e["kind"] == "cond_wait":
            waiting_on[(e["thread"], e.get("cond"))] = e["id"]
        elif e["kind"] == "cond_notify":
            waiters = [
                w for w in events
                if w["kind"] == "cond_wait"
                and w.get("cond") == e.get("cond", "?")
            ]
            # Not precise enough to attribute a single waiter statically; the
            # engine's links capture the real pairing.
            for w in waiters:
                dsu.union(e["id"], w["id"])

    # 3) Use engine replay links for exact condition pairing.
    st = replay(events)
    for a, b, _kind in st["links"]:
        if a is not None:
            dsu.union(a, b)

    return {eid: dsu.find(eid) for eid in dsu.parent}


def reduce_events(
    events: list[dict],
    declared_types: dict[str, str] | None = None,
    fingerprint: str | None = None,
    ts: int | None = None,
    event_id: int | None = None,
) -> dict:
    """Return the smallest subsequence still reproducing the target cycle.

    Removal works on pairing components so acquires never lose their releases
    and cond_wait/notify semantics stay intact.
    """
    window = events_through(events, ts, event_id)
    baseline = analyze(window, declared_types)
    if not baseline["cycles"]:
        return {
            "reduced": [],
            "fingerprint": None,
            "original_count": len(window),
            "removed": [],
            "reason": "no_cycle",
        }
    target = fingerprint or baseline["cycles"][0]["fingerprint"]
    components = _pair_components(window)
    groups: dict[int, list[dict]] = defaultdict(list)
    for e in window:
        groups[components[e["id"]]].append(e)

    kept = window
    changed = True
    while changed:
        changed = False
        kept_components = {components[e["id"]] for e in kept}
        # Removing larger components first converges faster.
        for comp in sorted(kept_components,
                           key=lambda c: -len(groups[c])):
            trial = [e for e in kept if components[e["id"]] != comp]
            report = analyze(trial, declared_types)
            if any(c["fingerprint"] == target for c in report["cycles"]):
                kept = trial
                changed = True
                break

    kept_ids = {e["id"] for e in kept}
    reduced = [dict(e) for e in ordered_events(kept)]
    return {
        "reduced": reduced,
        "fingerprint": target,
        "original_count": len(window),
        "removed_count": len(window) - len(kept),
        "removed": [e["id"] for e in window if e["id"] not in kept_ids],
        "reason": "ok",
    }
