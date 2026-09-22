"""Lock-order reconstruction engine.

Replays lock/thread/condition events in *collected (timestamp) order* while
keeping the original *arrival order* visible, and builds candidate ownership
(hold) and waiting graphs with explicit confidence. The engine never invents
releases: missing events become ``inferred``/``tentative`` edges plus recorded
issues instead.
"""
from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

MUTEX = "mutex"
RWLOCK = "rwlock"
NONREENTRANT = "nonreentrant"
LOCK_TYPES = (MUTEX, RWLOCK, NONREENTRANT)

# Confidence levels (ordered worst -> best).
TENTATIVE = "tentative"
INFERRED = "inferred"
OBSERVED = "observed"
_LEVEL = {OBSERVED: 0, INFERRED: 1, TENTATIVE: 2}


def worse(a: str, b: str) -> str:
    return a if _LEVEL[a] >= _LEVEL[b] else b


def normalize_event(e: Dict[str, Any]) -> Dict[str, Any]:
    """Return a normalized copy of a raw event dict."""
    ev = dict(e)
    ev.setdefault("gen", "default")
    ev["type"] = str(ev["type"])
    for k in ("thread", "lock", "mode", "cond", "stack", "stack_acq", "ts"):
        ev.setdefault(k, None)
    if ev.get("stack") is None:
        ev["stack"] = []
    if not isinstance(ev["stack"], list):
        ev["stack"] = [ev["stack"]]
    t = ev["type"]
    if t in ("request", "acquired", "release", "cond_wait",
             "cond_notify", "cond_notify_all", "cond_timedwait") and not ev.get("lock"):
        raise ValueError(f"event {t!r} requires a lock: {ev}")
    if t in ("request", "acquired") and ev.get("mode") is None:
        ev["mode"] = "X" if ev.get("lock_type") == MUTEX or ev.get("lock_type") == NONREENTRANT else "S"
    if t in ("cond_wait", "cond_timedwait") and ev.get("mode") is None:
        ev["mode"] = "X"
    if t == "release":
        ev["mode"] = None
    if ev.get("ts") is None:
        ev["ts"] = 0
    ev["ts"] = float(ev["ts"])
    ev.setdefault("arrival", ev["ts"])
    ev["arrival"] = float(ev["arrival"])
    return ev


@dataclass
class Req:
    mode: str
    ev: Dict[str, Any]
    ticket: Optional[Dict[str, Any]] = None  # saved state for cond reacquire


@dataclass
class _State:
    alive: Dict[str, bool] = field(default_factory=dict)
    # lock -> {"type", "holders": {thread: {"mode","count","acq","rel"}}, "order": [thread..]}
    locks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pending: Dict[str, List[Req]] = field(default_factory=lambda: defaultdict(list))
    # cond -> list of (thread, lock, mode, ev)
    cond_waiters: Dict[str, List[Tuple[str, str, str, Dict[str, Any]]]] = field(default_factory=lambda: defaultdict(list))
    issues: List[Dict[str, Any]] = field(default_factory=list)
    replayed: int = 0


class Engine:
    """Replay a normalized event stream and answer snapshots at points in time."""

    def __init__(self, events: List[Dict[str, Any]],
                 lock_types: Optional[Dict[str, str]] = None,
                 gen: str = "default"):
        self.raw = [normalize_event(e) for e in events]
        self.lock_types = dict(lock_types or {})
        self.gen = gen
        for e in self.raw:
            if e.get("lock_type") and e.get("lock") and e["lock"] not in self.lock_types:
                self.lock_types[e["lock"]] = e["lock_type"]
        self.arrival_order = sorted(range(len(self.raw)),
                                   key=lambda i: (self.raw[i]["arrival"], i))
        self.order = sorted(range(len(self.raw)),
                            key=lambda i: (self.raw[i]["ts"], i))
        self.states: List[_State] = []
        self._checkpoints: List[Tuple[int, _State]] = []
        self._build()

    # ------------------------------------------------------------------ build
    def _type_of(self, st: _State, lock: str, ev: Dict[str, Any]) -> str:
        lt = self.lock_types.get(lock) or ev.get("lock_type") or MUTEX
        return lt

    def _ensure_lock(self, st: _State, lock: str, lt: str) -> Dict[str, Any]:
        lk = st.locks.get(lock)
        if lk is None:
            lk = {"type": lt, "holders": {}, "order": []}
            st.locks[lock] = lk
        return lk

    def _issue(self, st: _State, kind: str, ev: Dict[str, Any], detail: str) -> None:
        st.issues.append({"kind": kind, "detail": detail,
                          "ts": ev["ts"], "thread": ev.get("thread"),
                          "lock": ev.get("lock"), "event_index": ev.get("_idx"),
                          "confidence": TENTATIVE if kind in (
                              "unmatched_release", "unmatched_acquired",
                              "unmatched_notify", "thread_ended_holding",
                              "unknown_thread") else INFERRED})

    def _check_thread(self, st: _State, ev: Dict[str, Any]) -> None:
        th = ev.get("thread")
        if th and not st.alive.get(th, False):
            st.alive[th] = True
            self._issue(st, "unknown_thread", ev,
                        f"thread {th} used without thread_start; inferred alive")

    def _grantable(self, lk: Dict[str, Any], mode: str) -> bool:
        holders = lk["holders"]
        if not holders:
            return True
        if mode == "S":
            return all(h["mode"] == "S" for h in holders.values())
        return False

    def _conflicts(self, holder_thread: str, hmode: str,
                   req_thread: str, mode: str, ltype: str) -> bool:
        if holder_thread == req_thread:
            if ltype == MUTEX:
                return False
            if ltype == NONREENTRANT:
                return True
            # rwlock: re-entrant S is fine; everything else (S->X upgrade,
            # X->X, X->S) conflicts and can deadlock on upgrade.
            if hmode == "S" and mode == "S":
                return False
            return True
        if mode == "S":
            return hmode == "X"
        return True  # waiting for X conflicts with any other holder

    def _add_hold(self, lk: Dict[str, Any], th: str, mode: str,
                  acq: Dict[str, Any], rel: Optional[Dict[str, Any]]) -> None:
        h = lk["holders"].get(th)
        if h:
            h["count"] += 1
            if mode == "X":
                h["mode"] = "X"
            h["acq"].append(acq)
            if rel is not None:
                h["rel"].append(rel)
        else:
            lk["holders"][th] = {"mode": mode, "count": 1,
                                 "acq": [acq], "rel": [] if rel is None else [rel]}
            lk["order"].append(th)

    def _release_one(self, st: _State, lk: Dict[str, Any], th: str,
                     rel: Dict[str, Any]) -> bool:
        h = lk["holders"].get(th)
        if not h:
            return False
        h["count"] -= 1
        h["rel"].append(rel)
        if h["count"] <= 0:
            del lk["holders"][th]
            lk["order"].remove(th)
        return True

    def _drop_pending(self, st: _State, th: Optional[str] = None,
                      lock: Optional[str] = None, ev: Optional[Dict[str, Any]] = None,
                      kind: str = "cancelled") -> None:
        """Remove matching pending requests (timeout / thread exit)."""
        for lk_name, reqs in list(st.pending.items()):
            kept: List[Req] = []
            for r in reqs:
                if th is not None and r.ev["thread"] != th:
                    kept.append(r)
                elif lock is not None and lk_name != lock:
                    kept.append(r)
                else:
                    # restore ticket -> becomes a stale waiting reacquire? It is
                    # cancelled, so ticket is abandoned (never reacquired).
                    if r.ticket is not None:
                        self._issue(st, "cond_reacquire_lost",
                                    ev or r.ev,
                                    f"{r.ev['thread']} condition reacquire of "
                                    f"{lk_name} never completed ({kind})")
            st.pending[lk_name] = kept

    # ------------------------------------------------------------------ apply
    def _save_ticket(self, st: _State, th: str, lock: str) -> Optional[Dict[str, Any]]:
        lk = st.locks.get(lock)
        h = lk["holders"].get(th) if lk else None
        if not h:
            return None
        ticket = {"mode": h["mode"], "count": h["count"],
                  "acq": list(h["acq"]), "rel": list(h["rel"])}
        del lk["holders"][th]
        lk["order"].remove(th)
        return ticket

    def _grant(self, st: _State, lk_name: str, r: Req,
               acq_ev: Dict[str, Any], inferred: bool) -> None:
        lk = self._ensure_lock(st, lk_name, self._type_of(st, lk_name, acq_ev))
        mode = r.mode
        acq = {"event_index": acq_ev["_idx"], "ts": acq_ev["ts"],
               "stack": acq_ev.get("stack") or [], "inferred": inferred}
        if r.ticket is not None:
            # condition wait: reacquire restores the pre-wait nesting level.
            tk = r.ticket
            lk["holders"][acq_ev["thread"]] = {
                "mode": tk["mode"], "count": tk["count"],
                "acq": tk["acq"] + [acq], "rel": list(tk["rel"]),
                "restored_from": tk["acq"]}
            if mode == "X":
                lk["holders"][acq_ev["thread"]]["mode"] = "X"
            lk["order"].append(acq_ev["thread"])
        else:
            h = lk["holders"].get(acq_ev["thread"])
            if h:
                if lk["type"] == RWLOCK and mode == "X":
                    h["mode"] = "X"  # S -> X upgrade
                h["count"] += 1
                h["acq"].append(acq)
            else:
                lk["holders"][acq_ev["thread"]] = {"mode": mode, "count": 1,
                                                   "acq": [acq], "rel": []}
                lk["order"].append(acq_ev["thread"])
        st.pending[lk_name].remove(r)

    def _apply(self, st: _State, ev: Dict[str, Any]) -> None:
        t = ev["type"]
        th = ev.get("thread")
        lk_name = ev.get("lock")
        if t == "thread_start":
            st.alive[th] = True
            return
        if t == "thread_end":
            if st.alive.get(th):
                self._drop_pending(st, th=th, ev=ev, kind="thread_exit")
                for name, lk in st.locks.items():
                    if th in lk["holders"]:
                        self._issue(st, "thread_ended_holding", ev,
                                    f"{th} ended still holding {name}; "
                                    "release not invented")
                st.alive[th] = False
            else:
                self._issue(st, "unknown_thread", ev,
                            f"thread_end for unknown thread {th}")
            return

        self._check_thread(st, ev)
        if t == "request":
            lt = self._type_of(st, lk_name, ev)
            lk = self._ensure_lock(st, lk_name, lt)
            st.pending[lk_name].append(Req(mode=ev["mode"], ev=ev))
            return
        if t == "acquired":
            lt = self._type_of(st, lk_name, ev)
            lk = self._ensure_lock(st, lk_name, lt)
            q = st.pending[lk_name]
            match = next((r for r in q if r.ev["thread"] == th), None)
            if match is None:
                # acquired without request: treat as instant request+grant,
                # but flag it.
                self._issue(st, "unmatched_acquired", ev,
                            f"{th} acquired {lk_name} with no pending request")
                match = Req(mode=ev.get("mode") or "X", ev=ev)
                q.append(match)
            if match.mode != ev.get("mode") and ev.get("mode"):
                match.mode = ev["mode"]
            self._grant(st, lk_name, match, ev, inferred=False)
            return
        if t == "release":
            lk = st.locks.get(lk_name)
            if not lk or not self._release_one(st, lk, th, {
                    "event_index": ev["_idx"], "ts": ev["ts"],
                    "stack": ev.get("stack") or []}):
                self._issue(st, "unmatched_release", ev,
                            f"{th} released {lk_name} it did not hold; "
                            "release not applied")
            return
        if t in ("cond_wait", "cond_timedwait"):
            # atomically release the lock and enqueue on the condition
            cond = ev.get("cond") or lk_name
            lk = st.locks.get(lk_name)
            ticket = self._save_ticket(st, th, lk_name) if lk else None
            if ticket is None:
                self._issue(st, "cond_wait_without_lock", ev,
                            f"{th} cond_wait on {lk_name} without holding it")
                ticket = {"mode": ev.get("mode") or "X", "count": 1,
                          "acq": [], "rel": []}
            st.cond_waiters[cond].append((th, lk_name, ticket["mode"], ev))
            # enqueue reacquire as a pending request carrying the ticket
            st.pending[lk_name].append(
                Req(mode=ticket["mode"], ev=ev, ticket=ticket))
            if t == "cond_timedwait" and ev.get("timeout_release"):
                self._issue(st, "inferred_timedwait", ev,
                            f"{th} cond_timedwait may time out on {lk_name}")
            return
        if t in ("cond_notify", "cond_notify_all"):
            cond = ev.get("cond") or lk_name
            waiters = st.cond_waiters.get(cond, [])
            if not waiters:
                self._issue(st, "unmatched_notify", ev,
                            f"notify on {cond} with no waiters")
                return
            n = 1 if t == "cond_notify" else len(waiters)
            woken, rest = waiters[:n], waiters[n:]
            st.cond_waiters[cond] = rest
            # wakeup = reacquire request becomes active immediately (it was
            # already pending with ticket; FIFO position kept).
            for cth, clock, cmode, wev in woken:
                self._issue(st, "cond_woken", ev,
                            f"{cth} woken from {cond} by {th}", )
            return
        if t == "timeout":
            before = sum(len(v) for v in st.pending.values())
            self._drop_pending(st, th=th, lock=lk_name, ev=ev, kind="timeout")
            after = sum(len(v) for v in st.pending.values())
            if before == after:
                self._issue(st, "timeout_without_wait", ev,
                            f"timeout for {th} on {lk_name} matched no request")
            return
        self._issue(st, "unknown_event", ev, f"unknown event type {t}")

    # ----------------------------------------------------------------- replay
    def _build(self) -> None:
        state = _State()
        for seq, idx in enumerate(self.order):
            ev = self.raw[idx]
            ev["_idx"] = idx
            prev_issues = len(state.issues)
            self._apply(state, ev)
            state.replayed = seq + 1
            self.states.append(copy.deepcopy(state))
        # arrival-order view: which timestamps arrived "late"
        max_seen = -1.0
        self.late: Dict[int, bool] = {}
        for idx in self.arrival_order:
            self.late[idx] = self.raw[idx]["ts"] < max_seen
            max_seen = max(max_seen, self.raw[idx]["ts"])

    # --------------------------------------------------------------- snapshot
    def _hold_edges(self, st: _State) -> List[Dict[str, Any]]:
        edges: List[Dict[str, Any]] = []
        for lk_name in sorted(st.locks):
            lk = st.locks[lk_name]
            for th in lk["order"]:
                h = lk["holders"][th]
                conf = OBSERVED
                refs = [dict(a) for a in h["acq"]]
                if any(a.get("inferred") for a in h["acq"]):
                    conf = INFERRED
                if h.get("restored_from"):
                    refs = refs  # restored ticket acquisitions included
                edges.append({
                    "id": f"hold:{lk_name}->{th}",
                    "kind": "hold", "from": f"lock:{lk_name}",
                    "to": f"thread:{th}", "lock": lk_name, "thread": th,
                    "mode": h["mode"], "count": h["count"],
                    "shared": h["mode"] == "S",
                    "confidence": conf,
                    "acq_stack": h["acq"][-1]["stack"] if h["acq"] else [],
                    "evidence": refs,
                })
        return edges

    def _wait_edges(self, st: _State) -> List[Dict[str, Any]]:
        edges: List[Dict[str, Any]] = []
        for lk_name in sorted(st.pending):
            lk = st.locks.get(lk_name)
            for pos, r in enumerate(st.pending[lk_name]):
                th = r.ev["thread"]
                if lk is None or not lk["holders"]:
                    edges.append(self._wait_edge(lk_name, r, pos, [],
                                                 OBSERVED, "free_but_ungranted"))
                    continue
                conflicting: List[Dict[str, Any]] = []
                for hth in lk["order"]:
                    h = lk["holders"][hth]
                    if self._conflicts(hth, h["mode"], th, r.mode, lk["type"]):
                        conflicting.append({
                            "thread": hth, "mode": h["mode"],
                            "acq_stack": h["acq"][-1]["stack"] if h["acq"] else [],
                            "acq_event_index": h["acq"][-1]["event_index"] if h["acq"] else None,
                        })
                if not conflicting:
                    edges.append(self._wait_edge(lk_name, r, pos, [],
                                                 OBSERVED, "reentrant_ok"))
                else:
                    conf = TENTATIVE if self.late.get(r.ev["_idx"]) else OBSERVED
                    edges.append(self._wait_edge(lk_name, r, pos,
                                                 conflicting, conf, "blocked"))
        return edges

    def _wait_edge(self, lk_name: str, r: Req, pos: int,
                   blockers: List[Dict[str, Any]], conf: str,
                   reason: str) -> Dict[str, Any]:
        return {
            "id": f"wait:{r.ev['thread']}->{lk_name}:{pos}",
            "kind": "wait", "from": f"thread:{r.ev['thread']}",
            "to": f"lock:{lk_name}", "lock": lk_name,
            "thread": r.ev["thread"], "mode": r.mode, "queue_pos": pos,
            "confidence": conf, "reason": reason,
            "wait_stack": r.ev.get("stack") or [],
            "request_event_index": r.ev["_idx"],
            "condition_wait": r.ticket is not None,
            "blocked_by": blockers,
            "evidence": [{"event_index": r.ev["_idx"], "ts": r.ev["ts"],
                          "stack": r.ev.get("stack") or []}],
        }

    @staticmethod
    def _find_cycles(hold: List[Dict[str, Any]],
                     wait: List[Dict[str, Any]]) -> List[List[Dict[str, str]]]:
        """Cycle search on the resource-allocation graph.

        thread -wait-> lock -hold-> thread ... Returns paths of edges
        (alternating wait/hold) forming a directed cycle.
        """
        out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for e in wait:
            if e.get("blocked_by"):
                # direct edges only to the blocking holders
                for b in e["blocked_by"]:
                    out[e["from"]].append({"to": f"thread:{b['thread']}",
                                           "via": e, "blocker": b})
            else:
                out[e["from"]].append({"to": e["to"], "via": e, "blocker": None})
        for e in hold:
            out[e["from"]].append({"to": e["to"], "via": e, "blocker": None})

        cycles: List[Tuple[str, ...]] = []
        seen = set()

        def dfs(start: str, node: str, path_nodes: List[str],
                path_edges: List[Dict[str, Any]]) -> None:
            for step in out.get(node, []):
                nxt = step["to"]
                if nxt == start and len(path_nodes) >= 2:
                    sig = tuple(path_nodes)
                    rotations = [sig[i:] + sig[:i] for i in range(len(sig))]
                    key = tuple(sorted(rotations))
                    if key not in seen:
                        seen.add(key)
                        cycles.append(list(path_edges + [step]))
                elif nxt not in path_nodes:
                    dfs(start, nxt, path_nodes + [nxt], path_edges + [step])

        nodes = set(out)
        for n in sorted(nodes):
            dfs(n, n, [n], [])
        return cycles

    def _describe_cycles(self, st: _State,
                         hold: List[Dict[str, Any]],
                         wait: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        raw_cycles = self._find_cycles(hold, wait)
        result = []
        for ci, edges in enumerate(raw_cycles):
            threads, locks, chain, conf = [], [], [], OBSERVED
            for step in edges:
                e = step["via"]
                conf = worse(conf, e["confidence"])
                if e["kind"] == "wait":
                    if e["thread"] not in threads:
                        threads.append(e["thread"])
                    if e["lock"] not in locks:
                        locks.append(e["lock"])
                    blocker = step.get("blocker") or (e["blocked_by"][0] if e.get("blocked_by") else None)
                    chain.append({
                        "kind": "wait", "thread": e["thread"], "lock": e["lock"],
                        "mode": e["mode"], "wait_stack": e["wait_stack"],
                        "request_event_index": e["request_event_index"],
                        "condition_wait": e["condition_wait"],
                        "held_by": blocker["thread"] if blocker else None,
                        "hold_stack": blocker["acq_stack"] if blocker else None,
                        "hold_event_index": blocker["acq_event_index"] if blocker else None,
                        "confidence": e["confidence"],
                    })
                else:
                    chain.append({
                        "kind": "hold", "lock": e["lock"], "thread": e["thread"],
                        "mode": e["mode"], "acq_stack": e["acq_stack"],
                        "confidence": e["confidence"],
                    })
            result.append({
                "id": "cycle:" + ">".join(
                    f"{c['thread']}@{c['lock']}" for c in chain if c["kind"] == "wait"),
                "threads": threads, "locks": locks, "chain": chain,
                "confidence": conf,
            })
        return result

    def snapshot(self, index: Optional[int] = None) -> Dict[str, Any]:
        """State after ``index`` timestamp-ordered events (1-based)."""
        if not self.states:
            st = _State()
        else:
            index = len(self.states) if index is None else max(0, min(index, len(self.states)))
            st = self.states[index - 1] if index > 0 else _State()
        hold = self._hold_edges(st)
        wait = self._wait_edges(st)
        cycles = self._describe_cycles(st, hold, wait)
        lock_nodes = [{"id": f"lock:{n}", "lock": n, "type": lk["type"],
                       "holders": list(lk["order"]),
                       "waiters": [r.ev["thread"] for r in st.pending.get(n, [])]}
                      for n, lk in st.locks.items()]
        return {
            "index": 0 if not self.states else (len(self.states) if index is None else index),
            "total": len(self.states),
            "threads": [{"id": f"thread:{t}", "thread": t, "alive": a}
                        for t, a in sorted(st.alive.items())],
            "locks": lock_nodes,
            "hold_edges": hold,
            "wait_edges": wait,
            "cycles": cycles,
            "issues": list(st.issues),
            "cond_waiters": {c: [{"thread": w[0], "lock": w[1], "mode": w[2],
                                  "event_index": w[3]["_idx"]} for w in ws]
                             for c, ws in st.cond_waiters.items() if ws},
        }

    def timeline(self) -> List[Dict[str, Any]]:
        out = []
        for seq, idx in enumerate(self.order):
            e = self.raw[idx]
            out.append({"seq": seq + 1, "event_index": idx,
                        "arrival_index": self.arrival_order.index(idx) + 1,
                        "late": self.late[idx], **{k: e[k] for k in
                        ("type", "thread", "lock", "mode", "cond", "ts",
                         "arrival", "stack", "gen")}})
        return out
