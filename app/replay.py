"""Reconstruction engine: replay ordered trace events into hold/wait graphs.

The engine never *invents* events. Missing releases are reported as
confidence flags; the lock simply stays held. Out-of-order arrival is
detected against collection-generation seq order.
"""
from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .events import Event, order_key


def _lk(lock: str, gen: int) -> str:
    return "g%d:%s" % (gen, lock)


@dataclass
class Hold:
    mode: str  # S / X
    acquired_ts: float
    acquire_event: Event
    grant_event: Event  # acquired / cond_notify that produced the hold
    depth: int = 1  # reentry depth (recursive mutex, X reentry)


@dataclass
class Pending:
    thread: str
    lock: str  # scoped key
    mode: str
    since_ts: float
    request_event: Event
    reason: str = "acquire"  # acquire / cond_reacquire
    cond: Optional[str] = None
    cond_wait_event: Optional[Event] = None
    released_modes: Tuple[str, ...] = ()


@dataclass
class Replayer:
    """Step-through replayer.

    `lock_types` maps scoped lock key -> mutex|recursive|rwlock.
    Feed events already ordered by order_key via `step`; call `finish`
    for end-of-stream confidence flags, then `snapshot`.
    """

    lock_types: Dict[str, str] = field(default_factory=dict)
    holds: Dict[str, Dict[str, Hold]] = field(default_factory=dict)
    pending: Dict[Tuple[str, str], Pending] = field(default_factory=dict)
    threads: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    cond_waiters: Dict[str, Dict[str, Pending]] = field(default_factory=dict)
    flags: List[Dict[str, Any]] = field(default_factory=list)
    last_seq: Dict[int, int] = field(default_factory=dict)
    last_ts: float = float("-inf")
    processed: int = 0

    def clone(self) -> "Replayer":
        return copy.deepcopy(self)

    # ------------------------------------------------------------------ flags
    def _flag(self, kind: str, message: str, event: Optional[Event] = None,
              until_ts: Optional[float] = None) -> None:
        entry = {"kind": kind, "message": message}
        if event is not None:
            entry["thread"] = event.thread
            entry["lock"] = event.lock
            entry["gen"] = event.gen
            entry["event_id"] = event.id
            entry["ts"] = event.ts
        if until_ts is not None:
            entry["until_ts"] = until_ts
        self.flags.append(entry)

    def _type(self, key: str) -> str:
        return self.lock_types.get(key, "mutex")

    def _owner_modes(self, key: str) -> Dict[str, Hold]:
        return self.holds.get(key, {})

    def _effective_mode(self, key: str) -> Optional[str]:
        owners = self._owner_modes(key)
        if not owners:
            return None
        if any(h.mode == "X" for h in owners.values()):
            return "X"
        return "S"

    def _thread_record(self, thread: str, gen: int) -> Dict[str, Any]:
        rec = self.threads.get(thread)
        if rec is None:
            rec = {"alive": False, "gen": gen, "started": None, "ended": None}
            self.threads[thread] = rec
        return rec

    # ----------------------------------------------------------- grant checks
    def _blocked_for(self, key: str, thread: str, mode: str) -> List[str]:
        """Return threads currently blocking `thread` from taking `key`."""
        owners = self._owner_modes(key)
        blockers = []
        waiters_ahead = False
        for owner, hold in owners.items():
            if owner == thread:
                continue
            if self._type(key) == "rwlock":
                if mode == "X" or hold.mode == "X":
                    blockers.append(owner)
            else:
                blockers.append(owner)
        # FIFO fairness: an existing waiter ahead blocks later S readers too
        for (other_thread, other_lock), pend in self.pending.items():
            if other_lock != key or other_thread == thread:
                continue
            waiters_ahead = True
            if self._type(key) == "rwlock" and mode == "S" and pend.mode == "S":
                continue
            blockers.append(other_thread)
        if waiters_ahead and not blockers:
            # multiple queued S readers do not block each other
            return []
        return blockers

    def _grant(self, key: str, thread: str, mode: str, acquire_event: Event,
               grant_event: Event) -> bool:
        owners = self.holds.setdefault(key, {})
        ltype = self._type(key)
        if thread in owners:
            hold = owners[thread]
            if hold.mode == "S" and mode == "X":
                if ltype != "rwlock":
                    self._flag("invalid_upgrade",
                               "S->X upgrade on non-rwlock", grant_event)
                    return False
                blockers = [o for o in owners if o != thread]
                if blockers:
                    self._flag("blocked_upgrade",
                               "S->X upgrade waits for other readers",
                               grant_event)
                    return False
                hold.mode = "X"
                hold.depth += 1
                hold.acquire_event = acquire_event
                hold.grant_event = grant_event
                return True
            hold.depth += 1
            hold.acquire_event = acquire_event
            hold.grant_event = grant_event
            if ltype == "mutex":
                self._flag("mutex_reacquire",
                           "plain mutex re-acquired by same owner "
                           "(treated as reentrant candidate)", grant_event)
            return True

        blockers = self._blocked_for(key, thread, mode)
        if blockers:
            return False

        owners[thread] = Hold(mode=("X" if ltype != "rwlock" else mode),
                              acquired_ts=grant_event.ts,
                              acquire_event=acquire_event,
                              grant_event=grant_event)
        rec = self._thread_record(thread, grant_event.gen)
        stack = list(grant_event.stack or acquire_event.stack or [])
        rec.setdefault("hold_stack", {})[key] = stack
        return True

    # ----------------------------------------------------------------- steps
    def step(self, event: Event) -> None:
        self.processed += 1
        prev_seq = self.last_seq.get(event.gen)
        if prev_seq is not None and event.seq < prev_seq:
            self._flag("out_of_order",
                       "event seq %d arrived after seq %d (gen %d)"
                       % (event.seq, prev_seq, event.gen), event)
        self.last_seq[event.gen] = max(event.seq, prev_seq or event.seq)
        self.last_ts = event.ts

        handler = getattr(self, "_on_" + event.type, None)
        if handler is not None:
            handler(event)

    def _on_thread_start(self, e: Event) -> None:
        rec = self._thread_record(e.thread, e.gen)
        if rec["alive"]:
            self._flag("thread_restart",
                       "thread_start for already live thread", e)
        rec["alive"] = True
        rec["started"] = e.ts
        rec["gen"] = e.gen

    def _on_thread_end(self, e: Event) -> None:
        rec = self._thread_record(e.thread, e.gen)
        rec["alive"] = False
        rec["ended"] = e.ts
        for key, owners in list(self.holds.items()):
            if e.thread in owners:
                self._flag("thread_end_held",
                           "thread ended while still holding lock "
                           "(no release observed)", e)
                del owners[e.thread]
        for (thread, key), pend in list(self.pending.items()):
            if thread == e.thread:
                if pend.reason == "cond_reacquire":
                    cond = pend.cond
                    waiters = self.cond_waiters.get(cond, {})
                    waiters.pop(thread, None)
                self._flag("wait_cancelled_exit",
                           "pending acquisition abandoned on thread exit",
                           pend.request_event)
                del self.pending[(thread, key)]

    def _on_acquire_req(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        pkey = (e.thread, key)
        mode = e.mode or ("X" if self._type(key) != "rwlock" else "S")
        if pkey in self.pending:
            self._flag("duplicate_request",
                       "new acquire request while a previous one is pending",
                       e)
            return
        self.pending[pkey] = Pending(thread=e.thread, lock=key, mode=mode,
                                     since_ts=e.ts, request_event=e)

    def _on_acquired(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        pkey = (e.thread, key)
        pend = self.pending.pop(pkey, None)
        if pend is None:
            owners = self.holds.get(key, {})
            if e.thread in owners:
                self._flag("unsolicited_grant",
                           "acquired with no pending request "
                           "(lock already held)", e)
            else:
                self._flag("unsolicited_grant",
                           "acquired with no preceding acquire request", e)
            req = e
            mode = e.mode or ("X" if self._type(key) != "rwlock" else "S")
        else:
            req = pend.request_event
            mode = pend.mode
        granted = self._grant(key, e.thread, mode, req, e)
        if not granted and pend is not None:
            # grant observed while still blocked: keep waiting but flag it
            self.pending[pkey] = pend
            self._flag("grant_while_blocked",
                       "acquired event while lock is still unavailable", e)

    def _on_release(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        owners = self.holds.get(key, {})
        hold = owners.get(e.thread)
        if hold is None:
            self._flag("release_without_hold",
                       "release by thread that does not hold the lock", e)
            return
        hold.depth -= 1
        if hold.depth <= 0:
            del owners[e.thread]
            rec = self.threads.get(e.thread)
            if rec is not None:
                rec.get("hold_stack", {}).pop(key, None)

    def _on_cond_wait(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        cond = e.cond or (e.lock + "#cv")
        owners = self.holds.get(key, {})
        hold = owners.get(e.thread)
        released_modes: Tuple[str, ...] = ()
        if hold is None:
            self._flag("cond_wait_without_hold",
                       "cond_wait by thread that does not hold the guard", e)
            mode = e.mode or ("X" if self._type(key) != "rwlock" else "S")
        else:
            mode = hold.mode
            released_modes = (mode,) if hold.depth <= 1 else tuple(
                [mode] * hold.depth)
            del owners[e.thread]
            rec = self.threads.get(e.thread)
            if rec is not None:
                rec.get("hold_stack", {}).pop(key, None)
        pkey = (e.thread, key)
        pend = Pending(thread=e.thread, lock=key, mode=mode, since_ts=e.ts,
                       request_event=e, reason="cond_reacquire", cond=cond,
                       cond_wait_event=e, released_modes=released_modes)
        self.pending[pkey] = pend
        self.cond_waiters.setdefault(cond, {})[e.thread] = pend

    def _wake(self, pend: Pending, grant_event: Event, all_waiters: bool) -> None:
        key = pend.lock
        granted = self._grant(key, pend.thread, pend.mode,
                              pend.cond_wait_event or pend.request_event,
                              grant_event)
        if granted:
            self.pending.pop((pend.thread, key), None)
            self.cond_waiters.get(pend.cond, {}).pop(pend.thread, None)
        elif all_waiters:
            # broadcast: every waiter competes; those blocked stay pending
            pass

    def _on_cond_notify(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        cond = e.cond or (e.lock + "#cv")
        waiters = self.cond_waiters.get(cond, {})
        if not waiters:
            self._flag("stray_notify",
                       "cond_notify with no waiting thread", e)
            return
        notified = sorted(waiters.values(), key=lambda p: p.since_ts)[0]
        self._wake(notified, e, False)

    def _on_cond_notify_all(self, e: Event) -> None:
        cond = e.cond or (e.lock + "#cv")
        waiters = list(self.cond_waiters.get(cond, {}).values())
        if not waiters:
            self._flag("stray_notify",
                       "cond_notify_all with no waiting thread", e)
            return
        for pend in sorted(waiters, key=lambda p: p.since_ts):
            self._wake(pend, e, True)

    def _on_acquire_timeout(self, e: Event) -> None:
        key = _lk(e.lock, e.gen)
        pkey = (e.thread, key)
        pend = self.pending.pop(pkey, None)
        if pend is None:
            self._flag("timeout_without_wait",
                       "acquire_timeout with no matching pending request", e)
            return
        if pend.reason == "cond_reacquire":
            self.cond_waiters.get(pend.cond, {}).pop(e.thread, None)
        self._flag("wait_timeout",
                   "pending acquisition cancelled by timeout",
                   pend.request_event)

    # --------------------------------------------------------------- finish
    def finish(self) -> None:
        """End-of-stream bookkeeping. Only valid for full-stream replay."""
        for key, owners in self.holds.items():
            for thread, hold in owners.items():
                self._flag("missing_release",
                           "lock still held at end of stream "
                           "(no release observed; not synthesized)",
                           hold.grant_event)
        for pend in self.pending.values():
            self._flag("pending_at_end",
                       "request still waiting at end of stream",
                       pend.request_event)

    # -------------------------------------------------------------- snapshot
    def _edge_refs(self, *events: Event) -> List[Dict[str, Any]]:
        refs = []
        seen = set()
        for ev in events:
            if ev is None:
                continue
            ident = (ev.id, ev.seq, ev.ts, ev.type)
            if ident in seen:
                continue
            seen.add(ident)
            refs.append({
                "id": ev.id,
                "seq": ev.seq,
                "ts": ev.ts,
                "type": ev.type,
                "thread": ev.thread,
                "lock": ev.lock,
                "gen": ev.gen,
            })
        return refs

    def snapshot(self) -> Dict[str, Any]:
        """Build the held graph, wait graph and wait-for graph at this point."""
        lock_rows: Dict[str, Dict[str, Any]] = {}
        hold_edges = []
        for key, owners in self.holds.items():
            gen = int(key.split(":", 1)[0][1:])
            lock_id = key.split(":", 1)[1]
            ltype = self._type(key)
            holders = sorted(owners)
            mode = self._effective_mode(key)
            row = lock_rows.setdefault(key, {
                "lock": lock_id, "gen": gen, "type": ltype,
                "mode": mode, "holders": holders,
            })
            for thread, hold in owners.items():
                rec = self.threads.get(thread, {})
                stack = (rec.get("hold_stack", {}) or {}).get(key, [])
                hold_edges.append({
                    "thread": thread,
                    "lock": lock_id,
                    "gen": gen,
                    "mode": hold.mode,
                    "depth": hold.depth,
                    "since_ts": hold.acquired_ts,
                    "stack": stack,
                    "events": self._edge_refs(hold.acquire_event,
                                              hold.grant_event),
                })

        wait_edges = []
        wait_for: Dict[str, Set[str]] = defaultdict(set)
        wait_for_edges: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for pend in sorted(self.pending.values(),
                           key=lambda p: (p.lock, p.thread)):
            gen = int(pend.lock.split(":", 1)[0][1:])
            lock_id = pend.lock.split(":", 1)[1]
            blockers = self._blocked_for(pend.lock, pend.thread, pend.mode)
            wait_edges.append({
                "thread": pend.thread,
                "lock": lock_id,
                "gen": gen,
                "mode": pend.mode,
                "reason": pend.reason,
                "cond": pend.cond,
                "since_ts": pend.since_ts,
                "stack": list(pend.request_event.stack or []),
                "blockers": sorted(blockers),
                "events": self._edge_refs(pend.request_event,
                                          pend.cond_wait_event),
            })
            for blocker in blockers:
                wait_for[pend.thread].add(blocker)
                wait_for.setdefault(blocker, set())
                wait_for_edges[(pend.thread, blocker)] = {
                    "waiter": pend.thread,
                    "blocker": blocker,
                    "lock": lock_id,
                    "gen": gen,
                    "mode": pend.mode,
                    "wait_event": self._edge_refs(pend.request_event)[0],
                }

        cycles = self._cycles(dict(wait_for), wait_for_edges)
        thread_rows = []
        for thread, rec in sorted(self.threads.items()):
            thread_rows.append({
                "thread": thread,
                "gen": rec.get("gen", 0),
                "alive": rec["alive"],
                "started": rec.get("started"),
                "ended": rec.get("ended"),
                "waiting_on": next((w["lock"] for w in wait_edges
                                    if w["thread"] == thread), None),
                "holding": sorted(k.split(":", 1)[1]
                                  for k, owners in self.holds.items()
                                  if thread in owners),
            })

        return {
            "ts": self.last_ts if self.last_ts != float("-inf") else None,
            "processed": self.processed,
            "threads": thread_rows,
            "locks": sorted(lock_rows.values(),
                            key=lambda r: (r["gen"], r["lock"])),
            "hold_edges": hold_edges,
            "wait_edges": wait_edges,
            "wait_for_edges": [wait_for_edges[k]
                               for k in sorted(wait_for_edges)],
            "cycles": cycles,
            "confidence": list(self.flags),
        }

    def _cycles(self, graph: Dict[str, Set[str]],
                wait_for_edges: Dict[Tuple[str, str], Dict[str, Any]]
                ) -> List[Dict[str, Any]]:
        """Enumerate elementary directed cycles (each once, rotated min-first)."""
        found: Dict[Tuple[str, ...], List[str]] = {}

        def normalize(cycle: List[str]) -> Tuple[str, ...]:
            index = min(range(len(cycle)), key=lambda i: cycle[i])
            rotated = cycle[index:] + cycle[:index]
            return tuple(rotated)

        def dfs(start: str, node: str, path: List[str], seen: Set[str]) -> None:
            for nxt in sorted(graph.get(node, ())):
                if nxt == start and len(path) >= 2:
                    key = normalize(path)
                    found.setdefault(key, list(key))
                elif nxt not in seen and nxt >= start:
                    seen.add(nxt)
                    dfs(start, nxt, path + [nxt], seen)
                    seen.discard(nxt)

        for start in sorted(graph):
            dfs(start, start, [start], {start})

        cycles = []
        for nodes in found.values():
            members = set(nodes)
            edges = []
            holds = []
            waits = []
            for i, waiter in enumerate(nodes):
                blocker = nodes[(i + 1) % len(nodes)]
                edge = wait_for_edges[(waiter, blocker)]
                edges.append(edge)
                lock_id = edge["lock"]
                gen = edge["gen"]
                key = _lk(lock_id, gen)
                hold = self.holds.get(key, {}).get(blocker)
                rec = self.threads.get(blocker, {})
                holds.append({
                    "thread": blocker,
                    "lock": lock_id,
                    "gen": gen,
                    "mode": hold.mode if hold else None,
                    "depth": hold.depth if hold else 0,
                    "stack": (rec.get("hold_stack", {}) or {}).get(key, []),
                    "events": self._edge_refs(hold.acquire_event,
                                              hold.grant_event)
                    if hold else [],
                })
                pend = self.pending.get((waiter, key))
                waits.append({
                    "thread": waiter,
                    "lock": lock_id,
                    "gen": gen,
                    "mode": edge["mode"],
                    "stack": list(pend.request_event.stack or [])
                    if pend else [],
                    "events": self._edge_refs(pend.request_event,
                                              pend.cond_wait_event)
                    if pend else [],
                })
            cycles.append({
                "threads": nodes,
                "length": len(nodes),
                "wait_for_edges": edges,
                "holding_evidence": holds,
                "waiting_evidence": waits,
                "signature": " -> ".join(list(nodes) + [nodes[0]]),
            })
        cycles.sort(key=lambda c: (c["length"], c["signature"]))
        return cycles


def analyze(events: List[Event], lock_types: Optional[Dict[str, str]] = None,
            at_ts: Optional[float] = None,
            complete: bool = True) -> Dict[str, Any]:
    """Replay events up to `at_ts` (inclusive) and return a snapshot.

    Out-of-order detection always uses arrival (seq) order of the *full*
    batch; only events with ts <= at_ts are replayed into the graph.
    """
    ordered = sorted(events, key=order_key)
    replay = Replayer(lock_types=dict(lock_types or {}))

    # arrival-order anomalies: the input list *is* collection order
    last_arrival: Dict[int, int] = {}
    for ev in events:
        prev = last_arrival.get(ev.gen)
        if prev is not None and ev.seq < prev:
            replay._flag("out_of_order",
                         "event seq %d arrived after seq %d (gen %d)"
                         % (ev.seq, prev, ev.gen), ev)
        last_arrival[ev.gen] = max(
            ev.seq, prev if prev is not None else ev.seq)

    for ev in ordered:
        if at_ts is not None and ev.ts > at_ts:
            break
        replay.step(ev)
    if complete:
        replay.finish()

    snap = replay.snapshot()
    snap["at_ts"] = at_ts
    snap["out_of_order_events"] = [
        f["ts"] for f in snap["confidence"] if f["kind"] == "out_of_order"
    ]
    return snap
