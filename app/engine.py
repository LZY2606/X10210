"""Reconstruction engine: ordered event stream -> timed hold/wait graphs.

Design notes
------------
* Events are ordered by (generation, timestamp, arrival-seq).  The generation
  tag separates process restarts that reuse the same lock addresses.
* The engine never invents a ``release``.  Missing/contradictory evidence is
  exposed as an anomaly and the derived edges carry ``tentative`` / ``low``
  confidence instead.
* Every graph edge records the exact event ids that produced it so the UI can
  drill back to raw events.
* ``StreamEngine`` supports out-of-order append: a late event only forces
  replay of frames from its logical position onward (checkpoints make that
  cheap), and the result is identical to a full replay.
"""
from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    Event, THREAD_START, THREAD_END, REQUEST, GRANT, RELEASE,
    COND_WAIT, COND_NOTIFY, COND_GRANT, CANCEL,
    EXCLUSIVE, SHARED, MUTEX, RECURSIVE, RWLOCK,
    FIRM, TENTATIVE, LOW,
)

# --- anomalies -------------------------------------------------------------
A_THREAD_LEAK = "thread_exited_while_holding"
A_DOUBLE_REQUEST = "request_while_pending"
A_ORPHAN_GRANT = "grant_without_request"
A_CONFLICT_GRANT = "grant_conflicts_with_holder"
A_GRANT_UNKNOWN_MODE = "grant_mode_unknown"
A_ORPHAN_RELEASE = "release_without_hold"
A_SELF_DEADLOCK = "self_deadlock_request"
A_UPGRADE_WHILE_READER = "rwlock_upgrade_contended"
A_COND_WAIT_NO_LOCK = "cond_wait_without_holding"
A_COND_GRANT_NO_WAIT = "cond_grant_without_wait"
A_UNKNOWN_THREAD = "event_on_unknown_thread"
A_CANCEL_NOTHING = "cancel_without_pending"
A_BAD_KIND = "unknown_event_kind"


def _eid(ev) -> int:
    return ev.id if ev.id is not None else 0


@dataclass
class Holder:
    """One lock acquisition held by a thread (one stack entry)."""
    mode: str
    count: int = 1                 # reentrancy depth (recursive / rw re-read)
    grant_id: int = 0
    request_id: int = 0
    stack: Optional[list] = None
    confidence: str = FIRM
    # events that explain the current depth (last relevant grant/release)
    provenance: List[int] = field(default_factory=list)

    def to_dict(self):
        return {
            "mode": self.mode, "count": self.count,
            "grant_id": self.grant_id, "request_id": self.request_id,
            "stack": self.stack, "confidence": self.confidence,
            "provenance": self.provenance,
        }


@dataclass
class Waiter:
    """A pending lock acquisition request (thread waits for a lock)."""
    mode: str
    request_id: int
    request_ts: float
    stack: Optional[list]
    timeout: bool
    confidence: str = FIRM
    provenance: List[int] = field(default_factory=list)
    # set when the wait is really a reacquire after cond_wait
    cond: Optional[str] = None
    cond_wait_id: int = 0

    def to_dict(self):
        return {
            "mode": self.mode, "request_id": self.request_id,
            "request_ts": self.request_ts, "stack": self.stack,
            "timeout": self.timeout, "confidence": self.confidence,
            "provenance": self.provenance, "cond": self.cond,
            "cond_wait_id": self.cond_wait_id,
        }


@dataclass
class CondWaitRecord:
    """Thread parked on a condition variable, having released its lock."""
    lock: str
    mode: str
    cond: str
    cond_wait_id: int
    request_id: int
    stack: Optional[list]
    released_provenance: List[int]
    timeout: bool = False


@dataclass
class Anomaly:
    event_id: int
    kind: str
    gen: int
    ts: float
    thread: str
    lock: Optional[str]
    detail: str
    confidence: str = TENTATIVE

    def to_dict(self):
        return {
            "event_id": self.event_id, "kind": self.kind, "gen": self.gen,
            "ts": self.ts, "thread": self.thread, "lock": self.lock,
            "detail": self.detail, "confidence": self.confidence,
        }


@dataclass
class OrderEdge:
    """An observed lock-order edge: thread acquired ``b`` while holding ``a``."""
    a: str
    b: str
    gen: int
    thread: str
    ts: float
    event_id: int
    mode_a: str
    mode_b: str
    stack: Optional[list] = None
    grant_event: int = 0
    request_event: int = 0
    lived: bool = False     # edge participated in a live cycle at some frame

    def key(self):
        return (self.gen, self.a, self.b, self.thread)

    def to_dict(self):
        return {
            "a": self.a, "b": self.b, "gen": self.gen,
            "thread": self.thread, "ts": self.ts,
            "event_id": self.event_id, "mode_a": self.mode_a,
            "mode_b": self.mode_b, "stack": self.stack,
            "grant_event": self.grant_event,
            "request_event": self.request_event,
            "lived": self.lived,
        }


@dataclass
class GenState:
    gen: int
    # thread -> list[lock] held (outermost last? we store acquisition order)
    held: Dict[str, List[str]] = field(default_factory=dict)
    holders: Dict[str, Dict[str, Holder]] = field(default_factory=dict)
    # lock -> {thread: Waiter}
    waiting: Dict[str, Dict[str, Waiter]] = field(default_factory=dict)
    cond_waiting: Dict[str, CondWaitRecord] = field(default_factory=dict)
    # uncontended requests that were immediately granted still register an
    # "in-flight" record so a following GRANT event pairs up correctly.
    inflight: Dict[Tuple[str, str], Waiter] = field(default_factory=dict)
    # (thread, lock) -> acquisition timestamp of current depth-1 hold
    hold_since: Dict[Tuple[str, str], float] = field(default_factory=dict)
    threads: set = field(default_factory=set)
    ended: set = field(default_factory=set)

    def ensure_thread(self, t):
        self.held.setdefault(t, [])
        self.holders.setdefault(t, {})
        self.threads.add(t)


@dataclass
class Frame:
    """State snapshot after one event."""
    index: int
    event_id: int
    gen: int
    ts: float
    kind: str
    cycle_ids: List[str] = field(default_factory=list)


def _modes_compatible(a: str, b: str) -> bool:
    return a == SHARED and b == SHARED


@dataclass
class ReplayResult:
    gens: Dict[int, GenState]
    anomalies: List[Anomaly]
    order_edges: Dict[Tuple, OrderEdge]
    frames: List[Frame]
    events: List[Event]
    lock_types: Dict[str, str]            # "gen:lock" -> type
    # frame index -> list[cycle dicts]  (persisted with the replay)
    cycles_at: Dict[int, List[dict]] = field(default_factory=dict)
    signature_first_frame: Dict[str, int] = field(default_factory=dict)
    # (gen, thread, lock) -> list of (start_ts, end_ts or None, mode)
    hold_windows: Dict[Tuple, List[tuple]] = field(default_factory=dict)


class ReplayEngine:
    """One-shot replay used by branches and by minimization."""

    def __init__(self, lock_types: Optional[Dict[str, str]] = None,
                 default_lock_type: str = MUTEX):
        self.lock_types = dict(lock_types or {})
        self.default_lock_type = default_lock_type
        self.gens: Dict[int, GenState] = {}
        self.anomalies: List[Anomaly] = []
        self.order_edges: Dict[Tuple, OrderEdge] = {}
        self.frames: List[Frame] = []
        self.events: List[Event] = []
        self.cycles_at: Dict[int, List[dict]] = {}
        self.signature_first: Dict[str, int] = {}
        self.hold_windows: Dict[Tuple, List[tuple]] = {}

    # -- helpers -----------------------------------------------------------
    def gstate(self, gen: int) -> GenState:
        gs = self.gens.get(gen)
        if gs is None:
            gs = GenState(gen=gen)
            self.gens[gen] = gs
        return gs

    def _lkey(self, gen, lock) -> str:
        return f"{gen}:{lock}"

    def lock_type(self, gen, lock) -> str:
        t = self.lock_types.get(self._lkey(gen, lock))
        return t if t in (MUTEX, RECURSIVE, RWLOCK) else self.default_lock_type

    def _anom(self, ev, kind, detail, confidence=TENTATIVE, lock=None):
        self.anomalies.append(Anomaly(
            event_id=_eid(ev), kind=kind, gen=ev.gen, ts=ev.ts,
            thread=ev.thread, lock=lock if lock is not None else ev.lock,
            detail=detail, confidence=confidence))

    def _check_thread(self, ev, gs) -> bool:
        if ev.thread not in gs.threads:
            self._anom(ev, A_UNKNOWN_THREAD,
                       f"event for thread {ev.thread!r} before thread_start",
                       lock=None)
            gs.ensure_thread(ev.thread)
            return False
        if ev.thread in gs.ended:
            self._anom(ev, A_UNKNOWN_THREAD,
                       f"event for already-finished thread {ev.thread!r}",
                       confidence=LOW, lock=None)
            return False
        return True

    def _stack_of(self, ev) -> Optional[list]:
        st = ev.stack
        if st is None and ev.data.get("stack"):
            st = ev.data["stack"]
        return list(st) if st else None

    def _push_order_edges(self, ev, gs, lock, mode):
        new_holder = gs.holders.get(ev.thread, {}).get(lock)
        for outer in gs.held.get(ev.thread, []):
            if outer == lock:
                continue
            h = gs.holders[ev.thread][outer]
            oe = OrderEdge(
                a=outer, b=lock, gen=ev.gen, thread=ev.thread, ts=ev.ts,
                event_id=_eid(ev), mode_a=h.mode, mode_b=mode,
                stack=list(new_holder.stack) if new_holder and new_holder.stack else None,
                grant_event=new_holder.grant_id if new_holder else 0,
                request_event=new_holder.request_id if new_holder else 0)
            self.order_edges.setdefault(oe.key(), oe)

    # -- main entry --------------------------------------------------------
    def run(self, events: List[Event]) -> ReplayResult:
        ordered = sorted(events, key=lambda e: e.sort_key())
        self.events = ordered
        for ev in ordered:
            self._apply(ev)
            cycles = detect_cycles_for_gen(self.gstate(ev.gen))
            self.frames.append(Frame(
                index=len(self.frames), event_id=_eid(ev),
                gen=ev.gen, ts=ev.ts, kind=ev.kind,
                cycle_ids=[c["signature"] for c in cycles]))
            if cycles:
                self.cycles_at[self.frames[-1].index] = cycles
                for c in cycles:
                    self.signature_first.setdefault(c["signature"],
                                                    self.frames[-1].index)
                self._mark_lived(gs=self.gstate(ev.gen), cycles=cycles)
        return ReplayResult(
            gens=self.gens, anomalies=self.anomalies,
            order_edges=self.order_edges, frames=self.frames,
            events=ordered, lock_types=self.lock_types,
            cycles_at=self.cycles_at,
            signature_first_frame=self.signature_first,
            hold_windows=self.hold_windows)

    def _mark_lived(self, gs, cycles):
        threads = {s for c in cycles for s in c["thread_sequence"]}
        for t in threads:
            for oe in list(self.order_edges.values()):
                if oe.thread == t and oe.gen == gs.gen:
                    oe.lived = True

    # -- dispatch ----------------------------------------------------------
    def _apply(self, ev: Event):
        gs = self.gstate(ev.gen)
        if ev.kind == THREAD_START:
            self._ev_thread_start(ev, gs)
        elif ev.kind == THREAD_END:
            self._ev_thread_end(ev, gs)
        elif ev.kind == REQUEST:
            self._ev_request(ev, gs)
        elif ev.kind == GRANT:
            self._ev_grant(ev, gs)
        elif ev.kind == RELEASE:
            self._ev_release(ev, gs)
        elif ev.kind == COND_WAIT:
            self._ev_cond_wait(ev, gs)
        elif ev.kind == COND_NOTIFY:
            self._ev_cond_notify(ev, gs)
        elif ev.kind == COND_GRANT:
            self._ev_cond_grant(ev, gs)
        elif ev.kind == CANCEL:
            self._ev_cancel(ev, gs)
        else:
            self._anom(ev, A_BAD_KIND, f"unknown event kind {ev.kind!r}",
                       confidence=LOW)

    def _ev_thread_start(self, ev, gs):
        if ev.thread in gs.threads and ev.thread not in gs.ended:
            self._anom(ev, A_UNKNOWN_THREAD,
                       f"duplicate thread_start for {ev.thread!r}", LOW,
                       lock=None)
        gs.ensure_thread(ev.thread)
        gs.ended.discard(ev.thread)

    def _ev_thread_end(self, ev, gs):
        if ev.thread not in gs.threads:
            self._anom(ev, A_UNKNOWN_THREAD,
                       f"thread_end for unknown {ev.thread!r}", lock=None)
            gs.ensure_thread(ev.thread)
        gs.ended.add(ev.thread)
        # any pending wait is implicitly revoked by termination
        for lock, waiters in list(gs.waiting.items()):
            if ev.thread in waiters:
                del waiters[ev.thread]
        gs.cond_waiting.pop(ev.thread, None)
        # never invent a release: surviving holds are a leak anomaly and the
        # derived hold edges become low-confidence evidence.
        held = gs.held.get(ev.thread, [])
        if held:
            for lock in held:
                h = gs.holders[ev.thread].get(lock)
                if h is not None:
                    h.confidence = LOW
                    h.provenance.append(_eid(ev))
                self._anom(ev, A_THREAD_LEAK,
                           f"thread {ev.thread!r} ended holding {lock!r} "
                           f"x{h.count if h else '?'}", confidence=LOW,
                           lock=lock)

    # -- request / grant / release ----------------------------------------
    def _ev_request(self, ev, gs):
        self._check_thread(ev, gs)
        if not ev.lock:
            self._anom(ev, A_BAD_KIND, "request without lock", LOW)
            return
        ltype = self.lock_type(ev.gen, ev.lock)
        mode = ev.mode or EXCLUSIVE
        t = ev.thread
        stack = self._stack_of(ev)

        # already pending? the old request is implicitly abandoned in the
        # trace; keep it but flag the ambiguity.
        if t in gs.waiting.get(ev.lock, {}):
            self._anom(ev, A_DOUBLE_REQUEST,
                       f"new request on {ev.lock!r} while previous pending",
                       lock=ev.lock)
        # any other pending request of this thread means this request can't
        # be queued faithfully - still record it as a second waiter.
        holder = gs.holders.get(t, {}).get(ev.lock)

        if holder is not None:
            if ltype == RECURSIVE:
                holder.count += 1
                holder.provenance.append(_eid(ev))
                return
            if ltype == RWLOCK:
                if holder.mode == SHARED and mode == SHARED:
                    holder.count += 1
                    holder.provenance.append(_eid(ev))
                    return
                if holder.mode == SHARED and mode == EXCLUSIVE:
                    # upgrade: only succeeds if this thread is sole reader
                    other_holders = [x for x, hh in gs.holders.items()
                                     if ev.lock in hh and x != t]
                    if not other_holders:
                        holder.mode = EXCLUSIVE
                        holder.count = 1
                        holder.provenance.append(_eid(ev))
                        self._push_order_edges(ev, gs, ev.lock, EXCLUSIVE)
                    else:
                        w = Waiter(mode=EXCLUSIVE, request_id=_eid(ev),
                                   request_ts=ev.ts, stack=stack,
                                   timeout=ev.timeout, provenance=[_eid(ev)])
                        gs.waiting.setdefault(ev.lock, {})[t] = w
                        self._anom(ev, A_UPGRADE_WHILE_READER,
                                   f"upgrade on {ev.lock!r} waits with "
                                   f"{len(other_holders)} other reader(s)",
                                   lock=ev.lock)
                    return
                if holder.mode == EXCLUSIVE and ltype == MUTEX:
                    pass  # fallthrough to anomaly below
            # non-reentrant self acquisition is a classic single-thread deadlock
            self._anom(ev, A_SELF_DEADLOCK,
                       f"{t!r} requests {ev.lock!r} it already holds "
                       f"(type={ltype})", confidence=LOW, lock=ev.lock)
            w = Waiter(mode=mode, request_id=_eid(ev), request_ts=ev.ts,
                       stack=stack, timeout=ev.timeout,
                       confidence=LOW, provenance=[_eid(ev)])
            gs.waiting.setdefault(ev.lock, {})[t] = w
            return

        # not held by this thread: can it be granted immediately?
        all_holders = gs.holders
        conflicts = [x for x, hh in all_holders.items()
                     if ev.lock in hh and not _modes_compatible(
                         hh[ev.lock].mode, mode)]
        w = Waiter(mode=mode, request_id=_eid(ev), request_ts=ev.ts,
                   stack=stack, timeout=ev.timeout,
                   provenance=[_eid(ev)])
        if not conflicts:
            # immediately satisfied: still remember request provenance so
            # the subsequent GRANT event pairs instead of looking orphan.
            gs.inflight[(t, ev.lock)] = w
            h = Holder(mode=mode, request_id=_eid(ev),
                       grant_id=0, stack=stack,
                       provenance=[_eid(ev)])
            gs.holders.setdefault(t, {})[ev.lock] = h
            gs.held.setdefault(t, []).append(ev.lock)
            gs.hold_since[(t, ev.lock)] = ev.ts
            self._push_order_edges(ev, gs, ev.lock, mode)
        else:
            gs.waiting.setdefault(ev.lock, {})[t] = w

    def _ev_grant(self, ev, gs):
        self._check_thread(ev, gs)
        if not ev.lock:
            self._anom(ev, A_ORPHAN_GRANT, "grant without lock", LOW)
            return
        t = ev.thread
        waiters = gs.waiting.get(ev.lock, {})
        w = waiters.get(t)
        inflight = False
        if w is None:
            w = gs.inflight.pop((t, ev.lock), None)
            inflight = w is not None
        stack = self._stack_of(ev)
        if w is None:
            # grant with no observable pending request.  It might be an
            # immediate acquire whose request event was lost: treat as a
            # tentative holder rather than refusing it.
            self._anom(ev, A_ORPHAN_GRANT,
                       f"grant of {ev.lock!r} to {t!r} without request",
                       lock=ev.lock)
            mode = ev.mode or EXCLUSIVE
            h = Holder(mode=mode, request_id=0, grant_id=_eid(ev),
                       stack=stack, confidence=TENTATIVE,
                       provenance=[_eid(ev)])
            self._install_holder(ev, gs, h, mode)
            return
        mode = ev.mode or w.mode or EXCLUSIVE
        if not ev.mode and not w.mode:
            mode = EXCLUSIVE
        conflicting = []
        for x, hh in gs.holders.items():
            if ev.lock not in hh or x == t:
                continue
            if not _modes_compatible(hh[ev.lock].mode, mode):
                conflicting.append((x, hh[ev.lock].mode))
        if conflicting:
            self._anom(ev, A_CONFLICT_GRANT,
                       f"grant of {ev.lock!r} conflicts with holder(s) "
                       f"{conflicting}", confidence=LOW, lock=ev.lock)
        # inflight grants already have a holder installed by REQUEST; just
        # stamp the grant evidence onto it instead of double-installing.
        if inflight:
            existing = gs.holders.get(t, {}).get(ev.lock)
            if existing is not None:
                existing.grant_id = _eid(ev)
                existing.provenance.extend([_eid(ev)])
                if conflicting:
                    existing.confidence = LOW
        else:
            self._install_holder(ev, gs, Holder(
                mode=mode, request_id=w.request_id, grant_id=_eid(ev),
                stack=w.stack or stack,
                provenance=w.provenance + [_eid(ev)],
                confidence=LOW if conflicting else FIRM), mode,
                lock=ev.lock)
        waiters.pop(t, None)
        gs.inflight.pop((t, ev.lock), None)

    def _install_holder(self, ev, gs, h: Holder, mode: str, lock: Optional[str] = None):
        lock = lock if lock is not None else ev.lock
        t = ev.thread
        existing = gs.holders.setdefault(t, {}).get(lock)
        if existing is not None:
            # recursive re-grant (e.g. orphan grant stacked on inferred hold)
            ltype = self.lock_type(ev.gen, ev.lock)
            ltype = self.lock_type(ev.gen, lock)
            if ltype == RECURSIVE or (ltype == RWLOCK and
                                      existing.mode == SHARED and mode == SHARED):
                existing.count += 1
                existing.provenance.extend(h.provenance)
                if h.confidence == LOW:
                    existing.confidence = LOW
                return
            # contradictory double ownership - keep newest, flag low
            existing.confidence = LOW
            h.confidence = LOW
        new_hold = lock not in gs.holders.get(t, {})
        gs.holders[t][lock] = h
        if lock not in gs.held.setdefault(t, []):
            gs.held[t].append(lock)
        if new_hold:
            gs.hold_since[(t, lock)] = ev.ts
        self._push_order_edges(ev, gs, lock, mode)

    def _ev_release(self, ev, gs):
        self._check_thread(ev, gs)
        if not ev.lock:
            self._anom(ev, A_ORPHAN_RELEASE, "release without lock", LOW)
            return
        t = ev.thread
        h = gs.holders.get(t, {}).get(ev.lock)
        if h is None:
            self._anom(ev, A_ORPHAN_RELEASE,
                       f"release of {ev.lock!r} not held by {t!r}", LOW,
                       lock=ev.lock)
            return
        h.count -= 1
        h.provenance.append(_eid(ev))
        if h.count <= 0:
            del gs.holders[t][ev.lock]
            if ev.lock in gs.held.get(t, []):
                gs.held[t].remove(ev.lock)
            start = gs.hold_since.pop((t, ev.lock), ev.ts)
            self.hold_windows.setdefault(
                (ev.gen, t, ev.lock), []).append((start, ev.ts, h.mode))

    # -- condition variables ----------------------------------------------
    def _ev_cond_wait(self, ev, gs):
        self._check_thread(ev, gs)
        lock = ev.lock
        cond = ev.cond or ev.data.get("cond") or f"cond:{lock}"
        t = ev.thread
        h = gs.holders.get(t, {}).get(lock) if lock else None
        if h is None:
            self._anom(ev, A_COND_WAIT_NO_LOCK,
                       f"cond_wait on {lock!r} without holding it", LOW,
                       lock=lock)
            released = []
            mode = ev.mode or EXCLUSIVE
            request_id = 0
            stack = self._stack_of(ev)
        else:
            released = list(h.provenance)
            mode = h.mode
            request_id = h.request_id
            stack = h.stack or self._stack_of(ev)
            del gs.holders[t][lock]
            if lock in gs.held.get(t, []):
                gs.held[t].remove(lock)
            start = gs.hold_since.pop((t, lock), ev.ts)
            self.hold_windows.setdefault(
                (ev.gen, t, lock), []).append((start, ev.ts, mode))
        rec = CondWaitRecord(
            lock=lock, mode=mode, cond=cond, cond_wait_id=_eid(ev),
            request_id=request_id, stack=stack,
            released_provenance=released + [_eid(ev)],
            timeout=ev.timeout)
        gs.cond_waiting[t] = rec

    def _ev_cond_notify(self, ev, gs):
        self._check_thread(ev, gs)
        # notify itself changes no lock state; record nothing structurally.
        # Presence of waiters without later grant is simply visible as a
        # remaining cond_waiting record at snapshot time.
        return

    def _ev_cond_grant(self, ev, gs):
        self._check_thread(ev, gs)
        t = ev.thread
        rec = gs.cond_waiting.get(t)
        stack = self._stack_of(ev)
        if rec is None:
            self._anom(ev, A_COND_GRANT_NO_WAIT,
                       f"cond_grant for {t!r} without cond_wait", LOW,
                       lock=ev.lock)
            lock = ev.lock
            if not lock:
                return
            self._install_holder(ev, gs, Holder(
                mode=ev.mode or EXCLUSIVE, request_id=0,
                grant_id=_eid(ev), stack=stack, confidence=LOW,
                provenance=[_eid(ev)]), ev.mode or EXCLUSIVE, lock=lock)
            return
        lock = ev.lock or rec.lock
        conflicting = []
        for x, hh in gs.holders.items():
            if lock not in hh or x == t:
                continue
            if not _modes_compatible(hh[lock].mode, rec.mode):
                conflicting.append((x, hh[lock].mode))
        confidence = LOW if conflicting else FIRM
        if conflicting:
            self._anom(ev, A_CONFLICT_GRANT,
                       f"cond_grant of {lock!r} conflicts with {conflicting}",
                       LOW, lock=lock)
        h = Holder(mode=rec.mode, request_id=rec.request_id,
                   grant_id=_eid(ev), stack=rec.stack or stack,
                   confidence=confidence,
                   provenance=rec.released_provenance + [_eid(ev)])
        gs.cond_waiting.pop(t, None)
        self._install_holder(ev, gs, h, rec.mode, lock=lock)

    def _ev_cancel(self, ev, gs):
        self._check_thread(ev, gs)
        t = ev.thread
        found = False
        if gs.cond_waiting.pop(t, None) is not None:
            found = True
        for lock, waiters in list(gs.waiting.items()):
            if t in waiters and (not ev.lock or lock == ev.lock):
                waiters.pop(t)
                found = True
        if not found:
            self._anom(ev, A_CANCEL_NOTHING,
                       f"cancel for {t!r} but no pending wait", LOW,
                       lock=ev.lock)


# --- cycle detection on the thread<->lock bipartite wait-for graph --------
def detect_cycles_for_gen(gs: GenState) -> List[dict]:
    """Enumerate elementary cycles of the wait-for graph.

    Nodes are tagged ``t:<thread>`` and ``l:<lock>``.  Edges:
      thread --holds--> lock   (hold edge)
      lock   --waited-by--> thread? For a compact directed cycle we use:
      thread -> lock (waits for) and lock -> thread (held by).
    Condition waits are shown separately by the UI and never count as lock
    waits here (the lock was released, by definition).
    """
    waits: Dict[str, List[str]] = {}       # thread -> [locks waited]
    wait_info: Dict[Tuple[str, str], Waiter] = {}
    for lock, waiters in gs.waiting.items():
        for t, w in waiters.items():
            if t in gs.ended:
                continue
            waits.setdefault(t, []).append(lock)
            wait_info[(t, lock)] = w

    holders_of: Dict[str, List[str]] = {}  # lock -> [threads]
    hold_info: Dict[Tuple[str, str], Holder] = {}
    for t, hh in gs.holders.items():
        if t in gs.ended:
            continue
        for lock, h in hh.items():
            holders_of.setdefault(lock, []).append(t)
            hold_info[(t, lock)] = h

    # build directed graph: thread node -> lock node (wait), lock node ->
    # thread node (held).  Cycles alternate T,L,T,L...
    adj: Dict[str, set] = defaultdict(set)
    nodes = set()
    for t, locks in waits.items():
        for lock in locks:
            nodes.add(f"t:{t}")
            for owner in holders_of.get(lock, []):
                if owner == t:
                    continue  # self deadlock reported as anomaly, not cycle
                nodes.add(f"l:{lock}")
                nodes.add(f"t:{owner}")
                adj[f"t:{t}"].add(f"l:{lock}")
                adj[f"l:{lock}"].add(f"t:{owner}")

    cycles_raw = _elementary_cycles(adj, nodes)

    out = []
    seen_sigs = set()
    for cyc in cycles_raw:
        if cyc and cyc[0].startswith("l:"):
            # rotate so a thread node leads (graph alternates T,L)
            k = next((i for i, n in enumerate(cyc) if n.startswith("t:")), 0)
            cyc = cyc[k:] + cyc[:k]
        threads = [n[2:] for n in cyc if n.startswith("t:")]
        locks = [n[2:] for n in cyc if n.startswith("l:")]
        # canonical signature: rotate so the lexicographically smallest
        # thread leads, then align locks.
        sig = _cycle_signature(threads, locks)
        if sig in seen_sigs:
            continue
        seen_sigs.add(sig)
        edge_evidence = []
        min_conf = FIRM
        for i, t in enumerate(threads):
            lock = locks[i]
            w = wait_info.get((t, lock))
            owner = threads[(i + 1) % len(threads)]
            h = hold_info.get((owner, lock))
            wc = w.confidence if w else LOW
            hc = h.confidence if h else LOW
            conf = _worst(wc, hc)
            min_conf = _worst(min_conf, conf)
            edge_evidence.append({
                "waiter": t, "lock": lock, "holder": owner,
                "wait_request_event": w.request_id if w else None,
                "wait_stack": w.stack if w else None,
                "wait_provenance": list(w.provenance) if w else [],
                "wait_timeout": w.timeout if w else False,
                "hold_grant_event": h.grant_id if h else None,
                "hold_request_event": h.request_id if h else None,
                "hold_stack": h.stack if h else None,
                "hold_provenance": list(h.provenance) if h else [],
                "mode_waited": w.mode if w else None,
                "mode_held": h.mode if h else None,
                "confidence": conf,
            })
        out.append({
            "gen": gs.gen,
            "signature": sig,
            "thread_sequence": threads,
            "lock_sequence": locks,
            "edges": edge_evidence,
            "confidence": min_conf,
            "length": len(threads),
        })
    return out


def _worst(a: str, b: str) -> str:
    rank = {FIRM: 0, TENTATIVE: 1, LOW: 2}
    return a if rank.get(a, 1) >= rank.get(b, 1) else b


def _cycle_signature(threads: List[str], locks: List[str]) -> str:
    n = len(threads)
    best = None
    for i in range(n):
        rt = tuple(threads[(i + k) % n] for k in range(n))
        rl = tuple(locks[(i + k) % n] for k in range(n))
        cand = (rt, rl)
        if best is None or cand < best:
            best = cand
    rt, rl = best  # type: ignore[misc]
    return "->".join(f"{rt[k]}@{rl[k]}" for k in range(n))


def _elementary_cycles(adj: Dict[str, set], nodes: set) -> List[List[str]]:
    """Johnson-lite: DFS from each node restricted to nodes >= start.

    Graphs here are small (handful of blocked threads), so the simple
    implementation is plenty and keeps cycles elementary.
    """
    order = sorted(nodes)
    index = {n: i for i, n in enumerate(order)}
    result: List[List[str]] = []

    for s_idx, start in enumerate(order):
        stack: List[str] = []
        on_stack: set = set()

        def dfs(v: str):
            stack.append(v)
            on_stack.add(v)
            for w in sorted(adj.get(v, ())):
                if index[w] < s_idx:
                    continue
                if w == start and len(stack) >= 2:
                    result.append(list(stack))
                elif w not in on_stack:
                    dfs(w)
            stack.pop()
            on_stack.discard(v)

        dfs(start)
    return result


# --- streaming engine ------------------------------------------------------
@dataclass
class AppendStats:
    inserted: int = 0
    late: int = 0
    replay_from_frame: int = 0
    frames_recomputed: int = 0
    total_frames: int = 0


class StreamEngine:
    """Incremental append over a fixed event list.

    Events may arrive out of (gen, ts) order.  Append records ingest order in
    ``Event.seq``; a late event forces replay starting at the nearest
    checkpoint at or before its sorted position, then walks forward.  The
    resulting frames are guaranteed identical to a full replay.
    """

    CHECKPOINT_EVERY = 32

    def __init__(self, lock_types: Optional[Dict[str, str]] = None,
                 default_lock_type: str = MUTEX):
        self.lock_types = dict(lock_types or {})
        self.default_lock_type = default_lock_type
        self.events: List[Event] = []
        self._seq = 0
        self._ordered: List[Event] = []
        self._checkpoints: Dict[int, dict] = {}
        self._dirty_from: Optional[int] = None
        self.result: Optional[ReplayResult] = None
        self.stats_history: List[AppendStats] = []

    def append(self, events: List[Event]) -> AppendStats:
        stats = AppendStats()
        prior_ordered_len = len(self._ordered)
        late = 0
        last_ts_key = self._ordered[-1].sort_key() if self._ordered else None
        for ev in events:
            self._seq += 1
            ev.seq = self._seq
            if ev.id is None:
                ev.id = self._seq
            self.events.append(ev)
            if last_ts_key is not None and ev.sort_key() < last_ts_key:
                late += 1
        new_ordered = sorted(self.events, key=lambda e: e.sort_key())
        stats.inserted = len(events)
        stats.late = late

        # earliest sorted position of any new event
        if prior_ordered_len == 0:
            start = 0
        else:
            start = len(new_ordered)
            for ev in events:
                pos = self._lower_bound(new_ordered, ev.sort_key())
                start = min(start, pos)
        self._ordered = new_ordered
        self._recompute_from(start, stats)
        self.stats_history.append(stats)
        return stats

    @staticmethod
    def _lower_bound(ordered, key):
        lo, hi = 0, len(ordered)
        while lo < hi:
            mid = (lo + hi) // 2
            if ordered[mid].sort_key() < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _save_checkpoint(self, idx, engine: ReplayEngine):
        self._checkpoints[idx] = {
            "gens": copy.deepcopy(engine.gens),
            "anomalies": list(engine.anomalies),
            "order_edges": dict(engine.order_edges),
            "frames": list(engine.frames),
            "cycles_at": dict(engine.cycles_at),
            "signature_first": dict(engine.signature_first),
            "hold_windows": dict(engine.hold_windows),
        }

    def _restore_checkpoint(self, idx) -> ReplayEngine:
        cp = self._checkpoints[idx]
        eng = ReplayEngine(self.lock_types, self.default_lock_type)
        eng.gens = copy.deepcopy(cp["gens"])
        eng.anomalies = list(cp["anomalies"])
        eng.order_edges = dict(cp["order_edges"])
        eng.frames = list(cp["frames"])
        eng.cycles_at = dict(cp["cycles_at"])
        eng.signature_first = dict(cp["signature_first"])
        eng.hold_windows = dict(cp.get("hold_windows", {}))
        return eng

    def _recompute_from(self, start: int, stats: AppendStats):
        # choose checkpoint at or before start
        cp_idx = max([i for i in self._checkpoints if i <= start],
                     default=-1)
        if cp_idx >= 0:
            eng = self._restore_checkpoint(cp_idx)
            begin = cp_idx
            # drop checkpoints after begin
            self._checkpoints = {i: v for i, v in self._checkpoints.items()
                                 if i <= begin}
        else:
            eng = ReplayEngine(self.lock_types, self.default_lock_type)
            begin = 0
            self._checkpoints = {}

        ordered = self._ordered
        for i in range(begin, len(ordered)):
            ev = ordered[i]
            eng._apply(ev)
            cycles = detect_cycles_for_gen(eng.gstate(ev.gen))
            frame = Frame(index=len(eng.frames), event_id=_eid(ev),
                          gen=ev.gen, ts=ev.ts, kind=ev.kind,
                          cycle_ids=[c["signature"] for c in cycles])
            eng.frames.append(frame)
            if cycles:
                eng.cycles_at[frame.index] = cycles
                for c in cycles:
                    eng.signature_first.setdefault(c["signature"], frame.index)
                eng._mark_lived(gs=eng.gstate(ev.gen), cycles=cycles)
            if (i + 1) % self.CHECKPOINT_EVERY == 0:
                self._save_checkpoint(i + 1, eng)

        self.result = ReplayResult(
            gens=eng.gens, anomalies=eng.anomalies,
            order_edges=eng.order_edges, frames=eng.frames,
            events=ordered, lock_types=self.lock_types,
            cycles_at=eng.cycles_at,
            signature_first_frame=eng.signature_first,
            hold_windows=eng.hold_windows)
        stats.replay_from_frame = begin
        stats.frames_recomputed = len(ordered) - begin
        stats.total_frames = len(ordered)

    def full_result(self) -> ReplayResult:
        assert self.result is not None, "append events first"
        return self.result
