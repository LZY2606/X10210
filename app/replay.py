"""Replay engine.

Replays an ordered event stream and maintains, at every point in time:

* a *hold graph*  thread -> lock frames (each frame links back to its events);
* a *wait graph*  thread -> holders it is blocked on, with confidence.

The engine never invents a ``release``. Missing or out-of-order arrivals are
recorded as anomalies; affected edges carry ``low`` confidence. Condition waits
park the frame, notifies move the parked thread back into lock contention, and
timeouts / thread termination cancel waits.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .model import (
    Event,
    ACQ_REQ, ACQ_OK, RELEASE, ACQ_TIMEOUT, THREAD_START, THREAD_END,
    COND_WAIT, COND_NOTIFY, COND_NOTIFY_ALL, COND_REACQ, PROCESS_START,
    LOCK_TYPE, MUTEX, RWLOCK,
)

HIGH = "high"
MEDIUM = "medium"
LOW = "low"


@dataclass
class Frame:
    """One held-lock acquisition of one thread (a stack entry)."""
    lock_key: str
    lock_addr: str
    gen: int
    mode: str
    acq_req_seq: int
    acq_req_ts: float
    acq_ok_seq: Optional[int] = None
    acq_ok_ts: Optional[float] = None
    inferred_grant: bool = False
    confidence: str = HIGH
    cond_park: Optional[dict] = None   # set while parked in condition_wait
    parked: bool = False              # temporarily released by cond_wait

    def to_dict(self) -> dict:
        return {
            "lock": self.lock_key, "lock_addr": self.lock_addr, "gen": self.gen,
            "mode": self.mode, "acq_req_seq": self.acq_req_seq,
            "acq_req_ts": self.acq_req_ts, "acq_ok_seq": self.acq_ok_seq,
            "acq_ok_ts": self.acq_ok_ts, "inferred_grant": self.inferred_grant,
            "confidence": self.confidence,
            "producing_events": self.event_refs(),
        }

    def event_refs(self) -> List[int]:
        refs = [self.acq_req_seq]
        if self.acq_ok_seq is not None:
            refs.append(self.acq_ok_seq)
        return refs


@dataclass
class WaitEntry:
    """A pending request: plain acquire, parked condition wait, or reacquire."""
    thread: str
    lock_key: str
    lock_addr: str
    gen: int
    mode: str
    kind: str                 # "acquire" | "cond_park" | "cond_reacq"
    start_seq: int
    start_ts: float
    cv_key: Optional[str] = None
    cv_addr: Optional[str] = None
    cond_wait_seq: Optional[int] = None
    frame: Optional[Frame] = None
    notify_seq: Optional[int] = None
    confidence: str = HIGH

    def producing_events(self) -> List[int]:
        refs = [self.start_seq]
        if self.cond_wait_seq is not None and self.cond_wait_seq != self.start_seq:
            refs.append(self.cond_wait_seq)
        if self.notify_seq is not None:
            refs.append(self.notify_seq)
        return refs


@dataclass
class LockState:
    key: str
    addr: str
    gen: int
    type_name: str = MUTEX
    recursive: bool = True
    declared: bool = False
    holders: Dict[str, List[Frame]] = field(default_factory=dict)  # thread -> stack
    wait_queue: List[str] = field(default_factory=list)

    def is_rw(self) -> bool:
        return self.type_name == RWLOCK

    def holder_modes(self) -> List[str]:
        return [frame.mode for stack in self.holders.values() for frame in stack]


@dataclass
class ThreadState:
    tid: str
    alive: bool = True
    started: bool = False
    start_seq: Optional[int] = None
    end_seq: Optional[int] = None
    hold_stack: List[Frame] = field(default_factory=list)
    holds: Dict[str, List[Frame]] = field(default_factory=dict)
    waiting: Optional[WaitEntry] = None


class Replayer:
    def __init__(self, events: Optional[List[Event]] = None):
        self.events: List[Event] = []
        self.locks: Dict[str, LockState] = {}
        self.threads: Dict[str, ThreadState] = {}
        self.cv_parked: Dict[str, List[WaitEntry]] = {}   # cv key -> parked
        self.anomalies: List[dict] = []
        self.snapshots: Dict[int, dict] = {}
        self.timeline: List[dict] = []
        self.processed = 0
        self.max_gen = 0
        self._last_ts: Optional[float] = None
        self._last_ts_seq: Optional[int] = None
        self._cycle_live: Dict[str, dict] = {}
        self._active_cycles: Dict[str, dict] = {}
        if events:
            self.append_events(events)

    # ---- basic lookups ----------------------------------------------------

    def _thread(self, tid: str) -> ThreadState:
        state = self.threads.get(tid)
        if state is None:
            state = ThreadState(tid=tid)
            self.threads[tid] = state
        return state

    def _lock(self, key: str, addr: str, gen: int) -> LockState:
        state = self.locks.get(key)
        if state is None:
            state = LockState(key=key, addr=addr, gen=gen)
            self.locks[key] = state
        return state

    def _anomaly(self, code: str, seq: int, message: str,
                 thread: Optional[str] = None, lock: Optional[str] = None,
                 confidence: str = LOW) -> None:
        self.anomalies.append({
            "code": code, "seq": seq, "message": message,
            "thread": thread, "lock": lock, "confidence": confidence,
        })

    # ---- event ingestion --------------------------------------------------

    def append_events(self, events: List[Event]) -> None:
        """Add events, preserving collection order (re-sorted by seq)."""
        self.events.extend(events)
        self.events.sort(key=lambda ev: (ev.seq, ev.batch, getattr(ev, "_idx", 0)))

    def run(self, upto: Optional[int] = None) -> None:
        """Process events through seq ``upto`` inclusive."""
        while self.processed < len(self.events):
            event = self.events[self.processed]
            if upto is not None and event.seq > upto:
                break
            self._step(event)
            self.processed += 1
            self._record_cycle_changes(event)
            self.snapshots[event.seq] = self._snapshot(event.seq)

    # ---- per-event dispatch ----------------------------------------------

    def _step(self, event: Event) -> None:
        self._check_order(event)
        dispatch = {
            ACQ_REQ: self._on_acquire_request,
            ACQ_OK: self._on_acquire_ok,
            RELEASE: self._on_release,
            ACQ_TIMEOUT: self._on_timeout,
            THREAD_START: self._on_thread_start,
            THREAD_END: self._on_thread_end,
            COND_WAIT: self._on_cond_wait,
            COND_NOTIFY: lambda ev: self._on_cond_notify(ev, all_waiters=False),
            COND_NOTIFY_ALL: lambda ev: self._on_cond_notify(ev, all_waiters=True),
            COND_REACQ: self._on_cond_reacq,
            PROCESS_START: self._on_process_start,
            LOCK_TYPE: self._on_lock_type,
        }
        handler = dispatch.get(event.kind)
        if handler is not None:
            handler(event)

    def _check_order(self, event: Event) -> None:
        if self._last_ts is not None and event.ts and event.ts < self._last_ts:
            self._anomaly(
                "timestamp_out_of_order", event.seq,
                f"ts {event.ts} precedes earlier ts {self._last_ts} "
                f"(seq {self._last_ts_seq}); events keep collection order",
                thread=event.thread, confidence=MEDIUM,
            )
        if event.ts:
            self._last_ts, self._last_ts_seq = event.ts, event.seq
        if self.processed and event.seq < self.events[self.processed - 1].seq:
            self._anomaly(
                "seq_out_of_order", event.seq,
                "arrived after an event with a larger seq; replayed by seq",
                thread=event.thread, confidence=MEDIUM,
            )

    def _on_process_start(self, event: Event) -> None:
        self.max_gen += 1
        # The event itself pins the generation for following same-address locks.
        event.gen = max(event.gen, self.max_gen)

    def _on_thread_start(self, event: Event) -> None:
        if not event.thread:
            self._anomaly("missing_thread", event.seq,
                          "thread_start without thread id")
            return
        state = self._thread(event.thread)
        if state.started:
            self._anomaly("duplicate_thread_start", event.seq,
                          f"thread {event.thread} started twice",
                          thread=event.thread, confidence=MEDIUM)
        state.started = True
        state.alive = True
        state.start_seq = event.seq

    def _on_lock_type(self, event: Event) -> None:
        if not event.lock:
            self._anomaly("missing_lock", event.seq,
                          "lock_type declaration without lock address")
            return
        key = event.lock_key()
        lock = self._lock(key, event.lock, event.gen)
        lock.type_name = event.lock_type or MUTEX
        lock.recursive = bool(event.recursive)
        lock.declared = True

    # ---- acquire / release -----------------------------------------------

    def _on_acquire_request(self, event: Event) -> None:
        if not event.thread or not event.lock:
            self._anomaly("missing_thread_or_lock", event.seq,
                          "acquire_request needs thread and lock")
            return
        thread = self._thread(event.thread)
        lock = self._lock(event.lock_key(), event.lock, event.gen)
        if not lock.declared and event.mode == "S":
            lock.type_name = RWLOCK
        if thread.waiting is not None:
            self._anomaly(
                "overlapping_requests", event.seq,
                f"thread {thread.tid} requests {lock.key} while already waiting",
                thread=thread.tid, lock=lock.key, confidence=MEDIUM,
            )
        # Every request parks as pending until acquire_ok is observed.
        # Reentrancy / upgrade compatibility is decided analytically, so
        # nesting stays paired with its own acquire_ok.
        entry = WaitEntry(
            thread=thread.tid, lock_key=lock.key, lock_addr=lock.addr,
            gen=lock.gen, mode=event.mode, kind="acquire",
            start_seq=event.seq, start_ts=event.ts,
        )
        blockers = self._blocked_by(thread, lock, event.mode)
        entry.confidence = LOW if blockers is None else HIGH
        thread.waiting = entry
        if thread.tid not in lock.wait_queue:
            lock.wait_queue.append(thread.tid)

    @staticmethod
    def _find_frame(thread: ThreadState, lock_key: str) -> Optional[Frame]:
        stack = thread.holds.get(lock_key)
        return stack[-1] if stack else None

    def _on_acquire_ok(self, event: Event) -> None:
        if not event.thread or not event.lock:
            self._anomaly("missing_thread_or_lock", event.seq,
                          "acquire_ok needs thread and lock")
            return
        thread = self._thread(event.thread)
        lock = self._lock(event.lock_key(), event.lock, event.gen)
        entry = thread.waiting
        new_frame = Frame(
            lock_key=lock.key, lock_addr=lock.addr, gen=lock.gen,
            mode=event.mode, acq_req_seq=event.seq, acq_req_ts=event.ts,
            acq_ok_seq=event.seq, acq_ok_ts=event.ts,
        )
        if entry is None or entry.lock_key != lock.key:
            # Success with no matching request: infer the request, do not
            # fabricate a release or pretend ordering was perfect.
            self._anomaly(
                "acquire_ok_without_request", event.seq,
                f"acquire_ok for {thread.tid} on {lock.key} has no pending "
                "acquire_request; request inferred at low confidence",
                thread=thread.tid, lock=lock.key, confidence=LOW,
            )
            new_frame.acq_req_seq = event.seq
            new_frame.confidence = LOW
            self._attach_frame(thread, lock, new_frame)
            return

        new_frame.mode = entry.mode or event.mode
        new_frame.acq_req_seq = entry.start_seq
        new_frame.acq_req_ts = entry.start_ts
        blockers = self._blocked_by(thread, lock, new_frame.mode)
        if blockers:
            self._anomaly(
                "acquire_ok_while_blocked", event.seq,
                f"{thread.tid} reports success on {lock.key} while holders "
                f"{sorted(blockers)} conflict; recorded with low confidence",
                thread=thread.tid, lock=lock.key, confidence=LOW,
            )
            new_frame.confidence = LOW
        thread.waiting = None
        if thread.tid in lock.wait_queue:
            lock.wait_queue.remove(thread.tid)
        self._attach_frame(thread, lock, new_frame)

    @staticmethod
    def _attach_frame(thread: ThreadState, lock: LockState, frame: Frame) -> None:
        thread.hold_stack.append(frame)
        thread.holds.setdefault(lock.key, []).append(frame)
        lock.holders.setdefault(thread.tid, []).append(frame)

    def _on_release(self, event: Event) -> None:
        if not event.thread or not event.lock:
            self._anomaly("missing_thread_or_lock", event.seq,
                          "release needs thread and lock")
            return
        thread = self._thread(event.thread)
        lock = self._lock(event.lock_key(), event.lock, event.gen)
        stack = thread.holds.get(lock.key)
        if not stack:
            # Never invent an acquire to balance this; just record it.
            self._anomaly(
                "release_without_hold", event.seq,
                f"release by {thread.tid} for {lock.key} it does not hold; "
                "no acquire fabricated",
                thread=thread.tid, lock=lock.key, confidence=LOW,
            )
            return
        frame = stack.pop()
        thread.hold_stack.remove(frame)
        holder_stack = lock.holders.get(thread.tid)
        if holder_stack and frame in holder_stack:
            holder_stack.remove(frame)
            if not holder_stack:
                del lock.holders[thread.tid]
        if not stack:
            del thread.holds[lock.key]

    def _on_timeout(self, event: Event) -> None:
        if not event.thread:
            self._anomaly("missing_thread", event.seq,
                          "acquire_timeout needs thread")
            return
        thread = self._thread(event.thread)
        entry = thread.waiting
        if entry is None:
            self._anomaly(
                "timeout_without_request", event.seq,
                f"timeout for {thread.tid} with no pending wait",
                thread=thread.tid, confidence=MEDIUM,
            )
            return
        if entry.kind == "cond_park":
            self._anomaly(
                "timeout_during_condition_wait", event.seq,
                f"timeout for {thread.tid} while parked on condition "
                f"{entry.cv_key}; wait cancelled without reacquire",
                thread=thread.tid, lock=entry.lock_key, confidence=MEDIUM,
            )
            self._remove_parked(entry)
        lock = self.locks.get(entry.lock_key)
        if lock and thread.tid in lock.wait_queue:
            lock.wait_queue.remove(thread.tid)
        thread.waiting = None

    @staticmethod
    def _remove_parked(entry: WaitEntry) -> None:
        # Parked entries are not kept on cv_parked after reacquire starts;
        # this is a safety cleanup for cancelled parks.
        return None

    # ---- condition variables ---------------------------------------------

    def _on_cond_wait(self, event: Event) -> None:
        if not event.thread or not event.lock:
            self._anomaly("missing_thread_or_lock", event.seq,
                          "condition_wait needs thread and lock")
            return
        thread = self._thread(event.thread)
        lock = self._lock(event.lock_key(), event.lock, event.gen)
        stack = thread.holds.get(lock.key)
        if not stack:
            self._anomaly(
                "condition_wait_without_hold", event.seq,
                f"{thread.tid} waits on condition without holding {lock.key}",
                thread=thread.tid, lock=lock.key, confidence=MEDIUM,
            )
            entry = WaitEntry(
                thread=thread.tid, lock_key=lock.key, lock_addr=lock.addr,
                gen=lock.gen, mode="X", kind="cond_park",
                start_seq=event.seq, start_ts=event.ts,
                cv_key=event.cv_key(), cv_addr=event.cv or event.lock,
                cond_wait_seq=event.seq, confidence=LOW,
            )
            self._park(entry)
            thread.waiting = entry
            return
        frame = stack[-1]
        # Atomic condition-wait semantics: temporarily drop out of the
        # lock's holder set, but keep the paired frame on the thread stack.
        holder_stack = lock.holders.get(thread.tid)
        if holder_stack and frame in holder_stack:
            holder_stack.remove(frame)
            if not holder_stack:
                del lock.holders[thread.tid]
        frame.parked = True
        park = {
            "seq": event.seq, "ts": event.ts,
            "cv": event.cv_key(), "released_frame": frame.to_dict(),
        }
        frame.cond_park = park
        entry = WaitEntry(
            thread=thread.tid, lock_key=lock.key, lock_addr=lock.addr,
            gen=lock.gen, mode=frame.mode, kind="cond_park",
            start_seq=event.seq, start_ts=event.ts,
            cv_key=event.cv_key(), cv_addr=event.cv or event.lock,
            cond_wait_seq=event.seq, frame=frame,
        )
        self._park(entry)
        thread.waiting = entry

    def _park(self, entry: WaitEntry) -> None:
        self.cv_parked.setdefault(entry.cv_key, []).append(entry)

    def _on_cond_notify(self, event: Event, all_waiters: bool) -> None:
        cv_key = event.cv_key()
        parked = self.cv_parked.get(cv_key, []) if cv_key else []
        if not parked:
            self._anomaly(
                "notify_without_waiter", event.seq,
                f"notify on {cv_key} with no parked waiter",
                thread=event.thread, confidence=MEDIUM,
            )
            return
        chosen = list(parked) if all_waiters else parked[:1]
        for entry in chosen:
            parked.remove(entry)
            lock = self.locks.get(entry.lock_key)
            if lock and entry.thread not in lock.wait_queue:
                lock.wait_queue.append(entry.thread)
            waiter = self.threads.get(entry.thread)
            if waiter is None:
                continue
            entry.kind = "cond_reacq"
            entry.notify_seq = event.seq
            # Re-contend the lock; edge computed analytically at snapshot time.
            waiter.waiting = entry

    def _on_cond_reacq(self, event: Event) -> None:
        if not event.thread or not event.lock:
            self._anomaly("missing_thread_or_lock", event.seq,
                          "condition_reacq needs thread and lock")
            return
        thread = self._thread(event.thread)
        lock = self._lock(event.lock_key(), event.lock, event.gen)
        entry = thread.waiting
        if entry is None or entry.kind not in ("cond_reacq", "cond_park"):
            self._anomaly(
                "reacq_without_wait", event.seq,
                f"condition_reacq for {thread.tid} has no active condition wait",
                thread=thread.tid, lock=lock.key, confidence=MEDIUM,
            )
            return
        frame = entry.frame
        blockers = self._blocked_by(thread, lock, entry.mode)
        if blockers:
            # Lock not available yet: stay in contention, flag uncertainty.
            entry.confidence = LOW
            self._anomaly(
                "reacq_while_blocked", event.seq,
                f"{thread.tid} condition_reacq on {lock.key} while held by "
                f"{sorted(blockers)}",
                thread=thread.tid, lock=lock.key, confidence=LOW,
            )
            return
        thread.waiting = None
        if thread.tid in lock.wait_queue:
            lock.wait_queue.remove(thread.tid)
        if frame is None:
            # condition_wait arrived without a held lock; infer reacquisition.
            frame = Frame(
                lock_key=lock.key, lock_addr=lock.addr, gen=lock.gen,
                mode=entry.mode, acq_req_seq=entry.cond_wait_seq or event.seq,
                acq_req_ts=entry.start_ts, confidence=LOW,
            )
        # ... and re-acquire after wakeup.
        frame.cond_park = None
        frame.parked = False
        frame.acq_ok_seq = event.seq
        frame.acq_ok_ts = event.ts
        if frame not in thread.hold_stack:
            thread.hold_stack.append(frame)
        thread.holds.setdefault(lock.key, [])
        if frame not in thread.holds[lock.key]:
            thread.holds[lock.key].append(frame)
        lock.holders.setdefault(thread.tid, [])
        if frame not in lock.holders[thread.tid]:
            lock.holders[thread.tid].append(frame)

    def _on_thread_end(self, event: Event) -> None:
        if not event.thread:
            self._anomaly("missing_thread", event.seq,
                          "thread_end needs thread")
            return
        thread = self._thread(event.thread)
        if not thread.alive and thread.end_seq is not None:
            self._anomaly("duplicate_thread_end", event.seq,
                          f"thread {thread.tid} ended twice",
                          thread=thread.tid, confidence=MEDIUM)
        # Termination cancels any pending wait (acquire or condition).
        if thread.waiting is not None:
            entry = thread.waiting
            lock = self.locks.get(entry.lock_key)
            if lock and thread.tid in lock.wait_queue:
                lock.wait_queue.remove(thread.tid)
            if entry.kind == "cond_park":
                parked = self.cv_parked.get(entry.cv_key or "", [])
                if entry in parked:
                    parked.remove(entry)
            self._anomaly(
                "wait_cancelled_by_thread_end", event.seq,
                f"{thread.tid} exited while waiting on {entry.lock_key}",
                thread=thread.tid, lock=entry.lock_key, confidence=MEDIUM,
            )
            thread.waiting = None
        # Held locks at thread end are leaked; never invent a release.
        leaked = [f.lock_key for stack in thread.holds.values() for f in stack]
        for lock_key in sorted(set(leaked)):
            self._anomaly(
                "lock_held_at_thread_end", event.seq,
                f"{thread.tid} ended while holding {lock_key}; "
                "no release fabricated",
                thread=thread.tid, lock=lock_key, confidence=LOW,
            )
        thread.alive = False
        thread.end_seq = event.seq

    # ---- blocking analysis -----------------------------------------------

    def _blocked_by(self, waiter: ThreadState, lock: LockState,
                    mode: str) -> Optional[List[str]]:
        """Threads currently preventing ``waiter`` from taking ``lock``.

        Returns an empty list when the request is compatible (acquired
        immediately, e.g. reentrant / shared read). ``None`` marks an
        analytically undecidable situation (low confidence edge).
        """
        holders = lock.holders
        my_frames = holders.get(waiter.tid, [])
        blockers: List[str] = []
        if lock.is_rw():
            other_modes = [
                (tid, frame.mode)
                for tid, stack in holders.items()
                for frame in stack
                if tid != waiter.tid
            ]
            want_x = mode == "X"
            own = [f.mode for f in my_frames]
            if waiter.tid in holders and not other_modes:
                if want_x and all(m == "S" for m in own):
                    return []          # S -> X upgrade by sole owner
                if all(m == mode for m in own) or not want_x:
                    return []          # reentrant same slot / X holder reads
            if want_x:
                blockers = [tid for tid, m in other_modes]
            else:
                blockers = [tid for tid, m in other_modes if m == "X"]
            return sorted(set(blockers))
        # Plain mutex.
        own = holders.get(waiter.tid)
        other = [tid for tid in holders if tid != waiter.tid]
        if other:
            return sorted(other)
        if own:
            return [] if lock.recursive else [waiter.tid]
        return []

    def _edge_confidence(self, entry: WaitEntry, blockers: List[str]) -> str:
        if entry.confidence == LOW:
            return LOW
        for tid in blockers:
            target = self.threads.get(tid)
            if target is not None and (not target.alive or target.end_seq is not None):
                return LOW
        for anomaly in self.anomalies[-12:]:
            if anomaly["code"] in ("seq_out_of_order", "timestamp_out_of_order"):
                return MEDIUM
        return HIGH

    # ---- snapshots / graph views -----------------------------------------

    def _wait_view(self) -> Dict[str, dict]:
        """Current wait graph: one node per waiting thread, with edges."""
        view: Dict[str, dict] = {}
        for tid, thread in self.threads.items():
            entry = thread.waiting
            if entry is None or entry.kind == "cond_park":
                continue
            lock = self.locks.get(entry.lock_key)
            blockers = self._blocked_by(thread, lock, entry.mode) if lock else []
            edges = []
            for holder_tid in blockers or []:
                holder = self.threads.get(holder_tid)
                holder_stack = lock.holders.get(holder_tid, []) if lock else []
                edges.append({
                    "from": tid,
                    "to": holder_tid,
                    "lock": entry.lock_key,
                    "lock_addr": entry.lock_addr,
                    "gen": entry.gen,
                    "mode": entry.mode,
                    "kind": entry.kind,
                    "confidence": self._edge_confidence(entry, blockers),
                    "waiting_since_seq": entry.start_seq,
                    "waiting_since_ts": entry.start_ts,
                    "producing_events": entry.producing_events(),
                    "waiter_stack": [f.to_dict() for f in thread.hold_stack],
                    "holder_stack": [f.to_dict() for f in holder_stack],
                    "holder_ended": bool(
                        holder is not None and holder.end_seq is not None),
                    "cv": entry.cv_key,
                })
            view[tid] = {
                "thread": tid,
                "lock": entry.lock_key,
                "mode": entry.mode,
                "kind": entry.kind,
                "since_seq": entry.start_seq,
                "confidence": entry.confidence,
                "edges": edges,
            }
        return view

    def _hold_view(self) -> Dict[str, dict]:
        nodes: Dict[str, dict] = {}
        for tid, thread in self.threads.items():
            frames = [f for f in thread.hold_stack if not f.parked]
            if not frames:
                continue
            nodes[tid] = {
                "thread": tid,
                "alive": thread.alive,
                "end_seq": thread.end_seq,
                "frames": [f.to_dict() for f in frames],
                "edges": [
                    {
                        "from": tid,
                        "lock": f.lock_key,
                        "mode": f.mode,
                        "confidence": f.confidence,
                        "producing_events": f.event_refs(),
                        "parked": f.parked,
                    }
                    for f in frames
                ],
            }
        return nodes

    def _parked_view(self) -> List[dict]:
        out = []
        for cv_key, entries in self.cv_parked.items():
            for entry in entries:
                out.append({
                    "thread": entry.thread,
                    "cv": cv_key,
                    "lock": entry.lock_key,
                    "since_seq": entry.start_seq,
                    "producing_events": entry.producing_events(),
                })
        return out

    def _snapshot(self, seq: int) -> dict:
        from .cycles import detect_cycles
        wait_view = self._wait_view()
        cycles = detect_cycles(wait_view)
        return {
            "seq": seq,
            "holds": self._hold_view(),
            "waits": wait_view,
            "parked": self._parked_view(),
            "cycles": cycles,
        }

    # ---- cycle lifecycle / timeline --------------------------------------

    @staticmethod
    def _cycle_sig(cycle: dict) -> str:
        edges = tuple(sorted(
            (edge["from"], edge["to"], edge["lock"]) for edge in cycle["edges"]
        ))
        return repr(edges)

    def _record_cycle_changes(self, event: Event) -> None:
        snapshot = self.snapshots  # noqa: F841 (snapshots recorded after this)
        current = {}
        for cycle in self._current_cycles():
            sig = self._cycle_sig(cycle)
            current[sig] = cycle
        for sig, cycle in current.items():
            if sig not in self._active_cycles:
                record = {
                    "signature": sig,
                    "threads": cycle["threads"],
                    "locks": cycle["locks"],
                    "first_seq": event.seq,
                    "first_ts": event.ts,
                    "last_seq": event.seq,
                    "status": "live",
                    "evidence": cycle,
                }
                self.timeline.append({
                    "seq": event.seq, "ts": event.ts,
                    "kind": "cycle_start", "signature": sig,
                    "threads": cycle["threads"], "locks": cycle["locks"],
                    "status": "live", "label": self._cycle_label(cycle),
                })
                self._active_cycles[sig] = record
            else:
                self._active_cycles[sig]["last_seq"] = event.seq
        gone = [sig for sig in self._active_cycles if sig not in current]
        for sig in gone:
            record = self._active_cycles.pop(sig)
            record["last_seq"] = event.seq
            record["status"] = "near_miss"
            record["near_miss_seq"] = event.seq
            self.timeline.append({
                "seq": event.seq, "ts": event.ts,
                "kind": "cycle_end", "signature": sig,
                "threads": record["threads"], "locks": record["locks"],
                "status": "near_miss", "label": self._cycle_label_from(record),
                "first_seq": record["first_seq"],
            })

    def _current_cycles(self) -> List[dict]:
        from .cycles import detect_cycles
        return detect_cycles(self._wait_view())

    @staticmethod
    def _cycle_label(cycle: dict) -> str:
        threads = cycle["threads"]
        return " -> ".join(threads + threads[:1])

    @staticmethod
    def _cycle_label_from(record: dict) -> str:
        threads = record["threads"]
        return " -> ".join(threads + threads[:1])

    # ---- public API -------------------------------------------------------

    def snapshot_at(self, seq: Optional[int] = None) -> dict:
        """Replay (from scratch if needed) up to ``seq`` and return snapshot."""
        target = self.events[-1].seq if (seq is None and self.events) else seq
        if self.snapshots and target in self.snapshots:
            return self.snapshots[target]
        # Rewind-and-replay view: cheap enough for trace sizes in scope, and
        # guarantees snapshots are never stitched across a mutation.
        fresh = Replayer()
        fresh.append_events(self.events)
        fresh.run(target)
        return fresh.snapshots[target] if target in fresh.snapshots else {
            "seq": target or 0, "holds": {}, "waits": {}, "parked": [],
            "cycles": [],
        }

    def end_state(self) -> dict:
        if not self.events:
            return {"seq": 0, "holds": {}, "waits": {}, "parked": [],
                    "cycles": [], "anomalies": [], "timeline": []}
        self.run()
        last_seq = self.events[-1].seq
        snap = self.snapshots[last_seq]
        return {
            **snap,
            "anomalies": list(self.anomalies),
            "timeline": list(self.timeline),
            "threads": {
                tid: {
                    "alive": state.alive, "started": state.started,
                    "start_seq": state.start_seq, "end_seq": state.end_seq,
                    "waiting": state.waiting.kind if state.waiting else None,
                }
                for tid, state in self.threads.items()
            },
        }
