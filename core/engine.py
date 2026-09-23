"""Incremental lock-order reconstruction engine.

The engine is a deterministic event-sourced state machine.  Events are kept
in canonical ``(generation, timestamp, sequence)`` order, even when their
arrival order differs.  Every state transition is replayable, which powers
the time slider, branch comparisons, minimizer and streaming append.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Iterable

from .models import Event

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

LOCK_TYPES = ("reentrant", "mutex", "rwlock")


def lock_id(gen: int, addr: str) -> str:
    return f"g{gen}:{addr}"


@dataclass
class HeldEntry:
    mode: str = "X"
    count: int = 0
    req_seq: int | None = None
    ok_seq: int | None = None
    stack: list[str] = field(default_factory=list)


@dataclass
class Waiter:
    thread: str
    mode: str
    kind: str = "lock"  # "lock" or "cond"
    req_seq: int | None = None
    cond: str | None = None
    wait_seq: int | None = None
    notify_seq: int | None = None
    stack: list[str] = field(default_factory=list)
    grant_seq: int | None = None
    tainted: bool = False


@dataclass
class Sleeper:
    thread: str
    lock: str
    cond: str
    mode: str
    wait_seq: int
    stack: list[str]
    notified: int | None = None


@dataclass
class LockState:
    type: str = "reentrant"
    holders: dict[str, HeldEntry] = field(default_factory=dict)
    waiters: list[Waiter] = field(default_factory=list)


@dataclass
class GenState:
    locks: dict[str, LockState] = field(default_factory=dict)
    sleepers: dict[str, list[Sleeper]] = field(default_factory=dict)


class Engine:
    def __init__(self, lock_types: dict[str, str] | None = None):
        # lock_types maps a bare lock address -> declared type (per generation
        # overrides use "gen:addr" keys).
        self.lock_types: dict[str, str] = {}
        for key, value in (lock_types or {}).items():
            self.declare_type(key, value)
        self.state = self._fresh_state()
        self.events: list[Event] = []

    def _fresh_state(self) -> dict[str, Any]:
        return {
            "gens": {},
            "threads": {},
            "locks": {},
            "issues": [],
            "last_seq": -1,
        }

    def declare_type(self, key: str, value: str) -> None:
        if value not in LOCK_TYPES:
            raise ValueError(f"unsupported lock type: {value!r}")
        self.lock_types[key] = value

    def type_for(self, gen: int, addr: str) -> str:
        return self.lock_types.get(
            lock_id(gen, addr), self.lock_types.get(addr, "reentrant")
        )

    def reset(self, events: Iterable[Event]) -> None:
        self.state = self._fresh_state()
        ordered = sorted(events, key=lambda event: event.key())
        self.events = ordered
        for event in ordered:
            self.apply(event)

    def append(self, event: Event) -> None:
        key = event.key()
        index = 0
        for index, existing in enumerate(self.events):
            if existing.key() > key:
                break
        else:
            index = len(self.events)
        self.events.insert(index, event)
        self._rebuild()

    def _rebuild(self) -> None:
        self.state = self._fresh_state()
        for event in self.events:
            self.apply(event)

    # ------------------------------------------------------------------ utils
    def _gen(self, gen: int) -> GenState:
        gens = self.state["gens"]
        if gen not in gens:
            gens[gen] = GenState()
        return gens[gen]

    def _lock(self, gen: int, addr: str) -> LockState:
        gs = self._gen(gen)
        lid = lock_id(gen, addr)
        if lid not in gs.locks:
            gs.locks[lid] = LockState(type=self.type_for(gen, addr))
        lock = gs.locks[lid]
        meta = self.state["locks"]
        if lid not in meta:
            meta[lid] = {"gen": gen, "addr": addr, "type": lock.type}
        return lock

    def _thread_meta(self, thread: str) -> dict[str, Any]:
        threads = self.state["threads"]
        if thread not in threads:
            threads[thread] = {"started": False, "ended": False, "seen": False}
        return threads[thread]

    def _issue(self, kind: str, event: Event, message: str, severity: str = MEDIUM) -> None:
        self.state["issues"].append(
            {
                "kind": kind,
                "severity": severity,
                "seq": event.seq,
                "ts": event.ts,
                "gen": event.gen,
                "thread": event.thread,
                "lock": event.lock,
                "cond": event.cond,
                "message": message,
            }
        )

    def _taint_generation(self, gen: int) -> None:
        gs = self._gen(gen)
        for lock in gs.locks.values():
            for waiter in lock.waiters:
                waiter.tainted = True

    def _find_waiter(
        self, lock: LockState, thread: str, req_id: str | None, kind: str
    ) -> tuple[int, Waiter | None]:
        if req_id is not None:
            for index, waiter in enumerate(lock.waiters):
                if waiter.thread == thread and waiter.req_seq is not None:
                    candidate = self.events[waiter.req_seq] if waiter.req_seq < len(self.events) else None
                    if candidate is not None and candidate.req_id == req_id:
                        return index, waiter
        for index, waiter in enumerate(lock.waiters):
            if waiter.thread == thread and waiter.kind == kind:
                return index, waiter
        return -1, None

    def _can_grant(self, lock: LockState, waiter: Waiter) -> bool:
        holders = lock.holders
        if waiter.mode == "X":
            return all(thread == waiter.thread for thread in holders)
        if lock.type == "rwlock":
            return all(
                thread == waiter.thread or entry.mode == "R"
                for thread, entry in holders.items()
            )
        return all(thread == waiter.thread for thread in holders)

    def _grant_waiters(self, lock: LockState, event: Event) -> None:
        while lock.waiters and self._can_grant(lock, lock.waiters[0]):
            waiter = lock.waiters.pop(0)
            entry = lock.holders.get(waiter.thread)
            if entry is None:
                entry = HeldEntry()
                lock.holders[waiter.thread] = entry
            if waiter.kind == "cond":
                entry.count += 1
                if entry.mode == "R" and waiter.mode == "X":
                    entry.mode = "X"
                elif entry.count == 1:
                    entry.mode = waiter.mode
            else:
                entry.count += 1
                if entry.count == 1:
                    entry.mode = waiter.mode
                elif waiter.mode == "X":
                    entry.mode = "X"
            entry.ok_seq = event.seq
            waiter.grant_seq = event.seq

    # ------------------------------------------------------------- transitions
    def apply(self, event: Event) -> None:
        meta = self._thread_meta(event.thread)
        meta["seen"] = True
        # Out-of-order arrival detection: a later arrival slot with a smaller
        # canonical position than an already ingested event.
        if self.events and event.arrival:
            for prior in self.events:
                if prior.arrival and prior.arrival > event.arrival and prior.key() < event.key():
                    self._issue(
                        "out_of_order",
                        event,
                        f"event seq={event.seq} arrived after seq={prior.seq} but belongs earlier",
                        MEDIUM,
                    )
                    self._taint_generation(event.gen)
                    break
        self.state["last_seq"] = max(self.state["last_seq"], int(event.arrival or event.seq))

        handler = {
            "acquire_req": self._acquire_req,
            "acquire_ok": self._acquire_ok,
            "release": self._release,
            "cancel": self._cancel,
            "thread_start": self._thread_start,
            "thread_end": self._thread_end,
            "cond_wait": self._cond_wait,
            "cond_wake": self._cond_wake,
            "cond_notify": self._cond_notify,
            "cond_notify_all": self._cond_notify_all,
        }.get(event.kind)
        if handler is not None:
            handler(event)

    def _acquire_req(self, event: Event) -> None:
        if event.lock is None:
            self._issue("malformed", event, "acquire request without lock", HIGH)
            return
        meta = self._thread_meta(event.thread)
        if meta["ended"]:
            self._issue("thread_dead_request", event, "request after thread end", MEDIUM)
            self._taint_generation(event.gen)
        lock = self._lock(event.gen, event.lock)
        held = lock.holders.get(event.thread)
        own_only = all(thread == event.thread for thread in lock.holders)
        if held is not None and own_only:
            if lock.type == "mutex":
                self._issue(
                    "self_deadlock",
                    event,
                    f"thread {event.thread} re-acquired non-reentrant mutex {event.lock}",
                    HIGH,
                )
                lock.waiters.append(
                    Waiter(
                        event.thread,
                        event.mode,
                        req_seq=event.seq,
                        stack=event.stack,
                    )
                )
                return
            if lock.type == "rwlock" and held.mode == "R" and event.mode == "X":
                waiter = Waiter(
                    event.thread,
                    "X",
                    req_seq=event.seq,
                    stack=event.stack,
                )
                # Upgrade blocks while other readers hold the lock.
                if any(
                    thread != event.thread and entry.mode == "R"
                    for thread, entry in lock.holders.items()
                ):
                    lock.waiters.append(waiter)
                else:
                    held.mode = "X"
                    held.count += 1
                    held.req_seq = event.seq
                    held.ok_seq = event.seq
                    held.stack = event.stack or held.stack
                return
            # Reentrant (or compatible rwlock read) nesting is not blocked.
            held.count += 1
            if event.mode == "X":
                held.mode = "X"
            held.req_seq = event.seq
            held.stack = event.stack or held.stack
            return
        waiter = Waiter(
            event.thread,
            event.mode,
            req_seq=event.seq,
            stack=event.stack,
        )
        lock.waiters.append(waiter)
        # An immediate FIFO grant only happens for a brand-new free lock; the
        # authoritative acquire_ok event normally records the success.
        if not lock.holders and len(lock.waiters) == 1:
            self._grant_waiters(lock, event)

    def _acquire_ok(self, event: Event) -> None:
        if event.lock is None:
            self._issue("malformed", event, "acquire success without lock", HIGH)
            return
        lock = self._lock(event.gen, event.lock)
        _, waiter = self._find_waiter(
            lock, event.thread, event.req_id, "lock"
        )
        if waiter is None:
            self._issue(
                "orphan_acquire",
                event,
                f"acquire success for {event.thread} without a pending request",
                MEDIUM,
            )
            self._taint_generation(event.gen)
            waiter = Waiter(
                event.thread,
                event.mode,
                req_seq=None,
                stack=event.stack,
                tainted=True,
            )
        if not self._can_grant(lock, waiter):
            self._issue(
                "conflicting_grant",
                event,
                f"grant of {event.lock} to {event.thread} conflicts with holders",
                HIGH,
            )
            waiter.tainted = True
        try:
            position = lock.waiters.index(waiter)
        except ValueError:
            position = -1
        if position > 0:
            # Removing an out-of-order grant taints the ordering but honors it.
            for earlier in lock.waiters[:position]:
                earlier.tainted = True
            lock.waiters.pop(position)
        elif position == 0:
            lock.waiters.pop(0)
        entry = lock.holders.get(event.thread)
        if entry is None:
            entry = HeldEntry()
            lock.holders[event.thread] = entry
        entry.count += 1
        if entry.count == 1:
            entry.mode = waiter.mode
        elif waiter.mode == "X":
            entry.mode = "X"
        entry.req_seq = waiter.req_seq
        entry.ok_seq = event.seq
        entry.stack = waiter.stack or event.stack or entry.stack
        waiter.grant_seq = event.seq
        self._grant_waiters(lock, event)

    def _release(self, event: Event) -> None:
        if event.lock is None:
            self._issue("malformed", event, "release without lock", HIGH)
            return
        lock = self._lock(event.gen, event.lock)
        entry = lock.holders.get(event.thread)
        if entry is None or entry.count <= 0:
            self._issue(
                "over_release",
                event,
                f"release of {event.lock} by {event.thread} without a hold",
                HIGH,
            )
            self._taint_generation(event.gen)
            return
        entry.count -= 1
        if entry.count <= 0:
            del lock.holders[event.thread]
        self._grant_waiters(lock, event)

    def _cancel(self, event: Event) -> None:
        if event.lock is not None:
            lock = self._lock(event.gen, event.lock)
            index, waiter = self._find_waiter(
                lock, event.thread, event.req_id, "lock"
            )
            if waiter is None:
                # Cancellation may also target a condition-side contender.
                index, waiter = self._find_waiter(
                    lock, event.thread, event.req_id, "cond"
                )
            if waiter is None:
                self._issue(
                    "cancel_without_wait",
                    event,
                    f"timeout/cancel by {event.thread} without a pending wait",
                    MEDIUM,
                )
                return
            lock.waiters.pop(index)
        # Timeout while still sleeping on the condition variable.
        gs = self._gen(event.gen)
        for cond, sleepers in list(gs.sleepers.items()):
            for index, sleeper in enumerate(sleepers):
                if sleeper.thread == event.thread and (
                    event.cond is None or sleeper.cond == event.cond
                ):
                    sleepers.pop(index)
                    if not sleepers:
                        del gs.sleepers[cond]
                    break

    def _thread_start(self, event: Event) -> None:
        meta = self._thread_meta(event.thread)
        if meta["started"]:
            self._issue(
                "duplicate_thread_start",
                event,
                f"thread {event.thread} started more than once",
                MEDIUM,
            )
            self._taint_generation(event.gen)
        meta["started"] = True
        meta["ended"] = False

    def _thread_end(self, event: Event) -> None:
        meta = self._thread_meta(event.thread)
        if meta["ended"]:
            self._issue(
                "duplicate_thread_end",
                event,
                f"thread {event.thread} ended more than once",
                MEDIUM,
            )
        meta["ended"] = True
        gs = self._gen(event.gen)
        for lock in gs.locks.values():
            if any(waiter.thread == event.thread for waiter in lock.waiters):
                lock.waiters = [
                    waiter for waiter in lock.waiters if waiter.thread != event.thread
                ]
                self._grant_waiters(lock, event)
            if event.thread in lock.holders:
                del lock.holders[event.thread]
                self._issue(
                    "release_on_exit",
                    event,
                    f"thread {event.thread} terminated while holding a lock; releases inferred",
                    MEDIUM,
                )
                self._grant_waiters(lock, event)
        for cond, sleepers in list(gs.sleepers.items()):
            remaining = [s for s in sleepers if s.thread != event.thread]
            if remaining:
                gs.sleepers[cond] = remaining
            else:
                del gs.sleepers[cond]

    def _cond_wait(self, event: Event) -> None:
        if event.lock is None or event.cond is None:
            self._issue("malformed", event, "cond_wait needs lock and cond", HIGH)
            return
        lock = self._lock(event.gen, event.lock)
        entry = lock.holders.get(event.thread)
        mode = entry.mode if entry is not None and entry.count > 0 else event.mode
        count = entry.count if entry is not None and entry.count > 0 else 1
        if entry is None or entry.count <= 0:
            self._issue(
                "wait_without_lock",
                event,
                f"thread {event.thread} waited on {event.cond} without holding {event.lock}",
                MEDIUM,
            )
            self._taint_generation(event.gen)
        lock.holders.pop(event.thread, None)
        gs = self._gen(event.gen)
        gs.sleepers.setdefault(event.cond, []).append(
            Sleeper(
                thread=event.thread,
                lock=event.lock,
                cond=event.cond,
                mode=mode,
                wait_seq=event.seq,
                stack=event.stack,
            )
        )
        # Remember the reacquire depth across the atomic wait.
        setattr(
            lock,
            f"cond_depth_{event.thread}",
            {"count": count, "mode": mode, "seq": event.seq, "stack": event.stack},
        )
        self._grant_waiters(lock, event)

    def _cond_notify(self, event: Event, all_waiters: bool = False) -> None:
        if event.cond is None:
            self._issue("malformed", event, "notify without cond", HIGH)
            return
        gs = self._gen(event.gen)
        sleepers = gs.sleepers.get(event.cond, [])
        targets = list(sleepers) if all_waiters else sleepers[:1]
        if not sleepers:
            # Notifying with no waiter is legal and common.
            return
        woken: list[Sleeper] = []
        for sleeper in targets:
            sleeper.notified = event.seq
            woken.append(sleeper)
        gs.sleepers[event.cond] = [s for s in sleepers if s not in woken]
        if not gs.sleepers[event.cond]:
            del gs.sleepers[event.cond]
        for sleeper in woken:
            lock = self._lock(event.gen, sleeper.lock)
            depth = getattr(lock, f"cond_depth_{sleeper.thread}", None)
            waiter = Waiter(
                sleeper.thread,
                sleeper.mode,
                kind="cond",
                cond=sleeper.cond,
                wait_seq=sleeper.wait_seq,
                notify_seq=event.seq,
                stack=sleeper.stack,
            )
            if depth is not None and depth.get("count", 1) > 1:
                # Reacquire the remembered nesting depth as one contender.
                waiter.mode = depth["mode"]
            lock.waiters.append(waiter)
        if woken:
            lock = self._lock(event.gen, woken[0].lock)
            self._grant_waiters(lock, event)

    def _cond_notify_all(self, event: Event) -> None:
        self._cond_notify(event, all_waiters=True)

    def _cond_wake(self, event: Event) -> None:
        if event.cond is None:
            self._issue("malformed", event, "cond wake without cond", HIGH)
            return
        gs = self._gen(event.gen)
        sleeper: Sleeper | None = None
        for candidate in gs.sleepers.get(event.cond, []):
            if candidate.thread == event.thread:
                sleeper = candidate
                break
        if sleeper is not None:
            if sleeper.notified is None:
                self._issue(
                    "wake_without_notify",
                    event,
                    f"thread {event.thread} woke from {event.cond} without a notify event",
                    MEDIUM,
                )
            gs.sleepers[event.cond] = [
                s for s in gs.sleepers[event.cond] if s is not sleeper
            ]
            if not gs.sleepers[event.cond]:
                del gs.sleepers[event.cond]
        lock_addr = event.lock or (sleeper.lock if sleeper is not None else None)
        if lock_addr is None:
            self._issue("malformed", event, "cond wake without lock", HIGH)
            return
        lock = self._lock(event.gen, lock_addr)
        _, waiter = self._find_waiter(lock, event.thread, event.req_id, "cond")
        if waiter is None:
            # Wake without an observed notify: synthesize the reacquire.
            mode = sleeper.mode if sleeper is not None else event.mode
            waiter = Waiter(
                event.thread,
                mode,
                kind="cond",
                cond=event.cond,
                wait_seq=sleeper.wait_seq if sleeper is not None else None,
                notify_seq=None,
                stack=(sleeper.stack if sleeper is not None else event.stack),
                tainted=True,
            )
            if sleeper is None:
                self._issue(
                    "unmatched_wake",
                    event,
                    f"wake of {event.thread} on {event.cond} without a matching wait",
                    HIGH,
                )
            lock.waiters.append(waiter)
        depth = getattr(lock, f"cond_depth_{event.thread}", None)
        total = depth.get("count", 1) if depth else 1
        try:
            lock.waiters.remove(waiter)
        except ValueError:
            pass
        if not self._can_grant(lock, waiter):
            self._issue(
                "conflicting_grant",
                event,
                f"cond reacquire of {lock_addr} by {event.thread} conflicts",
                HIGH,
            )
            waiter.tainted = True
        entry = lock.holders.get(event.thread)
        if entry is None:
            entry = HeldEntry()
            lock.holders[event.thread] = entry
        entry.count += total
        if entry.count == total:
            entry.mode = waiter.mode
        entry.req_seq = waiter.wait_seq
        entry.ok_seq = event.seq
        entry.stack = waiter.stack or event.stack or entry.stack
        waiter.grant_seq = event.seq
        if depth is not None:
            delattr(lock, f"cond_depth_{event.thread}")
        self._grant_waiters(lock, event)

    # ----------------------------------------------------------------- output
    def _event_brief(self, seq: int | None) -> dict[str, Any] | None:
        if seq is None:
            return None
        for event in self.events:
            if event.seq == seq:
                return event.to_dict()
        return None

    def _edge_confidence(self, tainted: bool, gen: int, declared: bool) -> str:
        if tainted:
            return MEDIUM
        for issue in self.state["issues"]:
            if issue["gen"] != gen:
                continue
            if issue["kind"] in {
                "out_of_order",
                "orphan_acquire",
                "wake_without_notify",
                "release_on_exit",
            }:
                return MEDIUM
            if issue["kind"] in {"missing_release"}:
                return MEDIUM
        if not declared:
            return MEDIUM
        return HIGH

    def snapshot(self, include_missing: bool = True) -> dict[str, Any]:
        held_edges: list[dict[str, Any]] = []
        waited_edges: list[dict[str, Any]] = []
        locks_out: dict[str, dict[str, Any]] = {}
        for gen, gs in self.state["gens"].items():
            for lid, lock in gs.locks.items():
                addr = self.state["locks"][lid]["addr"]
                declared = lid in self._declared_ids(gen, addr)
                locks_out[lid] = {
                    "id": lid,
                    "gen": gen,
                    "addr": addr,
                    "type": lock.type,
                    "declared": lock.type in self._declared_types_for(gen, addr),
                }
                for thread, entry in lock.holders.items():
                    confidence = self._edge_confidence(False, gen, declared)
                    hold_events = [
                        self._event_brief(entry.req_seq),
                        self._event_brief(entry.ok_seq),
                    ]
                    held_edges.append(
                        {
                            "id": f"held-{lid}-{thread}",
                            "kind": "held",
                            "gen": gen,
                            "lock": lid,
                            "thread": thread,
                            "mode": entry.mode,
                            "depth": entry.count,
                            "confidence": confidence,
                            "hold_stack": list(entry.stack),
                            "events": [event for event in hold_events if event],
                        }
                    )
                for waiter in lock.waiters:
                    confidence = self._edge_confidence(
                        waiter.tainted, gen, declared
                    )
                    origin = waiter.wait_seq if waiter.kind == "cond" else waiter.req_seq
                    waited_edges.append(
                        {
                            "id": f"wait-{lid}-{waiter.thread}-{origin}",
                            "kind": "wait",
                            "wait_kind": waiter.kind,
                            "gen": gen,
                            "lock": lid,
                            "thread": waiter.thread,
                            "mode": waiter.mode,
                            "cond": waiter.cond,
                            "confidence": confidence,
                            "wait_stack": list(waiter.stack),
                            "events": [
                                event
                                for event in (
                                    self._event_brief(waiter.req_seq),
                                    self._event_brief(waiter.wait_seq),
                                    self._event_brief(waiter.notify_seq),
                                )
                                if event
                            ],
                        }
                    )
        threads = {
            tid: {
                "id": tid,
                "started": meta["started"],
                "ended": meta["ended"],
            }
            for tid, meta in self.state["threads"].items()
        }
        sleepers_out = []
        for gen, gs in self.state["gens"].items():
            for cond, sleepers in gs.sleepers.items():
                for sleeper in sleepers:
                    sleepers_out.append(
                        {
                            "gen": gen,
                            "cond": cond,
                            "thread": sleeper.thread,
                            "lock": lock_id(gen, sleeper.lock),
                            "wait_event": self._event_brief(sleeper.wait_seq),
                            "notified": self._event_brief(sleeper.notified)
                            is not None,
                        }
                    )
        issues = [dict(issue) for issue in self.state["issues"]]
        if include_missing:
            for edge in held_edges:
                if not threads.get(edge["thread"], {}).get("ended"):
                    issues.append(
                        {
                            "kind": "missing_release",
                            "severity": MEDIUM,
                            "seq": None,
                            "gen": edge["gen"],
                            "thread": edge["thread"],
                            "lock": edge["lock"],
                            "message": f"thread {edge['thread']} ended still holding {edge['lock']}",
                        }
                    )
        cycles = find_cycles(held_edges, waited_edges)
        return {
            "threads": threads,
            "locks": locks_out,
            "held_edges": held_edges,
            "waited_edges": waited_edges,
            "sleepers": sleepers_out,
            "cycles": cycles,
            "issues": issues,
        }

    def _declared_ids(self, gen: int, addr: str) -> set[str]:
        ids = set()
        if lock_id(gen, addr) in self.lock_types:
            ids.add(lock_id(gen, addr))
        if addr in self.lock_types:
            ids.add(lock_id(gen, addr))
        return ids

    def _declared_types_for(self, gen: int, addr: str) -> set[str]:
        values = set()
        if addr in self.lock_types:
            values.add(self.lock_types[addr])
        if lock_id(gen, addr) in self.lock_types:
            values.add(self.lock_types[lock_id(gen, addr)])
        return values
