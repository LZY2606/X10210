"""Reconstruction engine: replay events into hold/wait graphs.

The replayer never fabricates events. Missing or out-of-order evidence is
reported through ``confidence`` entries and per-edge ``confidence`` levels.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .models import Event


@dataclass
class Req:
    """An outstanding acquire request (normal or condition-reacquire)."""

    thread: str
    lock: str
    mode: str
    req_event: int
    kind: str = "normal"  # normal | reacq
    cond: Optional[str] = None
    cond_wait_event: Optional[int] = None
    hold_stack: List[str] = field(default_factory=list)
    hold_refs: List[int] = field(default_factory=list)
    granted: bool = False
    granted_event: Optional[int] = None
    canceled_event: Optional[int] = None
    timeout: bool = False
    start_ts: float = 0.0
    end_ts: Optional[float] = None


@dataclass
class Susp:
    """A thread parked in condition wait between release and reacquire."""

    thread: str
    lock: str
    mode: str
    cond: Optional[str]
    wait_event: int
    start_ts: float
    notified: bool = False
    notify_event: Optional[int] = None
    reacq_req: Optional[int] = None
    done_event: Optional[int] = None
    end_ts: Optional[float] = None
    hold_refs: List[int] = field(default_factory=list)


@dataclass
class Own:
    owner: str
    mode: str  # X | S
    depth: int = 1
    grants: List[int] = field(default_factory=list)
    holders: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class Replayer:
    def __init__(
        self,
        events: List[Event],
        lock_types: Optional[Dict[str, str]] = None,
        generation: Optional[int] = None,
    ) -> None:
        self.events = sorted(events, key=lambda e: (e.generation, e.timestamp, e.seq, e.id))
        self.lock_types = dict(lock_types or {})
        gens = sorted({e.generation for e in self.events})
        self.generation = generation if generation is not None else (gens[-1] if gens else 0)
        evs = [e for e in self.events if e.generation == self.generation]
        self.ordered: List[Event] = evs
        self.by_id: Dict[int, Event] = {e.id: e}

        self.owns: Dict[str, Own] = {}
        self.waiters: Dict[str, List[Req]] = {}
        self.cond: Dict[Tuple[str, str], Susp] = {}
        self.alive: Set[str] = set()
        self.warnings: List[Dict[str, Any]] = []
        self.index_by_event: Dict[int, Dict[str, Any]] = {}

        # pairing/linkage built during replay (for reduction & swimlanes)
        self.pairs: Dict[int, List[int]] = {}
        self.cond_pairs: Dict[int, List[int]] = {}
        self.pair_group: Dict[int, int] = {}
        self._group = 0

        # timeline intervals
        self.hold_intervals: List[Dict[str, Any]] = []
        self.wait_intervals: List[Dict[str, Any]] = []
        self.cond_intervals: List[Dict[str, Any]] = []
        self._open_holds: Dict[Tuple[str, str], Dict[str, Any]] = {}

        self.cycle_intervals: List[Dict[str, Any]] = []
        self._active_cycles: Dict[str, Dict[str, Any]] = {}

        self.cursor = 0
        self.locked_declared = False
        self._replay_all()

    # ------------------------------------------------------------------ utils
    def _warn(self, code: str, message: str, event_id: Optional[int] = None) -> None:
        self.warnings.append({"code": code, "message": message, "event_id": event_id})

    def _ltype(self, lock: str) -> str:
        return self.lock_types.get(lock, "mutex")

    def _is_rw(self, lock: str) -> bool:
        return self._ltype(lock) == "rwlock"

    def _declared(self, lock: str) -> bool:
        return lock in self.lock_types

    def _owner_of(self, lock: str) -> Optional[str]:
        own = self.owns.get(lock)
        if own is None:
            return None
        if own.mode == "S":
            holders = [t for t, h in own.holders.items() if h["depth"] > 0]
            if len(holders) == 1:
                return holders[0]
            return None
        return own.owner

    def _holds(self, thread: str, lock: str) -> bool:
        own = self.owns.get(lock)
        if own is None:
            return False
        if own.mode == "X":
            return own.owner == thread and own.depth > 0
        h = own.holders.get(thread)
        return bool(h and h["depth"] > 0)

    def _mode_held(self, thread: str, lock: str) -> Optional[str]:
        own = self.owns.get(lock)
        if own is None:
            return None
        if own.mode == "X":
            return "X" if own.owner == thread and own.depth > 0 else None
        h = own.holders.get(thread)
        return "S" if h and h["depth"] > 0 else None

    def _thread_holds_refs(self, thread: str) -> List[int]:
        refs: List[int] = []
        for lock, own in self.owns.items():
            if own.mode == "X":
                if own.owner == thread and own.depth > 0 and own.grants:
                    refs.append(own.grants[-1])
            else:
                h = own.holders.get(thread)
                if h and h["depth"] > 0 and h["grants"]:
                    refs.append(h["grants"][-1])
        return refs

    def _thread_hold_stacks(self, thread: str) -> List[str]:
        for lock, own in self.owns.items():
            if own.mode == "X" and own.owner == thread and own.depth > 0 and own.grants:
                ev = self.by_id.get(own.grants[-1])
                if ev is not None:
                    return list(ev.stack)
            h = own.holders.get(thread) if own.mode == "S" else None
            if h and h["depth"] > 0 and h["grants"]:
                ev = self.by_id.get(h["grants"][-1])
                if ev is not None:
                    return list(ev.stack)
        return []
