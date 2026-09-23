"""Mutable replay state for a single reconstruction (one branch / generation set)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StackEntry:
    event_id: str
    request_event_id: Optional[str]
    mode: str  # effective holding mode: read / write
    ts: Optional[float]
    stack: List[str] = field(default_factory=list)
    request_stack: List[str] = field(default_factory=list)
    confidence: str = "high"
    reasons: List[str] = field(default_factory=list)
    after_cond: bool = False


@dataclass
class WaitRec:
    event_id: str
    lock: str
    mode: str  # requested: read / write / "" (mutex default write)
    ts: Optional[float]
    stack: List[str] = field(default_factory=list)
    timeout: Optional[float] = None
    cancelled: bool = False
    cancel_event_id: Optional[str] = None
    cancel_reason: str = ""
    was_blocked: bool = False
    in_cycle: bool = False
    confidence: str = "high"
    reasons: List[str] = field(default_factory=list)


@dataclass
class Interval:
    start_event: str
    start_ts: Optional[float]
    start_seq: int
    end_event: Optional[str] = None
    end_ts: Optional[float] = None
    end_seq: Optional[int] = None


@dataclass
class ThreadState:
    tid: str
    gen: int
    alive: bool = False
    started: bool = False
    # lock_id -> occurrences held, in acquisition order
    holds: Dict[str, List[StackEntry]] = field(default_factory=dict)
    waiting: Optional[WaitRec] = None
    # cond_id -> stack of active cond wait records (event ids, released lock)
    cond_depth: Dict[str, List[str]] = field(default_factory=dict)
    ended_event: Optional[str] = None

    def held_locks(self) -> List[str]:
        return [lock for lock, entries in self.holds.items() if entries]


@dataclass
class LockState:
    lock_id: str
    gen: int
    addr: str
    ltype: str  # mutex / reentrant / rw / unknown
    # thread id -> occurrences
    holders: Dict[str, List[StackEntry]] = field(default_factory=dict)
    confidence: str = "high"
    reasons: List[str] = field(default_factory=list)

    def holder_threads(self) -> List[str]:
        return [t for t, e in self.holders.items() if e]

    def mode_of(self, tid: str) -> str:
        entries = self.holders.get(tid) or []
        if any(e.mode == "write" for e in entries):
            return "write"
        return "read" if entries else ""


@dataclass
class State:
    current_gen: int = 0
    threads: Dict[str, ThreadState] = field(default_factory=dict)
    locks: Dict[str, LockState] = field(default_factory=dict)
    # lock_id -> list of closed hold intervals
    hold_intervals: Dict[str, List[Interval]] = field(default_factory=dict)
    open_intervals: Dict[str, Interval] = field(default_factory=dict)
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    event_count: int = 0

    def lock_key(self, gen: int, addr: str) -> str:
        return "g%d:%s" % (gen, addr)

    def thread_key(self, gen: int, tid: str) -> str:
        return "g%d:%s" % (gen, tid)

    def get_thread(self, gen: int, tid: str) -> ThreadState:
        key = self.thread_key(gen, tid)
        t = self.threads.get(key)
        if t is None:
            t = ThreadState(tid=tid, gen=gen)
            self.threads[key] = t
        return t

    def get_lock(self, gen: int, addr: str, ltypes: Dict[str, str]) -> LockState:
        key = self.lock_key(gen, addr)
        lock = self.locks.get(key)
        if lock is None:
            ltype = ltypes.get(addr) or ltypes.get(key) or "unknown"
            lock = LockState(lock_id=key, gen=gen, addr=addr, ltype=ltype)
            if ltype == "unknown":
                lock.confidence = "medium"
                lock.reasons.append("锁类型未知，按互斥语义推断")
            self.locks[key] = lock
        return lock

    def add_anomaly(self, event_id: str, gen: int, thread: Optional[str],
                    lock: Optional[str], level: str, code: str, message: str) -> None:
        self.anomalies.append({
            "event_id": event_id, "gen": gen, "thread": thread, "lock": lock,
            "level": level, "code": code, "message": message,
        })
