"""Event / lock model for the Lock-Order Anatomy Lab.

All event types share a common envelope; type specific fields live in the
loosely-typed ``data`` mapping.  Timestamps are arbitrary comparable numbers
(usually microseconds since epoch of the traced process).  The ``seq`` field
records ingest order so arrival order is preserved even when events are
streamed out of timestamp order.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

# --- event kinds -----------------------------------------------------------
THREAD_START = "thread_start"
THREAD_END = "thread_end"
REQUEST = "request"          # a thread calls lock / tryLock / readLock / writeLock
GRANT = "grant"              # the request succeeded
RELEASE = "release"
COND_WAIT = "cond_wait"      # entering cond.wait: releases *all* locks held by thread
COND_NOTIFY = "cond_notify"  # signal / signalAll on a condition variable
COND_GRANT = "cond_grant"    # reacquisition after cond wait completed
CANCEL = "cancel"            # timed-out / interrupted pending request

EVENT_KINDS = {
    THREAD_START, THREAD_END, REQUEST, GRANT, RELEASE,
    COND_WAIT, COND_NOTIFY, COND_GRANT, CANCEL,
}

# lock modes
EXCLUSIVE = "X"
SHARED = "S"

# lock types
MUTEX = "mutex"
RECURSIVE = "recursive"
RWLOCK = "rwlock"
LOCK_TYPES = {MUTEX, RECURSIVE, RWLOCK}

# confidence labels
FIRM = "firm"          # fully supported by consistent events
TENTATIVE = "tentative"  # reconstructed with missing / ambiguous evidence
LOW = "low"            # contradictory evidence


@dataclass
class Event:
    kind: str
    ts: float
    thread: str
    lock: Optional[str] = None
    gen: int = 1
    seq: int = 0
    mode: str = EXCLUSIVE          # for REQUEST/GRANT on rwlocks
    cond: Optional[str] = None     # condition variable id
    timeout: bool = False          # request had a timeout
    stack: Optional[list] = None
    data: Dict[str, Any] = field(default_factory=dict)
    id: Optional[int] = None       # database id once persisted
    source_branch: str = "base"

    def sort_key(self):
        # generation first (address reuse across process restarts),
        # then logical time, then arrival order.
        return (self.gen, self.ts, self.seq)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("id", None)
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any], seq: int = 0, id_: Optional[int] = None) -> "Event":
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        kwargs: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}
        for k, v in d.items():
            if k in known:
                kwargs[k] = v
            elif k != "id":
                extra[k] = v
        if "data" not in kwargs or kwargs["data"] is None:
            kwargs["data"] = {}
        if extra:
            kwargs["data"] = {**kwargs["data"], **extra}
        kwargs.setdefault("seq", seq)
        ev = cls(**kwargs)
        ev.id = id_ if id_ is not None else d.get("id")
        return ev
