"""Raw thread-event model for the lock-order dissection room.

Events are imported from arbitrary sources and kept in collection order
(``seq``). Each lock identity carries its process generation, so the same
address reused after a restart is a different lock.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

# Event kinds -----------------------------------------------------------------

ACQ_REQ = "acquire_request"       # thread asks for a lock
ACQ_OK = "acquire_ok"             # thread got the lock
RELEASE = "release"               # thread released the lock
ACQ_TIMEOUT = "acquire_timeout"   # a pending acquire gave up / timed out
THREAD_START = "thread_start"
THREAD_END = "thread_end"
COND_WAIT = "condition_wait"      # release lock, park on condition variable
COND_NOTIFY = "condition_notify"  # wake one waiter (it re-contends the lock)
COND_NOTIFY_ALL = "condition_notify_all"
COND_REACQ = "condition_reacq"    # reacquire completed after condition wait
PROCESS_START = "process_start"   # bumps the process generation
LOCK_TYPE = "lock_type"           # user/source declaration of a lock type

EVENT_KINDS = {
    ACQ_REQ, ACQ_OK, RELEASE, ACQ_TIMEOUT, THREAD_START, THREAD_END,
    COND_WAIT, COND_NOTIFY, COND_NOTIFY_ALL, COND_REACQ, PROCESS_START,
    LOCK_TYPE,
}

# Tolerant aliases accepted on import.
_KIND_ALIASES = {
    "acq_req": ACQ_REQ, "acquire": ACQ_REQ, "request": ACQ_REQ, "lock": ACQ_REQ,
    "acquire_req": ACQ_REQ, "acquire_requested": ACQ_REQ,
    "acq_ok": ACQ_OK, "acquired": ACQ_OK, "acquire_success": ACQ_OK,
    "acquire_granted": ACQ_OK, "grant": ACQ_OK,
    "rel": RELEASE, "unlock": RELEASE,
    "timeout": ACQ_TIMEOUT, "acquire_timed_out": ACQ_TIMEOUT,
    "cancel": ACQ_TIMEOUT, "cancelled": ACQ_TIMEOUT,
    "start": THREAD_START, "thread_begin": THREAD_START,
    "end": THREAD_END, "thread_exit": THREAD_END, "exit": THREAD_END,
    "wait": COND_WAIT, "cond_wait": COND_WAIT,
    "notify": COND_NOTIFY, "signal": COND_NOTIFY, "cond_notify": COND_NOTIFY,
    "notify_all": COND_NOTIFY_ALL, "broadcast": COND_NOTIFY_ALL,
    "cond_notify_all": COND_NOTIFY_ALL,
    "reacquire": COND_REACQ, "cond_reacquire": COND_REACQ,
    "reacquired": COND_REACQ, "condition_reacquired": COND_REACQ,
    "process": PROCESS_START, "restart": PROCESS_START,
    "type": LOCK_TYPE, "declare_lock": LOCK_TYPE,
}

MUTEX = "mutex"
RWLOCK = "rwlock"
RECURSIVE = "recursive"
NONRECURSIVE = "nonrecursive"

_MODE_ALIASES = {
    "x": "X", "w": "X", "write": "X", "exclusive": "X",
    "s": "S", "r": "S", "read": "S", "shared": "S",
}
_TYPE_ALIASES = {
    "mutex": MUTEX, "lock": MUTEX, "reentrant": MUTEX,
    "recursive": MUTEX, "recursive_mutex": MUTEX,
    "plain": MUTEX, "default": MUTEX,
    "rwlock": RWLOCK, "rw": RWLOCK, "readwritelock": RWLOCK,
    "read_write": RWLOCK, "read_write_lock": RWLOCK,
}


def norm_kind(value: Any) -> str:
    """Normalize an imported event-kind token.

    Raises ``ValueError`` for unknown kinds; callers record the anomaly
    instead of inventing an event.
    """
    if value in EVENT_KINDS:
        return value
    key = str(value).strip().lower().replace("-", "_")
    if key in EVENT_KINDS:
        return key
    if key in _KIND_ALIASES:
        return _KIND_ALIASES[key]
    raise ValueError(f"unknown event kind: {value!r}")


def norm_mode(value: Any) -> str:
    if value is None:
        return "X"
    key = str(value).strip().lower()
    if key in _MODE_ALIASES:
        return _MODE_ALIASES[key]
    return "X"


def norm_type(value: Any) -> str:
    key = str(value).strip().lower().replace("-", "_")
    if key in _TYPE_ALIASES:
        return _TYPE_ALIASES[key]
    raise ValueError(f"unknown lock type: {value!r}")


def is_recursive(decl: Optional[str]) -> bool:
    """Interpret a lock-type declaration.

    ``"recursive"`` / ``"nonrecursive"`` refine a plain mutex; rwlocks are
    marked recursive per slot (a thread may take read/write slots repeatedly).
    """
    if decl is None:
        return True  # default: conservative reentrant mutex
    decl = str(decl).strip().lower().replace("-", "_")
    return decl not in {NONRECURSIVE, "plain_mutex", "binary", "non_reentrant"}


@dataclass
class Event:
    """One raw or corrected collection event.

    ``seq`` preserves collection order; ``gen`` is the process generation the
    event belongs to (same address in a later gen is a different lock).
    """

    seq: int
    kind: str
    ts: float = 0.0
    thread: Optional[str] = None
    lock: Optional[str] = None
    gen: int = 0
    mode: str = "X"
    cv: Optional[str] = None          # condition variable id
    lock_type: Optional[str] = None   # for LOCK_TYPE: mutex|rwlock
    recursive: Optional[bool] = None  # for LOCK_TYPE
    batch: int = 0                    # collection batch (import order)
    origin_seq: Optional[int] = None  # corrected event -> original seq
    raw: Dict[str, Any] = field(default_factory=dict)

    def lock_key(self) -> Optional[str]:
        if self.lock is None:
            return None
        return f"g{self.gen}:{self.lock}"

    def cv_key(self) -> Optional[str]:
        if self.cv is None and self.lock is not None:
            # condition waits usually identify the associated lock only
            return f"g{self.gen}:cv:{self.cv if self.cv is not None else self.lock}"
        if self.cv is None:
            return None
        return f"g{self.gen}:cv:{self.cv}"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def parse_event(data: Dict[str, Any], seq: int, default_gen: int = 0,
                batch: int = 0) -> Event:
    """Build an :class:`Event` from a raw JSON-ish record.

    Sequence and generation may be supplied explicitly; otherwise collection
    order is preserved by the caller-assigned ``seq``.
    """
    kind = norm_kind(data.get("kind") or data.get("type") or data.get("event"))
    raw = dict(data)

    def opt(name):
        value = data.get(name)
        return None if value in (None, "") else str(value)

    ev_seq = data.get("seq")
    ev_ts = data.get("ts", data.get("timestamp", 0.0))
    try:
        ts = float(ev_ts) if ev_ts not in (None, "") else 0.0
    except (TypeError, ValueError):
        ts = 0.0

    gen_value = data.get("gen", data.get("generation"))
    gen = int(gen_value) if gen_value not in (None, "") else default_gen

    event = Event(
        seq=int(ev_seq) if ev_seq not in (None, "") else seq,
        kind=kind,
        ts=ts,
        thread=opt("thread") or opt("tid") or opt("thread_id"),
        lock=opt("lock") or opt("lock_addr") or opt("addr") or opt("lock_id"),
        gen=gen,
        mode=norm_mode(data.get("mode") or data.get("access")),
        cv=opt("cv") or opt("cond") or opt("condition"),
        batch=int(data.get("batch", batch) or batch),
        origin_seq=int(data["origin_seq"]) if data.get("origin_seq") not in (None, "") else None,
        raw=raw,
    )
    if kind == LOCK_TYPE:
        declared = data.get("lock_type", data.get("ltype", data.get("type_decl")))
        event.lock_type = norm_type(declared) if declared not in (None, "") else MUTEX
        rec = data.get("recursive", data.get("reentrant"))
        if rec is None:
            # "recursive"/"nonrecursive" may arrive as the type itself
            token = str(declared).strip().lower() if declared is not None else ""
            if token in ("recursive", "nonrecursive", "reentrant", "non_reentrant"):
                event.recursive = token in ("recursive", "reentrant")
            else:
                event.recursive = True
        else:
            event.recursive = bool(rec) and str(rec).lower() not in ("0", "false", "no")
    return event
