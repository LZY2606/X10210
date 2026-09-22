"""Thread / lock trace event model.

Events arrive as plain dicts (from ingestion, SQLite, or branch corrections).
`Event` is the normalized, immutable representation used by the replay engine.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


EVENT_TYPES = {
    "thread_start",
    "thread_end",
    "acquire_req",
    "acquired",
    "release",
    "cond_wait",
    "cond_notify",
    "cond_notify_all",
    "acquire_timeout",
}

LOCK_TYPES = {"mutex", "recursive", "rwlock"}

MODES = ("S", "X")


class EventError(ValueError):
    pass


@dataclass(frozen=True)
class Event:
    """A single trace event.

    seq preserves the *collection generation* order (arrival order);
    ts is the event timestamp used for time-point selection.
    """

    seq: int
    ts: float
    type: str
    thread: Optional[str] = None
    lock: Optional[str] = None
    mode: Optional[str] = None
    cond: Optional[str] = None
    stack: Optional[List[str]] = field(default_factory=list)
    gen: int = 0
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["stack"] = list(self.stack or [])
        return d


def _as_stack(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(s) for s in value]
    raise EventError("stack must be a list of strings")


def normalize_event(raw: Dict[str, Any], seq: int) -> Event:
    """Normalize one raw dict into an Event.

    Accepts flexible key aliases. Raises EventError on invalid payloads.
    """
    if not isinstance(raw, dict):
        raise EventError("event must be an object")

    etype = raw.get("type")
    if etype not in EVENT_TYPES:
        raise EventError("unknown event type: %r" % (etype,))

    if "ts" not in raw:
        raise EventError("event %r missing ts" % etype)
    try:
        ts = float(raw["ts"])
    except (TypeError, ValueError):
        raise EventError("ts must be numeric")

    thread = raw.get("thread") or raw.get("thread_id")
    lock = raw.get("lock") or raw.get("lock_id") or raw.get("addr")
    cond = raw.get("cond") or raw.get("cond_id")
    mode = raw.get("mode")
    if mode is not None:
        mode = str(mode).upper()
        if mode not in MODES:
            raise EventError("mode must be S or X, got %r" % mode)

    gen = int(raw.get("gen", 0))
    eid = raw.get("id")
    if eid is not None:
        eid = int(eid)
    stack = _as_stack(raw.get("stack"))

    if etype in ("thread_start", "thread_end") and not thread:
        raise EventError("%s requires thread" % etype)
    if etype not in ("thread_start", "thread_end") and not (thread and lock):
        raise EventError("%s requires thread and lock" % etype)

    return Event(
        seq=int(seq),
        ts=ts,
        type=etype,
        thread=str(thread) if thread is not None else None,
        lock=str(lock) if lock is not None else None,
        mode=mode,
        cond=str(cond) if cond is not None else None,
        stack=stack,
        gen=gen,
        id=eid,
    )


def normalize_events(raw_list: List[Dict[str, Any]]) -> List[Event]:
    events = []
    for index, raw in enumerate(raw_list):
        events.append(normalize_event(raw, int(raw.get("seq", index))))
    return events


def order_key(event: Event):
    """Logical timestamp order used for replay: generation, timestamp, seq."""
    return (event.gen, event.ts, event.seq)
