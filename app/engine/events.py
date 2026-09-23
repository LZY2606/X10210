"""Event model for lock-trace ingestion.

An event describes one observed runtime signal.  Events may arrive out of
order at ingest time (``order_events`` restores the acquisition order per
generation while flagging which ones were reordered).
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Iterable

EVENT_TYPES = {
    "acquire_request",
    "acquired",
    "release",
    "thread_start",
    "thread_end",
    "cond_wait",
    "cond_notify",
    "acquire_timeout",
}

# Default modes per lock type for events that omit ``mode``.
DEFAULT_MODE = {"mutex": "X", "reentrant": "X", "rwlock": "X"}


@dataclass
class Event:
    id: int
    type: str
    timestamp: float
    thread: str
    lock: str | None = None
    mode: str | None = None            # "S" / "X"; rwlock upgrade is an X request while holding S
    cond: str | None = None            # condition variable identity
    gen: str = "0"                     # acquisition generation (process restart)
    success: bool = True               # cond_notify may signal notify-all / spurious
    stack: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
    # Filled in by ``order_events``:
    seq: int = -1                      # replay position
    arrival: int = -1                  # original arrival order
    reordered: bool = False

    def key(self) -> tuple[str, str, str | None, str | None]:
        return (self.gen, self.thread, self.lock, self.cond)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def make_event(data: dict[str, Any], arrival: int = -1, eid: int | None = None) -> Event:
    """Build an :class:`Event` from an ingest payload, with light validation."""
    etype = data.get("type")
    if etype not in EVENT_TYPES:
        raise ValueError(f"unknown event type: {etype!r}")
    try:
        ts = float(data["timestamp"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("event needs a numeric timestamp") from exc
    thread = str(data.get("thread") or "")
    if not thread:
        raise ValueError("event needs a thread id")
    stack = data.get("stack") or []
    if isinstance(stack, str):
        stack = [stack]
    ev = Event(
        id=int(eid if eid is not None else data.get("id", arrival if arrival >= 0 else 0)),
        type=etype,
        timestamp=ts,
        thread=thread,
        lock=(str(data["lock"]) if data.get("lock") is not None else None),
        mode=(data.get("mode") or None),
        cond=(str(data["cond"]) if data.get("cond") is not None else None),
        gen=str(data.get("gen", "0")),
        mode_lock=(str(data["mode_lock"]) if data.get("mode_lock") is not None else (str(data["lock"]) if data.get("lock") is not None else None)) if False else None,
        success=bool(data.get("success", True)),
        stack=list(stack),
        raw={k: v for k, v in data.items() if k not in {"type", "timestamp", "thread", "lock", "mode", "cond", "gen", "success", "stack"}},
        arrival=arrival,
    )
    return ev


def order_events(events: Iterable[Event]) -> list[Event]:
    """Sort events into replay order.

    Primary key is (generation, timestamp).  Stable ordering preserves the
    original arrival order for equal timestamps, and every event whose replay
    position differs from its arrival position is flagged ``reordered``.
    """
    evs = list(events)
    for i, ev in enumerate(evs):
        if ev.arrival < 0:
            ev.arrival = i
    ordered = sorted(enumerate(evs), key=lambda pair: (pair[1].gen, pair[1].timestamp, pair[0]))
    out: list[Event] = []
    for seq, (orig_idx, ev) in enumerate(ordered):
        ev.seq = seq
        ev.reordered = orig_idx != seq
        out.append(ev)
    return out
