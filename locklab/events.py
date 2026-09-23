from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Dict, Iterable, List, Optional


REQUEST_EVENTS = {"request", "lock_request", "acquire_request"}
ACQUIRED_EVENTS = {"acquired", "lock_acquired", "acquire_success", "granted"}
RELEASE_EVENTS = {"release", "lock_release", "released"}
THREAD_START_EVENTS = {"thread_start", "thread_begin"}
THREAD_END_EVENTS = {"thread_end", "thread_exit", "thread_terminate"}
COND_WAIT_EVENTS = {"cond_wait", "condition_wait"}
COND_NOTIFY_EVENTS = {"cond_notify", "condition_notify", "cond_signal", "condition_signal"}
CANCEL_EVENTS = {"timeout", "lock_timeout", "cancel", "cancelled"}
TYPE_EVENTS = {"lock_type", "declare_lock_type"}
RESTART_EVENTS = {"process_restart", "generation_start"}


def canonical_event_type(value: str) -> str:
    kind = str(value or "").strip().lower()
    groups = (
        (REQUEST_EVENTS, "request"),
        (ACQUIRED_EVENTS, "acquired"),
        (RELEASE_EVENTS, "release"),
        (THREAD_START_EVENTS, "thread_start"),
        (THREAD_END_EVENTS, "thread_end"),
        (COND_WAIT_EVENTS, "cond_wait"),
        (COND_NOTIFY_EVENTS, "cond_notify"),
        (CANCEL_EVENTS, "cancel"),
        (TYPE_EVENTS, "lock_type"),
        (RESTART_EVENTS, "process_restart"),
    )
    for names, canonical in groups:
        if kind in names:
            return canonical
    raise ValueError(f"unsupported event type: {value}")


def normalize_mode(value: Any) -> str:
    mode = str(value or "write").strip().lower()
    if mode in {"s", "sh", "shared", "read", "r"}:
        return "read"
    if mode in {"x", "exclusive", "write", "w"}:
        return "write"
    raise ValueError(f"unsupported lock mode: {value}")


def normalize_stack(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [line for line in str(value).splitlines() if line.strip()]


@dataclass
class Event:
    event_id: str
    arrival_seq: int
    timestamp: float
    seq: int
    generation: int
    type: str
    thread_id: str = ""
    lock_id: str = ""
    condition_id: str = ""
    mode: str = "write"
    request_id: str = ""
    stack: List[str] = field(default_factory=list)
    lock_type: str = ""
    notify_all: bool = False
    confidence: float = 1.0
    raw: Dict[str, Any] = field(default_factory=dict)
    source_event_id: str = ""
    corrected_from: str = ""

    @property
    def key(self) -> str:
        return f"g{self.generation}:{self.event_id}"

    def identity_tuple(self) -> tuple:
        return (self.generation, self.lock_id, self.thread_id, self.mode, self.request_id)

    def public(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "source_event_id": self.source_event_id or self.event_id,
            "arrival_seq": self.arrival_seq,
            "timestamp": self.timestamp,
            "seq": self.seq,
            "generation": self.generation,
            "type": self.type,
            "thread_id": self.thread_id,
            "lock_id": self.lock_id,
            "condition_id": self.condition_id,
            "mode": self.mode,
            "request_id": self.request_id,
            "stack": self.stack,
            "lock_type": self.lock_type,
            "notify_all": self.notify_all,
            "confidence": self.confidence,
            "corrected_from": self.corrected_from,
        }


def normalize_event(item: Dict[str, Any], arrival_seq: int) -> Event:
    if not isinstance(item, dict):
        raise ValueError("event must be an object")
    event_type = canonical_event_type(str(item.get("type", "")))
    timestamp = item.get("timestamp", arrival_seq)
    if not isinstance(timestamp, (int, float)) or isinstance(timestamp, bool):
        raise ValueError("timestamp must be a number")
    seq = item.get("seq", arrival_seq)
    if not isinstance(seq, int) or isinstance(seq, bool):
        raise ValueError("seq must be an integer")
    generation = int(item.get("generation", 1))
    if generation < 1:
        raise ValueError("generation must be >= 1")
    event_id = str(item.get("event_id") or f"e{arrival_seq}")
    thread_id = str(item.get("thread_id") or item.get("thread") or "")
    lock_id = str(item.get("lock_id") or item.get("lock") or item.get("address") or "")
    condition_id = str(item.get("condition_id") or item.get("condition") or "")
    if event_type in {"request", "acquired", "release"} and not lock_id:
        raise ValueError(f"{event_type} requires lock_id")
    if event_type.startswith("cond_") and not condition_id:
        raise ValueError(f"{event_type} requires condition_id")
    if event_type in {"request", "acquired", "release", "thread_start", "thread_end", "cond_wait"} and not thread_id:
        raise ValueError(f"{event_type} requires thread_id")
    lock_type = str(item.get("lock_type") or "").strip().lower()
    if lock_type and lock_type not in {"mutex", "reentrant", "rwlock", "rw", "readwrite"}:
        raise ValueError(f"unsupported lock_type: {lock_type}")
    if lock_type in {"rw", "readwrite"}:
        lock_type = "rwlock"
    confidence = float(item.get("confidence", 1.0))
    return Event(
        event_id=event_id,
        arrival_seq=int(arrival_seq),
        timestamp=float(timestamp),
        seq=seq,
        generation=generation,
        type=event_type,
        thread_id=thread_id,
        lock_id=lock_id,
        condition_id=condition_id,
        mode=normalize_mode(item.get("mode", "write")),
        request_id=str(item.get("request_id") or ""),
        stack=normalize_stack(item.get("stack")),
        lock_type=lock_type,
        notify_all=bool(item.get("notify_all", False)),
        confidence=min(1.0, max(0.0, confidence)),
        raw=dict(item),
        source_event_id=str(item.get("source_event_id") or event_id),
        corrected_from=str(item.get("corrected_from") or ""),
    )


def normalize_events(items: Iterable[Dict[str, Any]]) -> List[Event]:
    events: List[Event] = []
    seen = set()
    for offset, item in enumerate(items, start=1):
        event = normalize_event(item, offset)
        if event.event_id in seen:
            raise ValueError(f"duplicate event_id: {event.event_id}")
        seen.add(event.event_id)
        events.append(event)
    return mark_late_arrivals(events)


def mark_late_arrivals(events: List[Event]) -> List[Event]:
    latest = None
    result: List[Event] = []
    for event in events:
        order = (event.timestamp, event.seq)
        if latest is not None and order < latest and event.confidence > 0.75:
            result.append(replace(event, confidence=0.75))
        else:
            result.append(event)
        if latest is None or order > latest:
            latest = order
    return result


def timeline_sort(events: Iterable[Event]) -> List[Event]:
    return sorted(events, key=lambda event: (event.timestamp, event.seq, event.arrival_seq))
