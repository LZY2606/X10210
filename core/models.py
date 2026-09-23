"""Event model and normalization for the lock-order dissection room."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

KIND_ALIASES = {
    "acquire": "acquire_req",
    "acquire_request": "acquire_req",
    "lock": "acquire_req",
    "acquired": "acquire_ok",
    "acquire_success": "acquire_ok",
    "release": "release",
    "unlock": "release",
    "thread_start": "thread_start",
    "start": "thread_start",
    "thread_end": "thread_end",
    "end": "thread_end",
    "wait": "cond_wait",
    "cond_wait_begin": "cond_wait",
    "notify": "cond_notify",
    "signal": "cond_notify",
    "notify_all": "cond_notify_all",
    "broadcast": "cond_notify_all",
    "wake": "cond_wake",
    "cond_wait_end": "cond_wake",
    "timeout": "cancel",
    "cancel": "cancel",
    "cancelled": "cancel",
}

CANONICAL_KINDS = {
    "acquire_req",
    "acquire_ok",
    "release",
    "thread_start",
    "thread_end",
    "cond_wait",
    "cond_wake",
    "cond_notify",
    "cond_notify_all",
    "cancel",
}


@dataclass
class Event:
    seq: int
    ts: float
    kind: str
    thread: str
    gen: int = 0
    lock: str | None = None
    cond: str | None = None
    mode: str = "X"
    req_id: str | None = None
    stack: list[str] = field(default_factory=list)
    arrival: float = 0.0
    note: str | None = None

    def key(self) -> tuple[int, float, int]:
        return (self.gen, self.ts, self.seq)

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "arrival": self.arrival,
            "kind": self.kind,
            "thread": self.thread,
            "gen": self.gen,
            "lock": self.lock,
            "cond": self.cond,
            "mode": self.mode,
            "req_id": self.req_id,
            "stack": list(self.stack),
            "note": self.note,
        }


def _norm_mode(raw: Any) -> str:
    value = str(raw or "X").strip().upper()
    if value in {"R", "READ", "SHARED", "S"}:
        return "R"
    if value in {"W", "WRITE", "EXCLUSIVE", "X"}:
        return "X"
    return "X"


def event_from_dict(data: dict[str, Any], seq: int | None = None) -> Event:
    raw_kind = str(data.get("kind", "")).strip().lower()
    kind = KIND_ALIASES.get(raw_kind, raw_kind)
    if kind not in CANONICAL_KINDS:
        raise ValueError(f"unknown event kind: {data.get('kind')!r}")
    thread = data.get("thread") or data.get("tid")
    if thread is None or str(thread) == "":
        raise ValueError("event requires a thread")
    if "ts" not in data:
        raise ValueError("event requires ts")
    lock = data.get("lock") or data.get("lock_addr") or data.get("addr")
    cond = data.get("cond") or data.get("cond_addr")
    resolved_seq = int(data["seq"]) if data.get("seq") is not None else (seq if seq is not None else 0)
    arrival = data.get("arrival")
    if arrival is None:
        arrival = data.get("arrival_seq", resolved_seq)
    stack = data.get("stack") or []
    if isinstance(stack, str):
        stack = [stack]
    return Event(
        seq=resolved_seq,
        ts=float(data["ts"]),
        kind=kind,
        thread=str(thread),
        gen=int(data.get("gen", data.get("generation", 0))),
        lock=str(lock) if lock is not None else None,
        cond=str(cond) if cond is not None else None,
        mode=_norm_mode(data.get("mode")),
        req_id=str(data["req_id"]) if data.get("req_id") is not None else None,
        stack=[str(frame) for frame in stack],
        arrival=float(arrival),
        note=data.get("note"),
    )
