from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


REQUEST_EVENTS = {"acquire_request"}
SUCCESS_EVENTS = {"acquire_success"}
CANCEL_EVENTS = {"acquire_timeout", "acquire_cancelled", "request_cancelled"}
CONDITION_CANCEL_EVENTS = {"condition_wait_timeout", "condition_wait_cancelled"}


@dataclass
class Event:
    id: int | None
    branch_id: int
    arrival_seq: int
    timestamp: str
    generation: str
    thread_id: str
    type: str
    lock: str | None = None
    target: str | None = None
    mode: str | None = None
    request_id: str | None = None
    stack: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)
    confidence: str = "observed"
    inferred: bool = False

    @property
    def lock_key(self) -> str | None:
        if not self.lock:
            return None
        return f"{self.generation}:{self.lock}"

    @property
    def condition_key(self) -> str | None:
        if not self.target:
            return None
        return f"{self.generation}:{self.target}"


@dataclass
class LockDeclaration:
    branch_id: int
    generation: str
    lock: str
    kind: str

    @property
    def lock_key(self) -> str:
        return f"{self.generation}:{self.lock}"


def event_from_row(row: Any) -> Event:
    import json

    raw = json.loads(row["data"])
    return Event(
        id=row["id"],
        branch_id=row["branch_id"],
        arrival_seq=row["arrival_seq"],
        timestamp=str(raw.get("timestamp", "")),
        generation=str(raw.get("generation", "default")),
        thread_id=str(raw.get("thread_id", raw.get("thread", ""))),
        type=str(raw.get("type", "")),
        lock=raw.get("lock"),
        target=raw.get("target") or raw.get("condition"),
        mode=raw.get("mode"),
        request_id=raw.get("request_id"),
        stack=list(raw.get("stack", [])),
        raw=raw,
        confidence=raw.get("confidence", "observed"),
        inferred=bool(raw.get("inferred", False)),
    )

