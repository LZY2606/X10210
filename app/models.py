from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    seq: int | None = None
    gen: str = "g0"
    ts: int = 0
    kind: Literal[
        "thread_start", "thread_end", "acquire", "acquire_ok",
        "acquire_cancel", "release", "cond_wait", "cond_notify",
    ]
    thread: str | None = None
    lock: str | None = None
    cond: str | None = None
    mode: Literal["E", "S", "X"] | None = "E"
    timeout_ms: int | None = None
    all: bool = False
    stack: list[str] | str | None = None
    note: str | None = None


class EventsIn(BaseModel):
    events: list[EventIn]


class EventPatch(BaseModel):
    ts: int | None = None
    kind: str | None = None
    thread: str | None = None
    lock: str | None = None
    cond: str | None = None
    mode: str | None = None
    timeout_ms: int | None = None
    all: bool | None = None
    stack: list[str] | str | None = None
    seq: int | None = None
    gen: str | None = None
    note: str | None = None


class CorrectionIn(BaseModel):
    note: str = ""
    event_patch: EventPatch | None = None
    event_id: int | None = None
    new_event: EventIn | None = None
    declare_lock: str | None = None
    lock_type: Literal["mutex", "recursive", "rwlock"] | None = None
    delete_event_id: int | None = None
    name: str | None = None


class DeclareTypeIn(BaseModel):
    lock: str
    type: Literal["mutex", "recursive", "rwlock"]


class ReduceIn(BaseModel):
    fingerprint: str | None = None
    ts: int | None = None
    event_id: int | None = None
    create_branch: bool = False
    name: str | None = None


class ForkIn(BaseModel):
    name: str
    note: str = ""
