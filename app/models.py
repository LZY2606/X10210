"""Canonical event model for the lock-order dissecting room.

Events are imported exactly as collected. Nothing is fabricated; uncertain
reconstruction results are reported through confidence entries instead.
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field

EVENT_TYPES = (
    "thread_start",
    "thread_end",
    "acq_req",
    "acq_grant",
    "acq_timeout",
    "release",
    "cond_wait",
    "cond_notify",
)

LOCK_MODES = ("X", "S")


class EventIn(BaseModel):
    seq: int = Field(..., description="collector sequence number within a generation")
    generation: int = Field(0, description="process generation; same address differs per generation")
    timestamp: float
    thread: str
    etype: str
    lock: Optional[str] = None
    mode: str = "X"
    cond: Optional[str] = None
    stack: List[str] = Field(default_factory=list)
    raw: Optional[str] = None


class Event(EventIn):
    id: int

    def key(self) -> tuple:
        return (self.generation, self.timestamp, self.seq, self.id)
