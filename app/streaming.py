"""Streaming append with checkpointed replay.

Events keep arriving. A naïve full replay per append is O(n) per event;
here checkpoints are taken every BUCKET_EVENTS events (in replay order),
so an append replays only from the latest checkpoint before the first
affected event, and callers learn the time window that actually changed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .events import Event, order_key
from .replay import Replayer

BUCKET_EVENTS = 64


@dataclass
class Checkpoint:
    position: int  # number of ordered events applied
    state: Replayer


@dataclass
class IncrementalReplay:
    lock_types: Dict[str, str] = field(default_factory=dict)
    events: List[Event] = field(default_factory=list)
    checkpoints: List[Checkpoint] = field(default_factory=list)
    _sorted: List[Event] = field(default_factory=list)
    _built_to: int = -1
    _head: Optional[Replayer] = None

    def __post_init__(self) -> None:
        self._sorted = sorted(self.events, key=order_key)
        self.checkpoints = []
        self._rebuild_tail()

    def _rebuild_tail(self) -> None:
        cp_pos = self.checkpoints[-1].position if self.checkpoints else 0
        if self.checkpoints:
            state = self.checkpoints[-1].state.clone()
        else:
            state = Replayer(lock_types=dict(self.lock_types))
            self.checkpoints.append(Checkpoint(0, state.clone()))
        for pos in range(cp_pos, len(self._sorted)):
            state.step(self._sorted[pos])
            if (pos + 1) % BUCKET_EVENTS == 0 \
                    and (pos + 1) < len(self._sorted):
                self.checkpoints.append(
                    Checkpoint(pos + 1, state.clone()))
        self._built_to = len(self._sorted)
        self._head = state

    def append(self, incoming: List[Event]) -> Dict[str, Any]:
        """Merge new events into the ordered stream and replay the tail.

        Returns the affected window [from_ts, to_ts] and how many events
        were re-applied.
        """
        if not incoming:
            return {"affected_from": None, "affected_to": None,
                    "replayed": 0, "total": len(self._sorted)}

        incoming_sorted = sorted(incoming, key=order_key)
        first_new_ts = incoming_sorted[0].ts
        merged = sorted(self.events + list(incoming), key=order_key)
        self.events = merged
        self._sorted = merged

        # position of earliest event in the affected suffix
        affected_pos = next(
            (i for i, ev in enumerate(merged) if ev.ts >= first_new_ts),
            len(merged))
        cp_pos = max((c.position for c in self.checkpoints
                      if c.position <= affected_pos), default=0)
        checkpoint = next(c for c in self.checkpoints if c.position == cp_pos)
        replay = checkpoint.state.clone()

        # drop checkpoints strictly after the resume point
        self.checkpoints = [c for c in self.checkpoints
                            if c.position <= cp_pos]

        for pos in range(cp_pos, len(merged)):
            replay.step(merged[pos])
            if (pos + 1) % BUCKET_EVENTS == 0 \
                    and (pos + 1) < len(merged):
                self.checkpoints.append(
                    Checkpoint(pos + 1, replay.clone()))
        self._built_to = len(merged)
        self._head = replay

        affected = merged[cp_pos:]
        return {
            "affected_from": affected[0].ts if affected else None,
            "affected_to": merged[-1].ts if merged else None,
            "replayed": len(affected),
            "total": len(merged),
            "checkpoints": len(self.checkpoints) + 1,
        }

    def snapshot(self, at_ts: Optional[float] = None,
                 complete: bool = False) -> Dict[str, Any]:
        """Snapshot at a time point; full-stream snapshots reuse the tail."""
        if at_ts is None and self._built_to == len(self._sorted):
            replay = self._head.clone()
        else:
            replay = Replayer(lock_types=dict(self.lock_types))
            for ev in self._sorted:
                if at_ts is not None and ev.ts > at_ts:
                    break
                replay.step(ev)
        if complete:
            replay.finish()
        snap = replay.snapshot()
        snap["at_ts"] = at_ts
        return snap

    def current(self) -> Replayer:
        assert self._head is not None
        return self._head
