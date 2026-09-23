"""带检查点的流式重建：追加事件后只重放受影响时间段。"""
from __future__ import annotations

import threading
from typing import Optional

from .engine import Engine

CHECKPOINT_EVERY = 8


def ordered_events(events: list[dict]) -> list[dict]:
    return sorted(events, key=lambda e: (e["timestamp"], e["arrival"]))


def rebuild(events: list[dict],
            generation: Optional[str] = None,
            timepoint: Optional[int] = None) -> Engine:
    evs = ordered_events(events)
    if generation is not None:
        evs = [e for e in evs if e["generation"] == generation]
    if timepoint is not None:
        evs = [e for e in evs if e["timestamp"] <= timepoint]
    return Engine(evs).run()


_HANDLERS = None


class IncrementalReconstructor:
    """缓存周期检查点；追加事件时从最后一个“时间上不受影响”的检查点恢复。"""

    def __init__(self, checkpoint_every: int = CHECKPOINT_EVERY):
        self._lock = threading.RLock()
        self._raw: list[dict] = []
        # (前缀长度, 该前缀最大时间戳, 快照)
        self._checkpoints: list[tuple[int, int, dict]] = []
        self.recompute_count = 0
        self.replayed_events = 0
        self.checkpoint_every = checkpoint_every
        self._engine: Optional[Engine] = None

    def append(self, new_events: list[dict]) -> None:
        with self._lock:
            if not new_events:
                return
            min_new_ts = min(e["timestamp"] for e in new_events)
            self._raw = ordered_events(self._raw + list(new_events))
            self._rebuild(min_new_ts)

    def _rebuild(self, min_new_ts: int) -> None:
        self.recompute_count += 1
        start_index = 0
        snap = None
        for index, max_ts, candidate in reversed(self._checkpoints):
            if max_ts < min_new_ts and index <= len(self._raw):
                start_index, snap = index, candidate
                break
        self.replayed_events = len(self._raw) - start_index
        if snap is not None:
            eng = Engine._from_snapshot(snap, self._raw[:start_index])
        else:
            eng = self._raw[:start_index] and Engine(
                self._raw[:start_index]).run() or Engine([])
            self._checkpoints = []

        handlers = {
            "thread_start": eng._on_thread_start,
            "thread_end": eng._on_thread_end,
            "acquire_request": eng._on_acquire_request,
            "acquire_ok": eng._on_acquire_ok,
            "acquire_fail": eng._on_acquire_fail,
            "release": eng._on_release,
            "cond_wait": eng._on_cond_wait,
            "cond_wake": eng._on_cond_wake,
            "cond_notify": eng._on_cond_notify,
        }
        kept = []
        for offset, ev in enumerate(self._raw[start_index:]):
            eng._sweep_timeouts(ev["timestamp"])
            handlers[ev["type"]](ev)
            idx = start_index + offset + 1
            if idx % self.checkpoint_every == 0 and idx < len(self._raw):
                kept.append((idx, ev["timestamp"], eng._snapshot()))
        # 保留早期检查点 + 本轮新检查点
        self._checkpoints = [c for c in self._checkpoints
                             if c[0] <= start_index] + kept
        self._engine = eng

    def engine(self) -> Engine:
        with self._lock:
            if self._engine is None:
                self._engine = Engine([])
            return self._engine

    def stats(self) -> dict:
        return {
            "events": len(self._raw),
            "recompute_count": self.recompute_count,
            "replayed_events_last_append": self.replayed_events,
            "checkpoints": len(self._checkpoints),
        }
