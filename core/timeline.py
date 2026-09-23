"""Replay timeline with sparse checkpoints for cheap time-slider queries."""
from __future__ import annotations

import copy
from typing import Any

from .engine import Engine
from .models import Event

CHECKPOINT_EVENTS = 64


class Timeline:
    """Owns canonical events and answers prefix-state questions.

    Sparse checkpoints keep replay cost bounded when events are appended in
    stream order.  An append that lands "in the past" (late arrival) invalidates
    only checkpoints at or after the insertion point, then rebuilds later ones
    lazily.
    """

    def __init__(
        self,
        events: list[Event] | None = None,
        lock_types: dict[str, str] | None = None,
    ):
        self.engine = Engine(lock_types=lock_types)
        self.events = sorted(events or [], key=lambda event: event.key())
        self._checkpoints: dict[int, dict[str, Any]] = {}
        self.replay_count = 0
        self.last_replay_events = 0
        self._build_checkpoints()

    def set_lock_types(self, lock_types: dict[str, str]) -> None:
        self.engine = Engine(lock_types=lock_types)
        self._checkpoints.clear()
        self._build_checkpoints()

    def _build_checkpoints(self) -> None:
        self.engine.state = self.engine._fresh_state()
        self._checkpoints[0] = copy.deepcopy(self.engine.state)
        for index, event in enumerate(self.events):
            self.engine.apply(event)
            if (index + 1) % CHECKPOINT_EVENTS == 0:
                self._checkpoints[index + 1] = copy.deepcopy(self.engine.state)
        self._checkpoints[len(self.events)] = copy.deepcopy(self.engine.state)

    def replace_all(self, events: list[Event]) -> None:
        self.events = sorted(events, key=lambda event: event.key())
        self._build_checkpoints()

    def append(self, event: Event) -> int:
        """Insert one arrival; return how many events were replayed."""
        key = event.key()
        position = len(self.events)
        for index, existing in enumerate(self.events):
            if existing.key() > key:
                position = index
                break
        self.events.insert(position, event)
        # Drop checkpoints at/after the insertion point.
        self._checkpoints = {
            index: state
            for index, state in self._checkpoints.items()
            if index < position or index == 0
        }
        # Find the nearest surviving checkpoint.
        base = max(self._checkpoints)
        return self._ensure_through(len(self.events), base)

    def _ensure_through(self, target: int, base: int | None = None) -> int:
        if base is None:
            base = max(index for index in self._checkpoints if index <= target)
        self.engine.state = copy.deepcopy(self._checkpoints[base])
        count = 0
        for index in range(base, target):
            self.engine.apply(self.events[index])
            count += 1
            if (index + 1) % CHECKPOINT_EVENTS == 0 and index + 1 < len(self.events):
                self._checkpoints[index + 1] = copy.deepcopy(self.engine.state)
        self._checkpoints[target] = copy.deepcopy(self.engine.state)
        self.replay_count += 1
        self.last_replay_events = count
        return count

    def _state_at_index(self, index: int) -> tuple[dict[str, Any], int]:
        index = max(0, min(index, len(self.events)))
        if index in self._checkpoints:
            return copy.deepcopy(self._checkpoints[index]), 0
        base = max(
            checkpoint for checkpoint in self._checkpoints if checkpoint <= index
        )
        return copy.deepcopy(self._checkpoints[base]), index - base

    def snapshot_at(self, ts: float | None = None, gen: int | None = None) -> dict[str, Any]:
        if ts is None:
            index = len(self.events)
        else:
            matched = [
                position
                for position, event in enumerate(self.events)
                if event.ts <= ts and (gen is None or event.gen == gen)
            ]
            index = (matched[-1] + 1) if matched else 0
        state, replays = self._state_at_index(index)
        self.engine.state = state
        if replays:
            sub = self.events[:index]
            for event in sub[len(sub) - replays:]:
                self.engine.apply(event)
            is_final = index == len(self.events)
        else:
            is_final = index == len(self.events)
        snapshot = self.engine.snapshot(include_missing=is_final)
        snapshot["index"] = index
        snapshot["total"] = len(self.events)
        snapshot["replayed"] = replays
        return snapshot

    def final_snapshot(self) -> dict[str, Any]:
        return self.snapshot_at(None)

    def incidents(self) -> list[dict[str, Any]]:
        """Intervals over which each distinct cycle persists."""
        active: dict[tuple[str, ...], dict[str, Any]] = {}
        results: list[dict[str, Any]] = []
        previous_index = 0
        for index in range(1, len(self.events) + 1):
            snapshot = self._light_cycles(index)
            present = {tuple(cycle["nodes"]): cycle for cycle in snapshot}
            for key, cycle in present.items():
                if key not in active:
                    active[key] = {
                        "key": key,
                        "start_index": index,
                        "start_ts": self.events[index - 1].ts,
                        "cycle": cycle,
                    }
            ended = [key for key in active if key not in present]
            for key in ended:
                record = active.pop(key)
                record["end_index"] = previous_index
                record["end_ts"] = self.events[previous_index - 1].ts
                results.append(record)
            previous_index = index
        for record in active.values():
            record["end_index"] = len(self.events)
            record["end_ts"] = self.events[-1].ts if self.events else record["start_ts"]
            results.append(record)
        results.sort(key=lambda record: record["start_index"])
        return results

    def _light_cycles(self, index: int) -> list[dict[str, Any]]:
        state, replays = self._state_at_index(index)
        self.engine.state = state
        for event in self.events[index - replays:index]:
            self.engine.apply(event)
        return self.engine.snapshot(include_missing=False)["cycles"]
