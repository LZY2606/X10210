from __future__ import annotations

from dataclasses import dataclass
from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import bisect
import copy
import re

EVENT_TYPES = {
    "thread_start", "thread_end",
    "acquire_request", "acquired", "release",
    "cond_wait", "cond_notify", "cond_notify_all",
    "timeout", "thread_exit", "process_start", "process_restart",
    "lock_type",
}
TYPE_ALIASES = {
    "start_thread": "thread_start", "thread_begin": "thread_start",
    "end_thread": "thread_end", "thread_finish": "thread_end",
    "acquire": "acquire_request", "lock_request": "acquire_request", "wait_lock": "acquire_request",
    "acquire_success": "acquired", "lock_acquired": "acquired", "grant": "acquired",
    "unlock": "release", "lock_release": "release",
    "wait": "cond_wait", "condition_wait": "cond_wait",
    "signal": "cond_notify", "notify_one": "cond_notify",
    "broadcast": "cond_notify_all", "notify_all": "cond_notify_all",
    "acquire_timeout": "timeout", "request_timeout": "timeout", "cancel_wait": "timeout",
    "exit": "thread_exit", "thread_terminated": "thread_exit",
    "restart": "process_restart", "generation_start": "process_restart",
    "declare_lock_type": "lock_type",
}
MODE_ALIASES = {"r": "read", "shared": "read", "s": "read", "w": "write", "exclusive": "write", "x": "write"}
CONF_RANK = {"high": 3, "medium": 2, "low": 1}


def _min_conf(a: str, b: str) -> str:
    return a if CONF_RANK[a] <= CONF_RANK[b] else b


def normalize_event(raw: Dict[str, Any], seq: Optional[int] = None, prefix: str = "ev") -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("event must be an object")
    source_type = str(raw.get("type", "")).strip()
    event_type = TYPE_ALIASES.get(source_type, source_type)
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported event type: {source_type}")
    event_id = raw.get("id") or f"{prefix}{seq if seq is not None else 0}"
    timestamp = int(raw.get("timestamp", 0))
    arrival = int(raw.get("arrival_seq", seq if seq is not None else 0))
    thread = raw.get("thread_id") or raw.get("thread")
    lock = raw.get("lock") or raw.get("lock_id") or raw.get("lock_addr")
    mode = MODE_ALIASES.get(str(raw.get("mode", "")).lower(), str(raw.get("mode") or "exclusive").lower())
    if mode not in {"read", "write", "exclusive"}:
        mode = "exclusive"
    stack = raw.get("stack") or raw.get("hold_stack") or []
    wait_stack = raw.get("wait_stack") or stack
    if isinstance(stack, str):
        stack = [line for line in re.split(r"\n+", stack) if line.strip()]
    if isinstance(wait_stack, str):
        wait_stack = [line for line in re.split(r"\n+", wait_stack) if line.strip()]
    event = {
        "id": str(event_id),
        "type": event_type,
        "timestamp": timestamp,
        "arrival_seq": arrival,
        "thread_id": str(thread) if thread is not None else None,
        "lock": str(lock) if lock is not None else None,
        "mode": mode,
        "cond": str(raw["cond"]) if raw.get("cond") is not None else None,
        "timeout_ms": raw.get("timeout_ms"),
        "stack": stack,
        "wait_stack": wait_stack,
        "generation": int(raw.get("generation", 0)),
        "lock_type": raw.get("lock_type"),
        "raw": raw.get("raw", raw),
        "inferred": bool(raw.get("inferred", False)),
    }
    if event["lock_type"] not in (None, "mutex", "recursive", "rwlock"):
        raise ValueError(f"unsupported lock_type: {event['lock_type']}")
    if event["timeout_ms"] is not None:
        event["timeout_ms"] = int(event["timeout_ms"])
    return event


def lock_key(lock: str, generation: int) -> str:
    return f"g{generation}:{lock}"


def infer_lock_types(events: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    declared: Dict[str, Dict[str, Any]] = {}
    observed_modes: Dict[str, set] = defaultdict(set)
    for event in events:
        if not event.get("lock"):
            continue
        generation = event["generation"]
        key = lock_key(event["lock"], generation)
        if event["type"] == "lock_type":
            previous = declared.get(key)
            if previous and previous["type"] != event["lock_type"]:
                declared[key] = {"type": event["lock_type"], "confidence": "low", "event_id": event["id"], "conflict": previous["event_id"]}
            else:
                declared[key] = {"type": event["lock_type"], "confidence": "high", "event_id": event["id"]}
        if event["mode"] in {"read", "write"}:
            observed_modes[key].add(event["mode"])
    result: Dict[str, Dict[str, Any]] = {}
    for key, modes in observed_modes.items():
        result[key] = {"type": "rwlock", "confidence": "medium", "event_id": None}
    for key, value in declared.items():
        result[key] = value
    return result


def empty_state(timestamp: int = 0) -> Dict[str, Any]:
    return {
        "timestamp": timestamp,
        "generation": 0,
        "threads": {},
        "locks": {},
        "holders": {},
        "pending": [],
        "cond_state": {},
        "anomalies": [],
        "last_event_id": None,
        "confidence": "high",
    }


def clone_state(state: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(state)

class Reconstruction:
    def __init__(self, events: Sequence[Dict[str, Any]], branch_id: str = "base"):
        self.branch_id = branch_id
        self.events = sorted(events, key=lambda item: (item["timestamp"], item["arrival_seq"]))
        for index, event in enumerate(self.events):
            event["seq"] = index
        self.lock_info = infer_lock_types(self.events)
        self._initial_state = empty_state()
        self._initial_state["locks"] = copy.deepcopy(self.lock_info)
        self._snapshots: List[Tuple[int, int, Dict[str, Any]]] = []
        self._next_check = 32
        self._build()

    def _anomaly(self, state: Dict[str, Any], code: str, message: str, event: Optional[Dict[str, Any]], confidence: str = "medium") -> None:
        state["anomalies"].append({
            "code": code,
            "message": message,
            "confidence": confidence,
            "event_id": event["id"] if event else None,
            "timestamp": event["timestamp"] if event else state["timestamp"],
        })
        state["confidence"] = _min_conf(state["confidence"], confidence)

    def _thread(self, state: Dict[str, Any], thread_id: str) -> Dict[str, Any]:
        thread = state["threads"].setdefault(thread_id, {
            "thread_id": thread_id, "status": "unknown", "started_event": None,
            "ended_event": None, "confidence": "medium",
        })
        return thread

    def _lock(self, state: Dict[str, Any], key: str, lock_addr: str) -> Dict[str, Any]:
        lock_info = state["locks"].setdefault(key, {"type": "mutex", "confidence": "medium", "event_id": None})
        lock_info.setdefault("address", lock_addr)
        lock_info.setdefault("generation", state["generation"])
        return lock_info

    def _holders_for(self, state: Dict[str, Any], key: str) -> List[Dict[str, Any]]:
        return state["holders"].setdefault(key, [])

    def _holder_by_thread(self, state: Dict[str, Any], key: str, thread_id: str) -> Optional[Dict[str, Any]]:
        for holder in self._holders_for(state, key):
            if holder["thread_id"] == thread_id:
                return holder
        return None

    def _pending_indexes(self, state: Dict[str, Any], key: str) -> List[int]:
        return [index for index, item in enumerate(state["pending"]) if item["lock_key"] == key]

    def _first_waiter(self, state: Dict[str, Any], key: str) -> Optional[Dict[str, Any]]:
        indexes = self._pending_indexes(state, key)
        return state["pending"][indexes[0]] if indexes else None

    def _pending_by_id(self, state: Dict[str, Any], request_id: str) -> Optional[Tuple[int, Dict[str, Any]]]:
        for index, item in enumerate(state["pending"]):
            if item["request_event_id"] == request_id:
                return index, item
        return None

    def _is_rw(self, state: Dict[str, Any], key: str, lock_addr: str) -> Dict[str, Any]:
        return self._lock(state, key, lock_addr)

    def _lock_type(self, state: Dict[str, Any], event: Dict[str, Any]) -> str:
        key = lock_key(event["lock"], event["generation"])
        return self._is_rw(state, key, event["lock"])["type"]

    def _can_grant(self, state: Dict[str, Any], key: str, mode: str) -> bool:
        holders = self._holders_for(state, key)
        first = self._first_waiter(state, key)
        if first is not None:
            return False
        lock_info = state["locks"].get(key, {"type": "mutex"})
        if lock_info["type"] == "rwlock":
            if mode == "read":
                return all(holder["mode"] == "read" for holder in holders)
            return not holders
        return not holders

    def _add_holder(self, state: Dict[str, Any], event: Dict[str, Any], mode: str, count: int = 1) -> None:
        key = lock_key(event["lock"], event["generation"])
        holder = self._holder_by_thread(state, key, event["thread_id"])
        if holder is None:
            holder = {
                "thread_id": event["thread_id"], "lock_key": key, "lock": event["lock"],
                "generation": event["generation"], "mode": mode, "count": 0,
                "acquired_event_ids": [], "hold_stacks": [],
            }
            self._holders_for(state, key).append(holder)
        holder["mode"] = mode
        holder["count"] += count
        holder["acquired_event_ids"].append(event["id"])
        holder["hold_stacks"].append(event.get("stack") or [])

    def _remove_pending(self, state: Dict[str, Any], index: int) -> Dict[str, Any]:
        return state["pending"].pop(index)

    def _expire_timeouts(self, state: Dict[str, Any], until: int) -> None:
        expired = []
        remaining = []
        for item in state["pending"]:
            timeout_at = item.get("timeout_at")
            if timeout_at is not None and timeout_at <= until:
                expired.append(item)
            else:
                remaining.append(item)
        state["pending"] = remaining
        for item in expired:
            if item.get("kind") == "condition":
                cond_waiters = state["cond_state"].setdefault(item["cond"], [])
                state["cond_state"][item["cond"]] = [waiter for waiter in cond_waiters if waiter.get("wait_event_id") != item.get("wait_event_id")]
                self._add_holder(state, item["resume_event"], item["resume_mode"], 1)
                state["last_event_id"] = item["resume_event"]["id"]
                self._anomaly(state, "cond_wait_timeout", "condition wait timed out and reacquired the lock", item["resume_event"], "medium")
            else:
                self._anomaly(state, "acquire_timeout", "timed out acquire request was cancelled", item["request_event"], "high")

    def _snapshot_due(self, index: int) -> bool:
        return index == self._next_check

    def _build(self) -> None:
        self._snapshots = [(0, -1, clone_state(self._initial_state))]
        state = clone_state(self._initial_state)
        for index, event in enumerate(self.events):
            self._advance(state, event)
            if self._snapshot_due(index):
                self._snapshots.append((event["timestamp"], index, clone_state(state)))
                self._next_check *= 2

    def _advance(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        self._expire_timeouts(state, event["timestamp"])
        state["timestamp"] = event["timestamp"]
        state["generation"] = event["generation"]
        handler = getattr(self, f"_on_{event['type']}", None)
        if handler:
            handler(state, event)
        state["last_event_id"] = event["id"]

    def state_at(self, timestamp: Optional[int] = None) -> Dict[str, Any]:
        if not self.events:
            state = empty_state(timestamp or 0)
            state["locks"] = copy.deepcopy(self.lock_info)
            return state
        if timestamp is None:
            timestamp = self.events[-1]["timestamp"]
        candidate = self._snapshots[0]
        for snap in self._snapshots:
            if snap[0] <= timestamp:
                candidate = snap
        _, start_index, state = candidate
        state = clone_state(state)
        for index in range(start_index + 1, len(self.events)):
            event = self.events[index]
            if event["timestamp"] > timestamp:
                break
            self._advance(state, event)
        state["timestamp"] = timestamp
        self._expire_timeouts(state, timestamp)
        state["timestamp"] = timestamp
        return state

    def _on_thread_start(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        thread = self._thread(state, event["thread_id"])
        if thread["status"] in {"alive", "ended"}:
            self._anomaly(state, "duplicate_thread_start", "thread started while already known", event, "low")
        thread.update({"status": "alive", "started_event": event["id"], "ended_event": None, "confidence": "high"})

    def _on_thread_end(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        self._finish_thread(state, event, normal=True)

    def _on_thread_exit(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        self._finish_thread(state, event, normal=False)

    def _finish_thread(self, state: Dict[str, Any], event: Dict[str, Any], normal: bool) -> None:
        thread = self._thread(state, event["thread_id"])
        if thread["status"] == "unknown":
            self._anomaly(state, "thread_end_without_start", "thread ended without a start event", event, "low")
        thread["status"] = "ended"
        thread["ended_event"] = event["id"]
        thread["confidence"] = "high"
        state["pending"] = [item for item in state["pending"] if item["thread_id"] != event["thread_id"]]
        for cond, waiters in list(state["cond_state"].items()):
            state["cond_state"][cond] = [waiter for waiter in waiters if waiter["thread_id"] != event["thread_id"]]
        for key, holders in list(state["holders"].items()):
            for holder in list(holders):
                if holder["thread_id"] == event["thread_id"]:
                    self._anomaly(
                        state,
                        "missing_release",
                        f"thread terminated while holding {key}; no release was fabricated",
                        event,
                        "low",
                    )
                    holder["abandoned"] = True
                    holder["abandoned_event_id"] = event["id"]

    def _on_process_start(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        if event["generation"] != state.get("generation", event["generation"]):
            self._reset_generation(state, event["generation"], event)
        state["generation"] = event["generation"]

    def _on_process_restart(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        generation = event["generation"] if event.get("generation", 0) else state["generation"] + 1
        self._reset_generation(state, generation, event)
        state["generation"] = generation

    def _reset_generation(self, state: Dict[str, Any], generation: int, event: Dict[str, Any]) -> None:
        state["holders"] = {key: value for key, value in state["holders"].items() if key.startswith(f"g{generation}:")}
        state["pending"] = [item for item in state["pending"] if item["generation"] == generation]
        state["cond_state"] = {}
        self._anomaly(state, "process_generation", "process generation boundary; reused addresses are distinct locks", event, "high")

    def _on_lock_type(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        key = lock_key(event["lock"], event["generation"])
        lock_info = state["locks"].get(key)
        declared = {"type": event["lock_type"], "confidence": "high", "event_id": event["id"], "address": event["lock"], "generation": event["generation"]}
        if lock_info and lock_info.get("type") != event["lock_type"]:
            declared["confidence"] = "low"
            declared["conflict"] = lock_info.get("event_id")
            self._anomaly(state, "conflicting_lock_type", "lock type declaration conflicts with observed usage", event, "low")
        state["locks"][key] = declared

    def _on_acquire_request(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        thread = self._thread(state, event["thread_id"])
        if thread["status"] == "ended":
            self._anomaly(state, "request_after_thread_end", "acquire request arrived after thread termination", event, "low")
        key = lock_key(event["lock"], event["generation"])
        lock_info = self._is_rw(state, key, event["lock"])
        if event["mode"] in {"read", "write"} and lock_info["type"] != "rwlock":
            lock_info.update({"type": "rwlock", "confidence": "medium", "event_id": None})
        mode = event["mode"]
        if lock_info["type"] != "rwlock":
            mode = "exclusive"
        if self._pending_by_id(state, event["id"])[0] is not None if False else False:
            pass
        duplicate = any(item.get("request_event_id") == event["id"] for item in state["pending"])
        if duplicate:
            self._anomaly(state, "duplicate_acquire_request", "duplicate acquire request ignored", event, "medium")
            return
        item = {
            "kind": "lock",
            "request_event_id": event["id"],
            "request_event": event,
            "thread_id": event["thread_id"],
            "lock_key": key,
            "lock": event["lock"],
            "generation": event["generation"],
            "mode": mode,
            "wait_stack": event.get("wait_stack") or event.get("stack") or [],
            "since": event["timestamp"],
            "timeout_at": event["timestamp"] + event["timeout_ms"] if event.get("timeout_ms") is not None else None,
        }
        if self._can_grant(state, key, mode):
            self._add_holder(state, event, mode)
        else:
            state["pending"].append(item)

    def _on_acquired(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        key = lock_key(event["lock"], event["generation"])
        lock_info = self._is_rw(state, key, event["lock"])
        if event["mode"] in {"read", "write"} and lock_info["type"] != "rwlock":
            lock_info.update({"type": "rwlock", "confidence": "medium", "event_id": None})
        mode = event["mode"] if lock_info["type"] == "rwlock" else "exclusive"
        request_id = event.get("raw", {}).get("request_id")
        match = None
        if request_id is not None:
            found = self._pending_by_id(state, str(request_id))
            if found:
                index, _ = found
                match = self._remove_pending(state, index)
        if match is None:
            candidates = self._pending_indexes(state, key)
            for index in reversed(candidates):
                waiter = state["pending"][index]
                if waiter["thread_id"] == event["thread_id"]:
                    match = self._remove_pending(state, index)
                    break
        if match is not None and str(match.get("kind", "")).startswith("condition"):
            self._restore_condition_holder(state, match, event)
            return
        if match is None:
            holder = self._holder_by_thread(state, key, event["thread_id"])
            if holder is not None:
                if lock_info["type"] == "recursive":
                    self._add_holder(state, event, holder["mode"] if holder["mode"] != "exclusive" else mode, 1)
                    return
                if lock_info["type"] == "rwlock" and holder["mode"] == "read" and mode == "write":
                    self._anomaly(state, "rw_upgrade", "read holder upgraded to write mode", event, "medium")
                    holder["mode"] = "write"
                    holder["count"] += 1
                    holder["acquired_event_ids"].append(event["id"])
                    holder["hold_stacks"].append(event.get("stack") or [])
                    return
                if lock_info["type"] == "mutex":
                    self._anomaly(state, "non_reentrant_relock", "same thread reacquired a plain mutex without release", event, "low")
                    return
                self._add_holder(state, event, mode, 1)
                return
            if self._holders_for(state, key) or self._first_waiter(state, key):
                self._anomaly(state, "acquired_without_grant", "acquired event has no visible compatible request or grant", event, "low")
            else:
                self._anomaly(state, "acquired_without_request", "acquired event has no matching request", event, "medium")
        self._add_holder(state, event, mode, 1)

    def _on_release(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        key = lock_key(event["lock"], event["generation"])
        holders = self._holders_for(state, key)
        holder = self._holder_by_thread(state, key, event["thread_id"])
        if holder is None:
            self._anomaly(state, "release_without_hold", "release does not match any holder", event, "medium")
            return
        release_count = int(event.get("raw", {}).get("count", 1) or 1)
        holder["count"] -= release_count
        if holder["count"] <= 0:
            holders.remove(holder)
            if not holders:
                del state["holders"][key]
        else:
            if len(holder["acquired_event_ids"]) >= release_count:
                del holder["acquired_event_ids"][-release_count:]
                del holder["hold_stacks"][-release_count:]

    def _on_cond_wait(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        if not event.get("cond"):
            self._anomaly(state, "cond_wait_without_cond", "condition wait has no condition id", event, "medium")
            return
        key = lock_key(event["lock"], event["generation"]) if event.get("lock") else None
        holder = self._holder_by_thread(state, key, event["thread_id"]) if key else None
        if key is None:
            self._anomaly(state, "cond_wait_without_lock", "condition wait did not name a temporarily released lock", event, "medium")
            saved = None
        elif holder is None:
            self._anomaly(state, "cond_wait_not_holder", "condition wait arrived while the thread was not a recorded holder", event, "low")
            saved = None
        else:
            saved = copy.deepcopy(holder)
            holders = self._holders_for(state, key)
            holders.remove(holder)
            if not holders:
                del state["holders"][key]
        waiter = {
            "kind": "condition",
            "wait_event_id": event["id"],
            "wait_event": event,
            "request_event_id": event["id"],
            "thread_id": event["thread_id"],
            "cond": event["cond"],
            "lock_key": key,
            "lock": event.get("lock"),
            "generation": event["generation"],
            "saved_holder": saved,
            "resume_mode": (saved or {}).get("mode", "exclusive"),
            "resume_event": event,
            "notified_event_id": None,
            "since": event["timestamp"],
            "timeout_at": event["timestamp"] + event["timeout_ms"] if event.get("timeout_ms") is not None else None,
            "wait_stack": event.get("wait_stack") or event.get("stack") or [],
        }
        state["cond_state"].setdefault(event["cond"], []).append(waiter)
        state["pending"].append(waiter)

    def _on_cond_notify(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        self._notify(state, event, all_waiters=False)

    def _on_cond_notify_all(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        self._notify(state, event, all_waiters=True)

    def _notify(self, state: Dict[str, Any], event: Dict[str, Any], all_waiters: bool) -> None:
        waiters = state["cond_state"].get(event["cond"], [])
        if not waiters:
            self._anomaly(state, "notify_without_waiter", "notification had no recorded condition waiter", event, "low")
            return
        selected = waiters if all_waiters else waiters[:1]
        selected_ids = {item["wait_event_id"] for item in selected}
        state["cond_state"][event["cond"]] = [item for item in waiters if item["wait_event_id"] not in selected_ids]
        for waiter in selected:
            waiter["notified_event_id"] = event["id"]
            waiter["notified_event"] = event
            waiter["kind"] = "condition_reacquire"
            key = waiter.get("lock_key")
            waiter["reason"] = "reacquire"

    def _restore_condition_holder(self, state: Dict[str, Any], waiter: Dict[str, Any], notify_event: Optional[Dict[str, Any]]) -> None:
        saved = waiter.get("saved_holder")
        if saved:
            holder = copy.deepcopy(saved)
            self._holders_for(state, waiter["lock_key"]).append(holder)
        else:
            resume = waiter["resume_event"]
            self._add_holder(state, resume, waiter["resume_mode"], 1)

    def _on_timeout(self, state: Dict[str, Any], event: Dict[str, Any]) -> None:
        request_id = event.get("raw", {}).get("request_id") or event.get("raw", {}).get("wait_event_id")
        cond_name = event.get("cond")
        removed = None
        if request_id is not None:
            for index, item in enumerate(list(state["pending"])):
                if item.get("request_event_id") == str(request_id) or item.get("wait_event_id") == str(request_id):
                    removed = state["pending"].pop(index)
                    break
        if removed is None:
            candidates = list(enumerate(state["pending"]))
            for index, item in reversed(candidates):
                if event.get("thread_id") and item["thread_id"] != event["thread_id"]:
                    continue
                if cond_name is not None and item.get("cond") != cond_name:
                    continue
                if event.get("lock") and item.get("lock") != event.get("lock"):
                    continue
                removed = state["pending"].pop(index)
                break
        if removed is None:
            self._anomaly(state, "timeout_without_request", "timeout does not match a pending request", event, "medium")
            return
        if removed.get("kind", "").startswith("condition"):
            waiters = state["cond_state"].get(removed["cond"], [])
            state["cond_state"][removed["cond"]] = [item for item in waiters if item.get("wait_event_id") != removed.get("wait_event_id")]
            self._anomaly(state, "condition_wait_cancelled", "condition wait was cancelled by timeout or thread termination", event, "high")
        else:
            self._anomaly(state, "acquire_timeout", "acquire request was cancelled by timeout", event, "high")


def _holder_evidence(holder: Dict[str, Any]) -> List[Dict[str, Any]]:
    evidence = []
    for event_id, stack in zip(holder.get("acquired_event_ids", []), holder.get("hold_stacks", [])):
        evidence.append({"event_id": event_id, "stack": stack})
    return evidence


def build_graph_view(state: Dict[str, Any]) -> Dict[str, Any]:
    nodes = {}
    hold_edges = []
    wait_edges = []
    condition_edges = []
    for thread_id, thread in state["threads"].items():
        nodes[f"thread:{thread_id}"] = {"id": f"thread:{thread_id}", "kind": "thread", "label": thread_id, **thread}
    for key, holders in state["holders"].items():
        lock_info = state["locks"].get(key, {"type": "mutex", "confidence": "low"})
        address = holders[0].get("lock") if holders else key.split(":", 1)[-1]
        generation = holders[0].get("generation") if holders else state["generation"]
        nodes[f"lock:{key}"] = {"id": f"lock:{key}", "kind": "lock", "label": address, "lock_key": key, "address": address, "generation": generation, **lock_info}
        for holder in holders:
            nodes.setdefault(f"thread:{holder['thread_id']}", {"id": f"thread:{holder['thread_id']}", "kind": "thread", "label": holder["thread_id"], "status": "unknown", "confidence": "medium"})
            hold_edges.append({
                "id": f"hold:{holder['thread_id']}:{key}",
                "source": f"lock:{key}",
                "target": f"thread:{holder['thread_id']}",
                "thread_id": holder["thread_id"],
                "lock_key": key,
                "lock": holder.get("lock"),
                "generation": holder.get("generation"),
                "mode": holder.get("mode"),
                "count": holder.get("count", 1),
                "abandoned": holder.get("abandoned", False),
                "confidence": "low" if holder.get("abandoned") else "high",
                "evidence_event_ids": list(holder.get("acquired_event_ids", [])),
                "evidence": _holder_evidence(holder),
            })
    for cond in state["cond_state"]:
        nodes[f"cond:{cond}"] = {"id": f"cond:{cond}", "kind": "condition", "label": cond}
    for item in state["pending"]:
        thread_id = item["thread_id"]
        nodes.setdefault(f"thread:{thread_id}", {"id": f"thread:{thread_id}", "kind": "thread", "label": thread_id, "status": "unknown", "confidence": "medium"})
        kind = item.get("kind", "lock")
        if kind == "condition":
            cond = item.get("cond")
            condition_edges.append({
                "id": f"condwait:{thread_id}:{cond}:{item.get('wait_event_id')}",
                "source": f"thread:{thread_id}",
                "target": f"cond:{cond}",
                "thread_id": thread_id,
                "condition": cond,
                "phase": "waiting",
                "confidence": "high",
                "evidence_event_ids": [item.get("wait_event_id")],
                "wait_stack": item.get("wait_stack", []),
            })
            continue
        if kind == "condition_reacquire":
            key = item.get("lock_key")
            if not key:
                continue
            lock_node = f"lock:{key}"
            nodes.setdefault(lock_node, {"id": lock_node, "kind": "lock", "label": item.get("lock"), "lock_key": key, "address": item.get("lock"), "generation": item.get("generation"), "type": "mutex", "confidence": "low"})
            holders = state["holders"].get(key, [])
            blockers = [holder for holder in holders if holder["thread_id"] != thread_id]
            edge = {
                "id": f"wait:{thread_id}:{key}:{item.get('wait_event_id')}",
                "source": f"thread:{thread_id}",
                "target": lock_node,
                "thread_id": thread_id,
                "lock_key": key,
                "lock": item.get("lock"),
                "generation": item.get("generation"),
                "mode": item.get("resume_mode", "exclusive"),
                "phase": "condition-reacquire",
                "reason": "condition_reacquire",
                "wait_event_id": item.get("wait_event_id"),
                "notify_event_id": item.get("notified_event_id"),
                "confidence": "medium" if blockers else "low",
                "evidence_event_ids": [value for value in [item.get("wait_event_id"), item.get("notified_event_id")] if value],
                "wait_stack": item.get("wait_stack", []),
                "blocker_thread_ids": [holder["thread_id"] for holder in blockers],
            }
            wait_edges.append(edge)
            continue
        key = item["lock_key"]
        lock_node = f"lock:{key}"
        info = state["locks"].get(key, {"type": "mutex", "confidence": "low"})
        nodes.setdefault(lock_node, {"id": lock_node, "kind": "lock", "label": item.get("lock"), "lock_key": key, "address": item.get("lock"), "generation": item.get("generation"), **info})
        holders = state["holders"].get(key, [])
        current_holder = next((holder for holder in holders if holder["thread_id"] == thread_id), None)
        blockers = []
        if info.get("type") == "rwlock" and item["mode"] == "read":
            blockers = [holder for holder in holders if holder["mode"] == "write" and holder["thread_id"] != thread_id]
        elif info.get("type") == "rwlock" and item["mode"] == "write":
            blockers = [holder for holder in holders if holder["thread_id"] != thread_id or holder["mode"] == "read"]
        else:
            blockers = [holder for holder in holders if holder["thread_id"] != thread_id]
        first_waiter = state["pending"][0] if state["pending"] else None
        queued_writer = any(waiter.get("lock_key") == key and waiter.get("mode") == "write" for waiter in state["pending"])
        edge = {
            "id": f"wait:{thread_id}:{key}:{item.get('request_event_id')}",
            "source": f"thread:{thread_id}",
            "target": lock_node,
            "thread_id": thread_id,
            "lock_key": key,
            "lock": item.get("lock"),
            "generation": item.get("generation"),
            "mode": item["mode"],
            "phase": "lock",
            "wait_event_id": item.get("request_event_id"),
            "confidence": "high" if blockers else "low",
            "evidence_event_ids": [item.get("request_event_id")],
            "wait_stack": item.get("wait_stack", []),
            "blocker_thread_ids": [holder["thread_id"] for holder in blockers],
            "upgrade": bool(current_holder and current_holder.get("mode") == "read" and item["mode"] == "write"),
            "queued_for_fairness": bool(not blockers and first_waiter and first_waiter is not item),
            "queued_writer": queued_writer,
        }
        wait_edges.append(edge)
    return {
        "timestamp": state["timestamp"],
        "generation": state["generation"],
        "confidence": state["confidence"],
        "nodes": list(nodes.values()),
        "hold_edges": hold_edges,
        "wait_edges": wait_edges,
        "condition_edges": condition_edges,
        "anomalies": state["anomalies"],
        "last_event_id": state["last_event_id"],
        "cycles": find_cycles(nodes, hold_edges, wait_edges),
    }


def find_cycles(nodes: Dict[str, Any], hold_edges: List[Dict[str, Any]], wait_edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    adjacency: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    hold_by_lock_thread: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for edge in hold_edges:
        if edge.get("abandoned"):
            continue
        adjacency[edge["source"]].append(edge)
        hold_by_lock_thread[(edge["target"], edge["source"])] = edge
    for edge in wait_edges:
        if edge.get("blocker_thread_ids"):
            adjacency[edge["source"]].append(edge)
    graph_nodes = set(adjacency)
    for source, edges in adjacency.items():
        graph_nodes.add(source)
        for edge in edges:
            graph_nodes.add(edge["target"])
    cycles = {}

    def canonical(path: Tuple[str, ...]) -> Tuple[str, ...]:
        return min(tuple(path[index:] + path[:index]) for index in range(len(path)))

    def dfs(start: str, node: str, path: List[str], edges: List[Dict[str, Any]]) -> None:
        if len(cycles) >= 32:
            return
        for edge in adjacency.get(node, []):
            if node.startswith("thread:"):
                blocker_ids = edge.get("blocker_thread_ids") or []
                transitions = [(f"thread:{thread_id}", edge, hold_by_lock_thread.get((f"thread:{thread_id}", edge["target"]))) for thread_id in blocker_ids]
            else:
                transitions = [(edge["target"], edge, None)]
            for target, used_edge, hold_edge in transitions:
                cycle_edges = edges + [used_edge]
                if hold_edge:
                    cycle_edges.append(hold_edge)
                if target == start and len(path) >= 3:
                    key = canonical(tuple(path))
                    cycles.setdefault(key, {"nodes": path, "edges": cycle_edges})
                    continue
                if target in path or target < start:
                    continue
                dfs(start, target, path + [target], cycle_edges)

    for node in sorted(item for item in graph_nodes if item.startswith("thread:")):
        dfs(node, node, [node], [])
    result = [_format_cycle(cycle) for cycle in cycles.values()]
    result.sort(key=lambda item: item["signature"])
    return result


def _format_cycle(cycle: Dict[str, Any]) -> Dict[str, Any]:
    thread_ids = []
    lock_keys = []
    evidence_event_ids = []
    hold_stack = []
    wait_stack = []
    edges = []
    for edge in cycle["edges"]:
        copied = {key: value for key, value in edge.items() if key not in {"wait_stack", "hold_stacks", "evidence"}}
        if edge.get("source", "").startswith("thread:"):
            copied["type"] = "wait"
            wait_stack.append({"thread_id": edge.get("thread_id"), "lock_key": edge.get("lock_key"), "event_id": edge.get("wait_event_id"), "stack": edge.get("wait_stack", [])})
        else:
            copied["type"] = "hold"
        if edge.get("thread_id") and edge["thread_id"] not in thread_ids:
            thread_ids.append(edge["thread_id"])
        if edge.get("lock_key") and edge["lock_key"] not in lock_keys:
            lock_keys.append(edge["lock_key"])
        for event_id in edge.get("evidence_event_ids", []):
            if event_id and event_id not in evidence_event_ids:
                evidence_event_ids.append(event_id)
        if copied.get("type") == "hold":
            hold_stack.append({"thread_id": edge.get("thread_id"), "lock_key": edge.get("lock_key"), "event_ids": edge.get("evidence_event_ids", [])})
        edges.append(copied)
    signature = tuple(thread_ids + lock_keys)
    return {
        "signature": " -> ".join(signature),
        "nodes": cycle["nodes"],
        "edges": edges,
        "thread_ids": thread_ids,
        "lock_keys": lock_keys,
        "evidence_event_ids": evidence_event_ids,
        "hold_stack": hold_stack,
        "wait_stack": wait_stack,
    }


class Minimizer:
    def __init__(self, reconstruction: Reconstruction):
        self.reconstruction = reconstruction

    def reduce(self, timestamp: Optional[int] = None, cycle_signature: Optional[str] = None) -> Dict[str, Any]:
        full_view = build_graph_view(self.reconstruction.state_at(timestamp))
        target = self._select_cycle(full_view, cycle_signature)
        if target is None:
            return {"events": [], "cycle": None, "removed": len(self.reconstruction.events), "remaining": 0, "reason": "no_cycle"}
        cutoff = timestamp if timestamp is not None else self.reconstruction.events[-1]["timestamp"]
        candidates = [event for event in self.reconstruction.events if event["timestamp"] <= cutoff]
        groups = self._groups(candidates)
        chosen = self._ddmin_groups(groups, target, full_view)
        selected_events = []
        for group in chosen:
            selected_events.extend(group)
        selected_ids = {event["id"] for event in selected_events}
        selected_events = [event for event in candidates if event["id"] in selected_ids]
        reduced = Reconstruction(selected_events, branch_id="reduced")
        view = build_graph_view(reduced.state_at(max(event["timestamp"] for event in selected_events)))
        cycle = self._select_cycle(view, target["signature"])
        return {
            "events": selected_events,
            "cycle": cycle,
            "removed": len(candidates) - len(selected_events),
            "remaining": len(selected_events),
            "reason": "cycle_preserved",
        }

    def _select_cycle(self, view: Dict[str, Any], signature: Optional[str]) -> Optional[Dict[str, Any]]:
        if not view["cycles"]:
            return None
        if signature:
            for cycle in view["cycles"]:
                if cycle["signature"] == signature:
                    return cycle
        return view["cycles"][0]

    def _groups(self, events: Sequence[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        groups: List[List[Dict[str, Any]]] = []
        grouped: set = set()
        by_id = {event["id"]: event for event in events}
        lock_events = [event for event in events if event["type"] in {"acquire_request", "acquired", "release", "cond_wait", "cond_notify", "cond_notify_all", "timeout"}]
        for event in lock_events:
            grouped.add(event["id"])
        related: List[Dict[str, Any]] = list(lock_events)
        for event in events:
            raw = event.get("raw", {})
            for key in ("request_id", "wait_event_id"):
                value = raw.get(key)
                if value is not None and str(value) in by_id:
                    related_event = by_id[str(value)]
                    if related_event["id"] not in grouped:
                        related.append(related_event)
                        grouped.add(related_event["id"])
        by_thread: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for event in related:
            by_thread[event.get("thread_id")].append(event)
        for thread_events in by_thread.values():
            thread_events.sort(key=lambda item: (item["timestamp"], item["arrival_seq"]))
            groups.append(thread_events)
        for event in events:
            if event["id"] in grouped:
                continue
            if event["type"] in {"thread_start", "thread_end", "thread_exit", "process_start", "process_restart", "lock_type"}:
                groups.append([event])
                grouped.add(event["id"])
        singleton = [event for event in events if event["id"] not in grouped]
        if singleton:
            groups.append(singleton)
        return groups

    def _ddmin_groups(self, groups: List[List[Dict[str, Any]]], target: Dict[str, Any], full_view: Dict[str, Any]) -> List[List[Dict[str, Any]]]:
        current = [list(group) for group in groups]
        chunk_count = 2
        while chunk_count <= len(current):
            chunk_size = len(current) // chunk_count
            reduced = False
            for index in range(chunk_count):
                start = index * chunk_size
                end = len(current) if index == chunk_count - 1 else start + chunk_size
                trial = current[:start] + current[end:]
                if self._valid(trial, target):
                    current = trial
                    reduced = True
                    chunk_count = max(2, chunk_count - 1)
                    break
            if reduced:
                continue
            complements = []
            for index in range(chunk_count):
                start = index * chunk_size
                end = len(current) if index == chunk_count - 1 else start + chunk_size
                complements.append(current[start:end])
            if chunk_count < len(current):
                for index, chunk in enumerate(complements):
                    if self._valid(chunk, target):
                        current = chunk
                        reduced = True
                        chunk_count = max(2, chunk_count - 1)
                        break
            if not reduced:
                if chunk_count >= len(current):
                    break
                chunk_count = min(len(current), chunk_count * 2)
        changed = True
        while changed:
            changed = False
            for index in range(len(current)):
                trial = current[:index] + current[index + 1:]
                if self._valid(trial, target):
                    current = trial
                    changed = True
                    break
        return current

    def _events_from_groups(self, groups: Sequence[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for group in groups:
            result.extend(group)
        return result

    def _valid(self, groups: Sequence[Sequence[Dict[str, Any]]], target: Dict[str, Any]) -> bool:
        events = self._events_from_groups(groups)
        if not events:
            return False
        if not self._pairing_valid(events):
            return False
        reconstruction = Reconstruction(events, branch_id="reduction-check")
        timestamp = max(event["timestamp"] for event in events)
        view = build_graph_view(reconstruction.state_at(timestamp))
        if not self._hard_pairing_valid(view):
            return False
        return self._select_cycle(view, target["signature"]) is not None

    def _pairing_valid(self, events: Sequence[Dict[str, Any]]) -> bool:
        by_id = {event["id"]: event for event in events}
        for event in events:
            raw = event.get("raw", {})
            for key in ("request_id", "wait_event_id"):
                value = raw.get(key)
                if value is not None and str(value) not in by_id:
                    return False
        threads: Dict[str, bool] = defaultdict(bool)
        for event in sorted(events, key=lambda item: (item["timestamp"], item["arrival_seq"])):
            if event["type"] == "thread_start":
                threads[event["thread_id"]] = True
            if event["type"] in {"thread_end", "thread_exit"} and not threads[event["thread_id"]]:
                return False
            if event["type"] == "cond_notify_all":
                if not any(item["type"] == "cond_wait" and item["cond"] == event["cond"] for item in events):
                    return False
        return True

    def _hard_pairing_valid(self, view: Dict[str, Any]) -> bool:
        bad = {"release_without_hold", "acquired_without_grant", "cond_wait_not_holder", "timeout_without_request"}
        return not any(anomaly["code"] in bad for anomaly in view["anomalies"])
