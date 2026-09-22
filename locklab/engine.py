from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


EVENT_KINDS = {
    "thread_start",
    "thread_end",
    "acquire_request",
    "acquire_success",
    "release",
    "cond_wait",
    "cond_notify",
}

LOCK_TYPES = {"mutex", "reentrant", "rwlock"}
CANCEL_KINDS = {"acquire_timeout", "acquire_cancel", "timeout", "cancel"}
ALL_KINDS = EVENT_KINDS | CANCEL_KINDS


@dataclass
class Event:
    id: str
    generation: str
    timestamp: float
    seq: int
    kind: str
    thread: str
    lock: Optional[str] = None
    mode: str = "exclusive"
    stack: List[str] = field(default_factory=list)
    request_id: Optional[str] = None
    cond: Optional[str] = None
    broadcast: bool = False
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def lock_key(self) -> Optional[Tuple[str, str]]:
        if self.lock is None:
            return None
        return (self.generation, self.lock)


@dataclass
class Depth:
    mode: str
    depth: int
    success_id: str


@dataclass
class Waiter:
    thread: str
    mode: str
    request_id: str
    event_id: str
    timestamp: float
    seq: int
    stack: List[str]
    cond_wait_id: Optional[str] = None
    ready: bool = False
    saved_mode: Optional[str] = None
    saved_depth: int = 0
    cond: Optional[str] = None


@dataclass
class CondWait:
    cond: str
    lock: Tuple[str, str]
    thread: str
    mode: str
    depth: int
    event_id: str
    timestamp: float
    seq: int
    stack: List[str]
    broadcast: bool = False


@dataclass
class Anomaly:
    severity: str
    code: str
    message: str
    event_id: Optional[str]
    thread: Optional[str] = None
    lock: Optional[str] = None
    generation: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "event_id": self.event_id,
            "thread": self.thread,
            "lock": self.lock,
            "generation": self.generation,
        }


def normalize_event(row: Dict[str, Any], fallback_seq: int = 0) -> Event:
    kind = str(row.get("kind") or row.get("type") or "")
    if kind not in ALL_KINDS:
        raise ValueError(f"unsupported event kind: {kind}")
    event_id = str(row.get("id") or f"event-{fallback_seq}")
    generation = str(row.get("generation") or row.get("gen") or "default")
    timestamp = float(row.get("timestamp", row.get("ts", fallback_seq)))
    seq = int(row.get("seq", row.get("arrival_seq", fallback_seq)))
    thread = str(row.get("thread") or row.get("thread_id") or "unknown")
    lock = row.get("lock") or row.get("lock_address") or row.get("addr")
    mode = str(row.get("mode") or ("exclusive" if kind != "acquire_request" else row.get("mode", "exclusive")))
    if mode not in {"shared", "exclusive"}:
        mode = "exclusive"
    stack = row.get("stack") or row.get("hold_stack") or []
    if not isinstance(stack, list):
        stack = [str(stack)]
    return Event(
        id=event_id,
        generation=generation,
        timestamp=timestamp,
        seq=seq,
        kind=kind,
        thread=thread,
        lock=None if lock is None else str(lock),
        mode=mode,
        stack=[str(item) for item in stack],
        request_id=(None if row.get("request_id") is None else str(row.request_id)) if False else str(row["request_id"]) if row.get("request_id") is not None else None,
        cond=None if row.get("cond") is None else str(row.get("cond")),
        broadcast=bool(row.get("broadcast", False)),
        raw=dict(row),
    )


def normalize_events(rows: Iterable[Dict[str, Any]]) -> List[Event]:
    events: List[Event] = []
    seen: Set[str] = set()
    for index, row in enumerate(rows):
        event = normalize_event(row, index)
        if event.id in seen:
            raise ValueError(f"duplicate event id: {event.id}")
        seen.add(event.id)
        events.append(event)
    return events


def cycle_signature(cycle: Dict[str, Any]) -> Tuple[str, ...]:
    return tuple(
        f"{edge['thread']}:{edge['lock']}:{edge['kind']}:{edge['source_event_id']}"
        for edge in cycle["edges"]
    )


def event_to_dict(event: Event) -> Dict[str, Any]:
    return {
        "id": event.id,
        "generation": event.generation,
        "timestamp": event.timestamp,
        "seq": event.seq,
        "kind": event.kind,
        "thread": event.thread,
        "lock": event.lock,
        "mode": event.mode,
        "stack": list(event.stack),
        "request_id": event.request_id,
        "cond": event.cond,
        "broadcast": event.broadcast,
        "raw": dict(event.raw),
    }


def out_of_order_events(events: Sequence[Event]) -> List[str]:
    by_generation: Dict[str, List[Event]] = defaultdict(list)
    for event in events:
        by_generation[event.generation].append(event)
    result: Set[str] = set()
    for generation, grouped in by_generation.items():
        arrival_order = sorted(grouped, key=lambda item: (item.seq, item.id))
        previous_time: Optional[float] = None
        for event in arrival_order:
            if previous_time is not None and event.timestamp < previous_time:
                result.add(event.id)
            previous_time = event.timestamp if previous_time is None else max(previous_time, event.timestamp)
    return sorted(result)


def analyze(
    rows_or_events: Sequence[Any],
    lock_types: Optional[Dict[Tuple[str, str], str]] = None,
    reused_events: int = 0,
) -> Dict[str, Any]:
    events = []
    for index, item in enumerate(rows_or_events):
        if isinstance(item, Event):
            events.append(item)
        else:
            events.extend(normalize_events([item]))
    analyzer = Analyzer(events, lock_types)
    out_of_order = set(out_of_order_events(events))
    snapshots: List[Dict[str, Any]] = []
    for index, event in enumerate(analyzer.events):
        analyzer.handle(event)
        generation_anomalies = [item for item in analyzer.anomalies if item.generation in (None, event.generation)]
        anomalies_so_far = [item.to_dict() for item in generation_anomalies]
        if event.id in out_of_order:
            already = any(item.event_id == event.id and item.code == "out_of_order_arrival" for item in generation_anomalies)
            if not already:
                analyzer.warn("medium", "out_of_order_arrival", "event arrived late and changed timestamp ordering", event)
                generation_anomalies = [item for item in analyzer.anomalies if item.generation in (None, event.generation)]
                anomalies_so_far = [item.to_dict() for item in generation_anomalies]
        cycles = analyzer.cycles()
        holds = analyzer.hold_edges()
        waits = analyzer.wait_edges()
        severities = {item["severity"] for item in anomalies_so_far}
        candidate = any(edge["confidence"] == "candidate" for edge in waits)
        if "high" in severities or cycles:
            confidence = "low" if "high" in severities else "candidate"
        elif "medium" in severities or candidate:
            confidence = "candidate"
        else:
            confidence = "high"
        snapshots.append(
            {
                "index": index,
                "event_id": event.id,
                "event": event_to_dict(event),
                "generation": event.generation,
                "timestamp": event.timestamp,
                "arrival_seq": event.seq,
                "confidence": confidence,
                "threads": {
                    name: dict(value)
                    for name, value in analyzer.threads.items()
                    if value.get("state") != "unknown"
                },
                "holds": [edge for edge in holds if edge["generation"] == event.generation],
                "waits": [edge for edge in waits if edge["generation"] == event.generation],
                "cycles": [cycle for cycle in cycles if {edge["generation"] for edge in cycle["edges"]} == {event.generation}],
                "anomalies": anomalies_so_far,
            }
        )
    generations = sorted({event.generation for event in events})
    return {
        "events": [event_to_dict(event) for event in analyzer.events],
        "raw_order": [event_to_dict(event) for event in sorted(events, key=lambda item: (item.seq, item.id))],
        "snapshots": snapshots,
        "generations": generations,
        "anomalies": [item.to_dict() for item in analyzer.anomalies],
        "out_of_order": sorted(out_of_order),
        "reused_events": max(0, min(reused_events, len(events))),
        "recomputed_events": max(0, len(events) - max(0, min(reused_events, len(events)))),
    }


class Analyzer:
    def __init__(self, events: Sequence[Event], lock_types: Optional[Dict[Tuple[str, str], str]] = None):
        self.events = sorted(events, key=lambda item: (item.generation, item.timestamp, item.seq, item.id))
        self.lock_types = dict(lock_types or {})
        self.arrival = {(event.generation, event.id): event.seq for event in events}
        self.anomalies: List[Anomaly] = []
        self.threads: Dict[str, Dict[str, Any]] = {}
        self.depths: Dict[Tuple[str, str], Dict[str, Depth]] = defaultdict(dict)
        self.waiters: Dict[Tuple[str, str], List[Waiter]] = defaultdict(list)
        self.cond_waiters: Dict[str, List[CondWait]] = defaultdict(list)
        self.snapshots: List[Dict[str, Any]] = []
        self._active_waiters: Dict[str, Waiter] = {}

    def lock_type(self, key: Tuple[str, str]) -> str:
        return self.lock_types.get(key, "reentrant")

    def warn(self, severity: str, code: str, message: str, event: Optional[Event] = None) -> None:
        self.anomalies.append(
            Anomaly(
                severity=severity,
                code=code,
                message=message,
                event_id=event.id if event else None,
                thread=event.thread if event else None,
                lock=event.lock if event else None,
                generation=event.generation if event else None,
            )
        )

    def thread_state(self, thread: str) -> str:
        return self.threads.get(thread, {}).get("state", "unknown")

    def owner_threads(self, key: Tuple[str, str]) -> List[str]:
        holders = self.depths[key]
        return [thread for thread, depth in holders.items() if depth.depth > 0]

    def current_mode(self, key: Tuple[str, str]) -> Optional[str]:
        modes = {depth.mode for depth in self.depths[key].values() if depth.depth > 0}
        if not modes:
            return None
        if modes == {"shared"}:
            return "shared"
        return "exclusive"

    def can_enter(self, waiter: Waiter, key: Tuple[str, str]) -> bool:
        holders = {thread: depth for thread, depth in self.depths[key].items() if depth.depth > 0}
        if not holders:
            return True
        lock_type = self.lock_type(key)
        thread_depth = holders.get(waiter.thread)
        if thread_depth is not None:
            if lock_type != "reentrant" and lock_type != "rwlock":
                return False
            if lock_type == "reentrant":
                return True
            if thread_depth.mode == "exclusive":
                return True
            if waiter.mode == "shared":
                return True
            others = [thread for thread in holders if thread != waiter.thread]
            return not others
        return False

    def refresh_waiters(self) -> None:
        self._active_waiters = {}
        for key, queue in self.waiters.items():
            blocked: List[Waiter] = []
            for waiter in queue:
                allowed = self.can_enter(waiter, key)
                if allowed:
                    exclusive_ahead = any(item.mode == "exclusive" for item in blocked)
                    if waiter.mode == "exclusive":
                        waiter.ready = not blocked
                    else:
                        waiter.ready = not exclusive_ahead
                else:
                    waiter.ready = False
                blocked.append(waiter)
                if not waiter.ready:
                    self._active_waiters[waiter.thread] = waiter

    def queue_for(self, thread: str) -> Optional[Waiter]:
        for queue in self.waiters.values():
            for waiter in queue:
                if waiter.thread == thread:
                    return waiter
        return None

    def remove_waiter(self, key: Tuple[str, str], request_id: str) -> Optional[Waiter]:
        queue = self.waiters[key]
        for index, waiter in enumerate(queue):
            if waiter.request_id == request_id:
                return queue.pop(index)
        return None

    def add_depth(self, key: Tuple[str, str], event: Event, mode: str, amount: int = 1) -> None:
        existing = self.depths[key].get(event.thread)
        if existing is None or existing.depth == 0:
            self.depths[key][event.thread] = Depth(mode=mode, depth=amount, success_id=event.id)
        elif existing.mode == mode or self.lock_type(key) == "reentrant":
            existing.depth += amount
            existing.mode = mode
        else:
            existing.depth += amount
            existing.mode = "exclusive"

    def release_depth(self, key: Tuple[str, str], event: Event, amount: Optional[int] = None) -> bool:
        depth = self.depths[key].get(event.thread)
        if depth is None or depth.depth <= 0:
            return False
        if amount is None:
            amount = depth.depth if self.lock_type(key) == "rwlock" else 1
        depth.depth -= max(1, amount)
        if depth.depth <= 0:
            self.depths[key].pop(event.thread, None)
        return True

    def handle(self, event: Event) -> None:
        if event.kind == "thread_start":
            state = self.threads.setdefault(event.thread, {"started": 0, "ended": 0})
            state["state"] = "alive"
            state["started"] += 1
            if state.get("ended", 0) >= state["started"]:
                pass
            return
        if event.kind == "thread_end":
            state = self.threads.setdefault(event.thread, {"started": 0, "ended": 0})
            state["state"] = "ended"
            state["ended"] += 1
            self.cancel_thread(event)
            held = [key for key, holders in self.depths.items() if event.thread in holders]
            for key in held:
                self.warn("high", "held_lock_at_thread_end", "thread ended while still holding a lock", event)
            return
        if event.kind in CANCEL_KINDS:
            self.cancel_request(event)
            return
        if event.lock is None:
            self.warn("medium", "missing_lock", "event does not identify a lock", event)
            return
        key = event.lock_key
        assert key is not None
        if event.kind == "acquire_request":
            self.request(event, key)
        elif event.kind == "acquire_success":
            self.grant(event, key)
        elif event.kind == "release":
            self.release(event, key)
        elif event.kind == "cond_wait":
            self.cond_wait(event, key)
        elif event.kind == "cond_notify":
            self.cond_notify(event)

    def request(self, event: Event, key: Tuple[str, str]) -> None:
        if self.thread_state(event.thread) == "ended":
            self.warn("high", "request_after_thread_end", "terminated thread requested a lock", event)
        existing = self.queue_for(event.thread)
        if existing is not None:
            self.warn("high", "duplicate_pending_request", "thread already has a pending acquire request", event)
            return
        request_id = event.request_id or event.id
        duplicate = any(waiter.request_id == request_id for queue in self.waiters.values() for waiter in queue)
        if duplicate:
            self.warn("high", "duplicate_request_id", "duplicate acquire request id", event)
            return
        waiter = Waiter(
            thread=event.thread,
            mode=event.mode,
            request_id=request_id,
            event_id=event.id,
            timestamp=event.timestamp,
            seq=event.seq,
            stack=event.stack,
        )
        self.waiters[key].append(waiter)
        self.refresh_waiters()

    def grant(self, event: Event, key: Tuple[str, str]) -> None:
        request_id = event.request_id or event.id
        waiter = self.remove_waiter(key, request_id)
        if waiter is None:
            matching = []
            for candidate_key, queue in self.waiters.items():
                for candidate in queue:
                    if candidate.thread == event.thread and candidate_key[1] == event.lock:
                        matching.append((candidate_key, candidate))
            if len(matching) == 1:
                actual_key, waiter = matching[0]
                self.remove_waiter(actual_key, waiter.request_id)
                key = actual_key
            else:
                self.warn("medium", "unmatched_acquire_success", "acquire success has no pending request", event)
                waiter = Waiter(
                    thread=event.thread,
                    mode=event.mode,
                    request_id=request_id,
                    event_id=event.id,
                    timestamp=event.timestamp,
                    seq=event.seq,
                    stack=event.stack,
                )
        mode = waiter.mode
        if self.can_enter(waiter, key):
            self.add_depth(key, event, mode)
        else:
            self.warn("high", "unready_acquire_success", "success arrived while lock was not available", event)
            self.add_depth(key, event, mode)
        self.restore_cond_state(waiter, event)
        self.refresh_waiters()

    def restore_cond_state(self, waiter: Waiter, event: Event) -> None:
        if waiter.cond_wait_id is None:
            return
        if waiter.saved_depth > 1:
            key = event.lock_key
            if key is not None:
                self.add_depth(key, event, waiter.saved_mode or waiter.mode, waiter.saved_depth - 1)

    def release(self, event: Event, key: Tuple[str, str]) -> None:
        if not self.release_depth(key, event):
            self.warn("medium", "release_without_hold", "release has no matching held lock", event)
        self.refresh_waiters()

    def cond_wait(self, event: Event, key: Tuple[str, str]) -> None:
        depth = self.depths[key].get(event.thread)
        if depth is None or depth.depth <= 0:
            self.warn("medium", "cond_wait_without_lock", "condition wait without holding its lock", event)
            return
        cond = event.cond or f"{event.generation}:{event.lock}"
        state = CondWait(
            cond=cond,
            lock=key,
            thread=event.thread,
            mode=depth.mode,
            depth=depth.depth,
            event_id=event.id,
            timestamp=event.timestamp,
            seq=event.seq,
            stack=event.stack,
            broadcast=event.broadcast,
        )
        self.cond_waiters[cond].append(state)
        self.depths[key].pop(event.thread, None)
        if self.queue_for(event.thread) is not None:
            self.warn("high", "cond_wait_with_pending_request", "condition wait overlaps another pending request", event)
        self.refresh_waiters()

    def cond_notify(self, event: Event) -> None:
        cond = event.cond
        if cond is None:
            self.warn("medium", "notify_without_condition", "condition notification has no condition id", event)
            return
        queue = self.cond_waiters.get(cond, [])
        candidates = [item for item in queue if item.lock[0] == event.generation]
        if not queue:
            self.warn("low", "notify_without_waiter", "notification found no condition waiter", event)
            return
        if not candidates:
            self.warn("low", "notify_wrong_generation", "notification only matches another generation", event)
            return
        candidates.sort(key=lambda item: (item.timestamp, item.seq, item.event_id))
        woken = candidates if event.broadcast else candidates[:1]
        for state in woken:
            queue.remove(state)
            waiter = Waiter(
                thread=state.thread,
                mode=state.mode,
                request_id=f"{state.event_id}:reacquire",
                event_id=event.id,
                timestamp=event.timestamp,
                seq=event.seq,
                stack=state.stack,
                cond_wait_id=state.event_id,
                saved_mode=state.mode,
                saved_depth=state.depth,
                cond=state.cond,
            )
            self.waiters[state.lock].append(waiter)
        if not queue:
            self.cond_waiters.pop(cond, None)
        self.refresh_waiters()

    def cancel_request(self, event: Event) -> None:
        if event.lock is None:
            for key, queue in list(self.waiters.items()):
                remaining = [item for item in queue if item.thread != event.thread]
                if len(remaining) != len(queue):
                    if remaining:
                        self.waiters[key] = remaining
                    else:
                        self.waiters.pop(key, None)
                    self.refresh_waiters()
                    return
            self.warn("low", "cancel_without_request", "cancel found no pending request", event)
            return
        key = event.lock_key
        assert key is not None
        request_id = event.request_id or event.raw.get("target_event") or event.id
        waiter = self.remove_waiter(key, str(request_id))
        if waiter is None:
            queue = self.waiters[key]
            remaining = [item for item in queue if item.thread != event.thread]
            found = len(remaining) != len(queue)
            if remaining:
                self.waiters[key] = remaining
            else:
                self.waiters.pop(key, None)
            if not found:
                self.warn("low", "cancel_without_request", "cancel found no pending request", event)
        self.refresh_waiters()

    def cancel_thread(self, event: Event) -> None:
        for key in list(self.waiters.keys()):
            remaining = [waiter for waiter in self.waiters[key] if waiter.thread != event.thread]
            if remaining:
                self.waiters[key] = remaining
            else:
                self.waiters.pop(key, None)
        for cond in list(self.cond_waiters.keys()):
            remaining = [waiter for waiter in self.cond_waiters[cond] if waiter.thread != event.thread]
            if remaining:
                self.cond_waiters[cond] = remaining
            else:
                self.cond_waiters.pop(cond, None)
        self.refresh_waiters()

    def hold_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for (generation, lock), holders in self.depths.items():
            for thread, depth in holders.items():
                if depth.depth <= 0:
                    continue
                edges.append(
                    {
                        "kind": "holds",
                        "source": f"thread:{thread}",
                        "target": f"lock:{generation}:{lock}",
                        "thread": thread,
                        "lock": lock,
                        "generation": generation,
                        "mode": depth.mode,
                        "depth": depth.depth,
                        "lock_type": self.lock_type((generation, lock)),
                        "source_event_id": depth.success_id,
                        "stack": self.event_stack(depth.success_id),
                    }
                )
        return edges

    def wait_edges(self) -> List[Dict[str, Any]]:
        edges = []
        self.refresh_waiters()
        for (generation, lock), queue in self.waiters.items():
            all_holders = {thread: depth for thread, depth in self.depths[(generation, lock)].items() if depth.depth > 0}
            for waiter in queue:
                holders = dict(all_holders)
                if waiter.thread in holders:
                    if self.can_enter(waiter, (generation, lock)) or waiter.mode == "exclusive":
                        holders.pop(waiter.thread)
                if waiter.ready and not holders:
                    continue
                target_threads = list(holders) or ["*ready*"]
                for holder_thread in target_threads:
                    depth = holders.get(holder_thread)
                    edges.append(
                        {
                            "kind": "waits_for",
                            "source": f"thread:{waiter.thread}",
                            "target": "lock:ready" if holder_thread == "*ready*" else f"lock:{generation}:{lock}",
                            "thread": waiter.thread,
                            "holder_thread": None if holder_thread == "*ready*" else holder_thread,
                            "lock": lock,
                            "generation": generation,
                            "request_mode": waiter.mode,
                            "held_mode": None if depth is None else depth.mode,
                            "ready": waiter.ready,
                            "request_id": waiter.request_id,
                            "source_event_id": waiter.event_id,
                            "hold_event_id": None if depth is None else depth.success_id,
                            "stack": waiter.stack,
                            "hold_stack": [] if depth is None else self.event_stack(depth.success_id),
                            "confidence": "high" if holders else "candidate",
                        }
                    )
        for cond, queue in self.cond_waiters.items():
            for waiter in queue:
                generation, lock = waiter.lock
                edges.append(
                    {
                        "kind": "condition_wait",
                        "source": f"thread:{waiter.thread}",
                        "target": f"cond:{generation}:{cond}",
                        "thread": waiter.thread,
                        "holder_thread": None,
                        "lock": lock,
                        "generation": generation,
                        "request_mode": waiter.mode,
                        "held_mode": "condition",
                        "ready": False,
                        "request_id": waiter.event_id,
                        "source_event_id": waiter.event_id,
                        "hold_event_id": waiter.event_id,
                        "stack": waiter.stack,
                        "hold_stack": waiter.stack,
                        "confidence": "candidate",
                    }
                )
        return edges

    def event_stack(self, event_id: str) -> List[str]:
        event = next((item for item in self.events if item.id == event_id), None)
        return event.stack if event else []

    def cycles(self) -> List[Dict[str, Any]]:
        holds = self.hold_edges()
        waits = [edge for edge in self.wait_edges() if edge["kind"] == "waits_for" and edge["holder_thread"]]
        outgoing: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        nodes: Set[str] = set()
        for edge in holds:
            nodes.add(edge["source"])
            nodes.add(edge["target"])
            outgoing[edge["source"]].append(edge)
        for edge in waits:
            generation = edge["generation"]
            lock = edge["lock"]
            holder = edge["holder_thread"]
            wait_edge = dict(edge)
            wait_edge["source"] = f"thread:{edge['thread']}"
            wait_edge["target"] = f"lock:{generation}:{lock}"
            hold_edge = next(
                item for item in holds
                if item["thread"] == holder and item["generation"] == generation and item["lock"] == lock
            )
            source = wait_edge["target"]
            target = f"thread:{holder}"
            nodes.update([wait_edge["source"], source, target])
            outgoing[wait_edge["source"]].append(wait_edge)
            outgoing[source].append({**hold_edge, "kind": "held_by", "source": source, "target": target})
        scc_nodes = self.cyclic_nodes(outgoing, nodes)
        cycles: List[Tuple[Tuple[str, ...], Dict[str, Any]]] = []
        seen: Set[Tuple[str, ...]] = set()

        def visit(start: str, current: str, path_nodes: List[str], path_edges: List[Dict[str, Any]]) -> None:
            for edge in outgoing.get(current, []):
                target = edge["target"]
                if target not in scc_nodes or target in path_nodes:
                    continue
                next_nodes = path_nodes + [target]
                next_edges = path_edges + [edge]
                if target == start:
                    if len([item for item in next_edges if item["kind"] == "waits_for"]) < 2:
                        continue
                    ordered = self.canonical_cycle(path_nodes, next_edges)
                    if ordered not in seen:
                        seen.add(ordered)
                        cycles.append((ordered, self.cycle_dict(list(ordered), next_edges)))
                else:
                    visit(start, target, next_nodes, next_edges)

        for node in sorted(scc_nodes):
            visit(node, node, [node], [])
        cycles.sort(key=lambda item: item[0])
        return [item[1] for item in cycles]

    def canonical_cycle(self, nodes: List[str], edges: List[Dict[str, Any]]) -> Tuple[str, ...]:
        thread_positions = [index for index, node in enumerate(nodes) if node.startswith("thread:")]
        starts = thread_positions or list(range(len(nodes)))
        variants = []
        length = len(nodes)
        for start in starts:
            forward = tuple(nodes[(start + offset) % length] for offset in range(length))
            variants.append(forward)
        return min(variants)

    def cycle_dict(self, signature_nodes: List[str], edges: List[Dict[str, Any]]) -> Dict[str, Any]:
        wait_edges = [edge for edge in edges if edge["kind"] == "waits_for"]
        return {
            "id": "|".join(node.split(":", 1)[-1] for node in signature_nodes),
            "nodes": signature_nodes,
            "edges": wait_edges,
            "threads": sorted({edge["thread"] for edge in wait_edges}),
            "locks": sorted({f'{edge["generation"]}:{edge["lock"]}' for edge in wait_edges}),
        }

    def cyclic_nodes(self, outgoing: Dict[str, List[Dict[str, Any]]], nodes: Set[str]) -> Set[str]:
        index = 0
        indexes: Dict[str, int] = {}
        low: Dict[str, int] = {}
        stack: List[str] = []
        on_stack: Set[str] = set()
        result: Set[str] = set()

        def strongconnect(node: str) -> None:
            nonlocal index
            indexes[node] = index
            low[node] = index
            index += 1
            stack.append(node)
            on_stack.add(node)
            for edge in outgoing.get(node, []):
                target = edge["target"]
                if target not in indexes:
                    strongconnect(target)
                    low[node] = min(low[node], low[target])
                elif target in on_stack:
                    low[node] = min(low[node], indexes[target])
            if low[node] == indexes[node]:
                component = []
                while True:
                    target = stack.pop()
                    on_stack.remove(target)
                    component.append(target)
                    if target == node:
                        break
                if len(component) > 1:
                    result.update(component)

        for node in nodes:
            if node not in indexes:
                strongconnect(node)
        return result
