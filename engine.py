from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple


ACQUIRE_REQUEST = "acquire_request"
ACQUIRE_SUCCESS = "acquire_success"
RELEASE = "release"
THREAD_START = "thread_start"
THREAD_END = "thread_end"
CONDITION_WAIT_START = "condition_wait_start"
CONDITION_WAIT_END = "condition_wait_end"
CONDITION_NOTIFY = "condition_notify"
CONDITION_NOTIFY_ALL = "condition_notify_all"
TIMEOUT = "timeout"
LOCK_DECLARE = "lock_declare"

EVENT_TYPES = {
    ACQUIRE_REQUEST,
    ACQUIRE_SUCCESS,
    RELEASE,
    THREAD_START,
    THREAD_END,
    CONDITION_WAIT_START,
    CONDITION_WAIT_END,
    CONDITION_NOTIFY,
    CONDITION_NOTIFY_ALL,
    TIMEOUT,
    LOCK_DECLARE,
}

READ_MODES = {"read", "shared", "R", "r", "S", "s"}
WRITE_MODES = {"write", "exclusive", "W", "w", "X", "x"}


def entity_key(generation: Any, address: str) -> str:
    return f"g{generation}:{address}"


def normalize_mode(value: Optional[str]) -> str:
    if value is None or value == "":
        return "write"
    mode = str(value)
    if mode in READ_MODES:
        return "read"
    if mode in WRITE_MODES:
        return "write"
    return "write"


def normalize_event(raw: Dict[str, Any], seq: Optional[int] = None) -> Dict[str, Any]:
    event = dict(raw or {})
    if "id" not in event or event["id"] in (None, ""):
        event["id"] = event.get("event_id") or f"evt-{seq if seq is not None else event.get('seq', 0)}"
    event["id"] = str(event["id"])
    event["type"] = str(event.get("type", "")).strip()
    event["seq"] = int(event.get("seq", seq if seq is not None else 0) or 0)
    event["timestamp"] = float(event.get("timestamp", event["seq"]) or 0.0)
    event["generation"] = str(event.get("generation", event.get("gen", 0)))
    if event["type"] in {ACQUIRE_REQUEST, ACQUIRE_SUCCESS, RELEASE, TIMEOUT, LOCK_DECLARE}:
        address = event.get("lock", event.get("lock_address", event.get("address")))
        event["lock"] = entity_key(event["generation"], address)
        event["lock_address"] = str(address) if address is not None else ""
    else:
        event["lock"] = str(event.get("lock", "")) if event.get("lock") else ""
        event["lock_address"] = str(event.get("lock_address", ""))
    if CONDITION_WAIT_START in event["type"] or CONDITION_WAIT_END in event["type"]:
        condition = event.get("condition", event.get("condition_address", event.get("address")))
        event["condition"] = entity_key(event["generation"], condition)
        event["condition_address"] = str(condition) if condition is not None else ""
    if "notify" in event["type"]:
        condition = event.get("condition", event.get("condition_address", event.get("address")))
        event["condition"] = entity_key(event["generation"], condition)
        event["condition_address"] = str(condition) if condition is not None else ""
    thread = event.get("thread", event.get("thread_id", event.get("tid")))
    event["thread"] = str(thread) if thread is not None else ""
    request_id = event.get("request_id")
    event["request_id"] = str(request_id) if request_id not in (None, "") else None
    if event["type"] == ACQUIRE_REQUEST:
        mode = normalize_mode(event.get("mode"))
        event["mode"] = mode
        if not event.get("request_id"):
            event["request_id"] = f"req:{event['generation']}:{event['thread']}:{event['lock']}:{event['seq']}"
    elif event["type"] in {ACQUIRE_SUCCESS, TIMEOUT} and event["lock"]:
        event["mode"] = normalize_mode(event.get("mode"))
        if not event.get("request_id"):
            event["request_id"] = f"last:{event['generation']}:{event['thread']}:{event['lock']}"
    else:
        event["mode"] = normalize_mode(event.get("mode")) if event.get("mode") else ""
    stack = event.get("stack") or event.get("stacktrace") or event.get("frames") or []
    if isinstance(stack, str):
        stack = [line for line in stack.splitlines() if line.strip()]
    event["stack"] = [str(frame) for frame in stack]
    event["wait_stack"] = [str(frame) for frame in (event.get("wait_stack") or event.get("wait_frames") or event["stack"])]
    event["note"] = str(event.get("note", "") or "")
    return event


def canonical_events(events: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = [normalize_event(event, index + 1) for index, event in enumerate(events)]
    return sorted(normalized, key=lambda item: (item["timestamp"], item["seq"], item["id"]))


def event_brief(event: Dict[str, Any]) -> Dict[str, Any]:
    keys = (
        "id", "type", "timestamp", "seq", "generation", "thread", "lock", "lock_address",
        "condition", "condition_address", "mode", "request_id", "stack", "wait_stack",
        "note", "outcome",
    )
    return {key: event.get(key) for key in keys if key in event}


@dataclass
class Token:
    thread: str
    lock: str
    mode: str
    acquire_event: Dict[str, Any]
    count: int = 1
    stacks: List[List[str]] = field(default_factory=list)
    inferred: bool = False
    request_id: Optional[str] = None

    def snapshot(self) -> Dict[str, Any]:
        confidence = "low" if self.inferred else "high"
        return {
            "thread": self.thread,
            "lock": self.lock,
            "mode": self.mode,
            "count": self.count,
            "confidence": confidence,
            "request_id": self.request_id,
            "acquire_event": event_brief(self.acquire_event),
            "stacks": list(self.stacks),
        }


@dataclass
class Pending:
    kind: str
    request: Dict[str, Any]
    mode: str
    condition: str = ""
    notified: bool = False
    notified_candidates: List[str] = field(default_factory=list)
    phase: str = "waiting"


@dataclass
class ConditionPause:
    condition: str
    lock: str
    tokens: List[Token]
    wait_event: Dict[str, Any]


class Replayer:
    def __init__(self, candidate: str = "strict") -> None:
        if candidate not in {"strict", "optimistic"}:
            raise ValueError("candidate must be strict or optimistic")
        self.candidate = candidate
        self.events_by_id: Dict[str, Dict[str, Any]] = {}
        self.thread_state: Dict[str, str] = {}
        self.locks: Dict[str, Dict[str, Any]] = {}
        self.holders: Dict[str, List[Token]] = {}
        self.tokens_by_thread: Dict[str, List[Token]] = {}
        self.pending: Dict[str, Pending] = {}
        self.pending_by_thread: Dict[str, Set[str]] = {}
        self.condition_waiters: Dict[str, List[str]] = {}
        self.pauses: Dict[str, ConditionPause] = {}
        self.anomalies: List[Dict[str, Any]] = []
        self.assumptions: List[Dict[str, Any]] = []
        self.groups: List[Set[str]] = []
        self.group_by_event: Dict[str, Set[str]] = {}
        self.last_timestamp: float = 0.0

    def replay(self, events: Sequence[Dict[str, Any]], after_index: Optional[int] = None) -> "Replayer":
        ordered = canonical_events(events)
        if after_index is not None:
            ordered = ordered[:after_index]
        for event in ordered:
            self.handle(event)
        return self

    def clone(self) -> "Replayer":
        return deepcopy(self)

    def _group(self, *event_ids: Any) -> Set[str]:
        ids = [str(event_id) for event_id in event_ids if event_id]
        groups = [self.group_by_event[event_id] for event_id in ids if event_id in self.group_by_event]
        if groups:
            target = groups[0]
            for group in groups[1:]:
                if group is not target:
                    target.update(group)
                    for event_id in group:
                        self.group_by_event[event_id] = target
                    if group in self.groups:
                        self.groups.remove(group)
            for event_id in ids:
                target.add(event_id)
                self.group_by_event[event_id] = target
            return target
        group = set(ids)
        self.groups.append(group)
        for event_id in ids:
            self.group_by_event[event_id] = group
        return group

    def _anomaly(self, kind: str, event: Dict[str, Any], message: str, confidence: str = "medium") -> None:
        self.anomalies.append({
            "kind": kind,
            "message": message,
            "confidence": confidence,
            "event": event_brief(event),
        })

    def _assume(self, kind: str, event: Dict[str, Any], message: str, alternatives: Optional[List[str]] = None) -> None:
        self.assumptions.append({
            "kind": kind,
            "message": message,
            "confidence": "low",
            "alternatives": alternatives or [],
            "event": event_brief(event),
        })

    def _lock_info(self, lock: str) -> Dict[str, Any]:
        return self.locks.setdefault(lock, {"type": "mutex", "reentrant": None, "declared": False})

    def _thread_tokens(self, thread: str, lock: Optional[str] = None) -> List[Token]:
        tokens = self.tokens_by_thread.setdefault(thread, [])
        if lock is None:
            return tokens
        return [token for token in tokens if token.lock == lock]

    def _active_tokens(self, lock: str) -> List[Token]:
        return [token for token in self.holders.setdefault(lock, []) if token.count > 0]

    def _blockers(self, thread: str, lock: str, mode: str, upgrade: bool = False) -> List[Token]:
        info = self._lock_info(lock)
        blockers: List[Token] = []
        for token in self._active_tokens(lock):
            if token.thread == thread:
                if info["type"] == "rwlock":
                    if upgrade and token.mode == "read" and mode == "write":
                        continue
                    if token.mode == "write" and mode == "read" and info.get("reentrant", True) is not False:
                        continue
                    blockers.append(token)
                elif info.get("reentrant", True) is False:
                    blockers.append(token)
                continue
            if info["type"] == "rwlock":
                if mode == "read" and token.mode == "read":
                    continue
            blockers.append(token)
        return blockers

    def handle(self, raw_event: Dict[str, Any]) -> None:
        event = normalize_event(raw_event, len(self.events_by_id) + 1)
        if event["id"] in self.events_by_id:
            self._anomaly("duplicate_event", event, "重复事件已忽略", "high")
            return
        self.events_by_id[event["id"]] = event
        self.last_timestamp = event["timestamp"]
        self._group(event["id"])
        handler = {
            THREAD_START: self._thread_start,
            THREAD_END: self._thread_end,
            LOCK_DECLARE: self._lock_declare,
            ACQUIRE_REQUEST: self._acquire_request,
            ACQUIRE_SUCCESS: self._acquire_success,
            RELEASE: self._release,
            TIMEOUT: self._timeout,
            CONDITION_WAIT_START: self._condition_wait_start,
            CONDITION_WAIT_END: self._condition_wait_end,
            CONDITION_NOTIFY: self._condition_notify,
            CONDITION_NOTIFY_ALL: self._condition_notify_all,
        }.get(event["type"])
        if handler is None:
            self._anomaly("unknown_event", event, f"未知事件类型 {event['type']}", "high")
        else:
            handler(event)

    def _thread_start(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        if self.thread_state.get(thread) == "ended":
            self._anomaly("thread_restarted", event, "线程结束后再次开始，按新生线程处理")
        self.thread_state[thread] = "running"

    def _lock_declare(self, event: Dict[str, Any]) -> None:
        lock = event["lock"]
        lock_type = str(event.get("lock_type", event.get("kind", "mutex"))).lower()
        if lock_type not in {"mutex", "rwlock"}:
            self._anomaly("unknown_lock_type", event, f"未知锁类型 {lock_type}", "high")
            lock_type = "mutex"
        reentrant = event.get("reentrant")
        if reentrant is None:
            reentrant = True if lock_type == "mutex" else None
        self.locks[lock] = {"type": lock_type, "reentrant": bool(reentrant), "declared": True}

    def _add_pending(self, pending: Pending, event: Dict[str, Any]) -> None:
        key = event["request_id"] or f"pending:{event['thread']}:{event['lock']}:{event['id']}"
        event["request_id"] = key
        if key in self.pending:
            self._anomaly("duplicate_pending_request", event, "同一请求重复等待", "high")
            return
        self.pending[key] = pending
        self.pending_by_thread.setdefault(event["thread"], set()).add(key)

    def _remove_pending(self, key: str) -> Optional[Pending]:
        pending = self.pending.pop(key, None)
        if pending:
            self.pending_by_thread.get(pending.request["thread"], set()).discard(key)
        return pending

    def _grant(self, thread: str, lock: str, mode: str, event: Dict[str, Any],
               inferred: bool = False, request_id: Optional[str] = None,
               stacks: Optional[List[List[str]]] = None) -> Token:
        info = self._lock_info(lock)
        existing = next((token for token in self._thread_tokens(thread, lock) if token.count > 0), None)
        if existing:
            recursive = True
            if info["type"] == "rwlock":
                recursive = existing.mode == mode or (existing.mode == "write" and mode == "read")
            if recursive:
                existing.count += 1
                existing.stacks.append(list(event.get("stack") or (stacks[0] if stacks else [])))
                if existing.mode == "read" and mode == "write":
                    existing.mode = "write"
                existing.inferred = existing.inferred and inferred
                if request_id:
                    self._group(existing.acquire_event["id"], event["id"], request_id)
                return existing
        token = Token(
            thread=thread,
            lock=lock,
            mode=mode,
            acquire_event=event,
            count=1,
            stacks=[list(event.get("stack") or (stacks[0] if stacks else []))],
            inferred=inferred,
            request_id=request_id,
        )
        self.holders.setdefault(lock, []).append(token)
        self.tokens_by_thread.setdefault(thread, []).append(token)
        return token

    def _acquire_request(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        lock = event["lock"]
        mode = event["mode"]
        if not thread or not lock:
            self._anomaly("invalid_acquire_request", event, "acquire 请求缺少线程或锁", "high")
            return
        if self.thread_state.get(thread) == "ended":
            self._anomaly("request_after_thread_end", event, "线程结束后出现 acquire 请求", "high")
        blockers = self._blockers(thread, lock, mode, upgrade=True)
        if not blockers:
            if self.candidate == "optimistic":
                self._grant(thread, lock, mode, event, inferred=True, request_id=event["request_id"])
                self._assume(
                    "implicit_grant",
                    event,
                    "未观察到 acquire_success；因当时无冲突，候选推断请求立即成功",
                    ["请求可能直到轨迹结束仍被阻塞"],
                )
            else:
                self._add_pending(Pending("lock", event, mode), event)
        else:
            self._add_pending(Pending("lock", event, mode), event)
            if self.candidate == "optimistic":
                self._assume(
                    "observed_block",
                    event,
                    "存在竞争持有者；请求按等待处理，但缺少显式采集事件",
                    ["采集可能丢失了等待或成功事件"],
                )

    def _acquire_success(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        lock = event["lock"]
        request_id = event.get("request_id")
        pending = self._remove_pending(request_id) if request_id else None
        if pending is None:
            candidates = [
                (key, item)
                for key, item in self.pending.items()
                if item.kind == "lock"
                and item.request["thread"] == thread
                and item.request["lock"] == lock
            ]
            if len(candidates) == 1:
                request_id, pending = candidates[0]
                self._remove_pending(request_id)
                self._anomaly(
                    "request_id_inferred",
                    event,
                    "成功事件缺少请求 ID，已关联到同线程同锁唯一等待",
                    "low",
                )
        mode = pending.mode if pending else event["mode"]
        blockers = self._blockers(thread, lock, mode, upgrade=True)
        if blockers:
            self._anomaly(
                "success_while_locked",
                event,
                "acquire_success 到达时仍存在不兼容持有者；保留为冲突证据",
                "high",
            )
            if pending:
                self._add_pending(pending, pending.request)
            return
        source = pending.request if pending else event
        inferred_existing = None
        if self.candidate == "optimistic" and not pending:
            inferred_existing = next((
                token for token in self._thread_tokens(thread, lock)
                if token.count > 0 and token.inferred and token.mode == mode
            ), None)
        if inferred_existing:
            inferred_existing.inferred = False
            inferred_existing.acquire_event = event
            inferred_existing.stacks.append(list(event.get("stack") or []))
        else:
            self._grant(thread, lock, mode, event, inferred=False, request_id=request_id, stacks=[source.get("stack", [])])
        if pending:
            ids = [pending.request["id"], event["id"]]
            if request_id:
                ids.append(request_id)
            self._group(*ids)

    def _release_once(self, token: Token, event: Dict[str, Any]) -> None:
        token.count -= 1
        token.stacks = token.stacks[:max(token.count, 0)]
        if token.count <= 0:
            if token.lock in self.holders:
                self.holders[token.lock] = [item for item in self.holders[token.lock] if item is not token]
            if token.thread in self.tokens_by_thread:
                self.tokens_by_thread[token.thread] = [
                    item for item in self.tokens_by_thread[token.thread] if item is not token
                ]

    def _release_tokens_for(self, thread: str, lock: str, mode: str = "") -> List[Token]:
        tokens = [token for token in self._thread_tokens(thread, lock) if token.count > 0]
        if not tokens:
            return []
        if mode == "read":
            tokens = [token for token in tokens if token.mode == "read"]
        return tokens

    def _release(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        lock = event["lock"]
        tokens = self._release_tokens_for(thread, lock, event.get("mode") or "")
        if not tokens:
            self._anomaly("release_without_hold", event, "release 未匹配到当前持有；未凭空补 acquire", "high")
            return
        for token in tokens:
            self._release_once(token, event)
            if token.request_id:
                self._group(event["id"], token.request_id)
            self._schedule_ready(event)

    def _held_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for lock, tokens in self.holders.items():
            for token in tokens:
                if token.count <= 0:
                    continue
                edges.append({
                    "id": f"holds:{token.thread}:{lock}:{token.mode}:{token.acquire_event['id']}",
                    "source": token.thread,
                    "target": lock,
                    "type": "holds",
                    "mode": token.mode,
                    "count": token.count,
                    "confidence": "low" if token.inferred else "high",
                    "events": [event_brief(token.acquire_event)],
                    "stack": token.stacks[-1] if token.stacks else token.acquire_event.get("stack", []),
                    "stacks": list(token.stacks),
                    "provenance": f"held from {token.acquire_event['id']}",
                })
        return edges

    def _wait_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for key, pending in self.pending.items():
            request = pending.request
            if pending.kind == "condition":
                edges.append({
                    "id": f"waits-condition:{key}",
                    "source": request["thread"],
                    "target": pending.condition,
                    "type": "waits_condition",
                    "mode": pending.mode,
                    "confidence": "medium" if pending.notified_candidates else "high",
                    "notified": pending.notified,
                    "phase": pending.phase,
                    "events": [event_brief(request)],
                    "stack": request.get("wait_stack", request.get("stack", [])),
                    "provenance": f"waiting since {request['id']}",
                    "candidate_waiters": pending.notified_candidates,
                })
                continue
            blockers = self._blockers(request["thread"], request["lock"], pending.mode, upgrade=True)
            if not blockers:
                confidence = "medium"
            else:
                confidence = "high"
            for blocker in blockers:
                edges.append({
                    "id": f"waits-for:{key}:{blocker.thread}:{blocker.lock}:{blocker.acquire_event['id']}",
                    "source": request["thread"],
                    "target": blocker.thread,
                    "type": "waits_for",
                    "lock": request["lock"],
                    "mode": pending.mode,
                    "blocker_mode": blocker.mode,
                    "blocker_count": blocker.count,
                    "confidence": confidence,
                    "events": [event_brief(request), event_brief(blocker.acquire_event)],
                    "wait_event": event_brief(request),
                    "hold_event": event_brief(blocker.acquire_event),
                    "wait_stack": request.get("wait_stack", request.get("stack", [])),
                    "hold_stack": blocker.stacks[-1] if blocker.stacks else blocker.acquire_event.get("stack", []),
                    "stack": request.get("wait_stack", request.get("stack", [])),
                    "provenance": f"wait {request['id']} blocked by {blocker.acquire_event['id']}",
                })
        return edges

    def _nodes(self, held_edges: Sequence[Dict[str, Any]], wait_edges: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        node_ids: Set[str] = set()
        nodes: List[Dict[str, Any]] = []

        def add(node_id: str, kind: str, status: str = "active") -> None:
            if node_id and node_id not in node_ids:
                node_ids.add(node_id)
                nodes.append({"id": node_id, "kind": kind, "status": status})

        for thread, status in self.thread_state.items():
            add(thread, "thread", status)
        for lock in self.locks:
            add(lock, "lock")
        for edge in held_edges:
            add(edge["source"], "thread", self.thread_state.get(edge["source"], "unknown"))
            add(edge["target"], "lock")
        for edge in wait_edges:
            add(edge["source"], "thread", self.thread_state.get(edge["source"], "unknown"))
            if edge["type"] == "waits_for":
                add(edge["target"], "thread", self.thread_state.get(edge["target"], "unknown"))
                add(edge["lock"], "lock")
            else:
                add(edge["target"], "condition")
        for condition in self.condition_waiters:
            add(condition, "condition")
        return nodes

    def detect_cycles(self, wait_edges: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        graph: Dict[str, List[Dict[str, Any]]] = {}
        for edge in wait_edges:
            if edge["type"] != "waits_for":
                continue
            graph.setdefault(edge["source"], []).append(edge)
        cycles: List[Dict[str, Any]] = []
        seen_signatures: Set[Tuple[str, ...]] = set()

        def signature(path: List[Dict[str, Any]]) -> Tuple[str, ...]:
            threads = [path[0]["source"]] + [edge["target"] for edge in path]
            rotations = []
            for offset in range(len(threads) - 1):
                rotations.append(tuple(threads[offset:-1] + threads[:offset]))
            return min(rotations)

        def walk(start: str, current: str, path: List[Dict[str, Any]], visited: Set[str]) -> None:
            for edge in graph.get(current, []):
                next_node = edge["target"]
                if next_node == start and path:
                    candidate_path = path + [edge]
                    sig = signature(candidate_path)
                    if sig in seen_signatures:
                        continue
                    seen_signatures.add(sig)
                    threads = [candidate_path[0]["source"]] + [item["target"] for item in candidate_path]
                    locks = [item["lock"] for item in candidate_path]
                    cycles.append({
                        "id": "cycle:" + ">".join(threads[:-1] + [threads[0]]),
                        "threads": threads[:-1],
                        "locks": locks,
                        "signature": list(sig),
                        "edges": list(candidate_path),
                        "evidence": [{
                            "waiting_thread": item["source"],
                            "holder_thread": item["target"],
                            "lock": item["lock"],
                            "wait_event": item.get("wait_event"),
                            "hold_event": item.get("hold_event"),
                            "wait_stack": item.get("wait_stack", []),
                            "hold_stack": item.get("hold_stack", []),
                            "confidence": item["confidence"],
                        } for item in candidate_path],
                    })
                    continue
                if next_node in visited:
                    continue
                visited.add(next_node)
                walk(start, next_node, path + [edge], visited)
                visited.remove(next_node)

        for node in list(graph):
            walk(node, node, [], {node})
        return cycles

    def snapshot(self, timestamp: Optional[float] = None,
                 event_id: Optional[str] = None, include_event: bool = True) -> Dict[str, Any]:
        events = list(self.events_by_id.values())
        ordered = canonical_events(events)
        if event_id is not None:
            limit = next((index for index, event in enumerate(ordered) if event["id"] == event_id), None)
            if limit is None:
                raise KeyError(f"unknown event {event_id}")
            if include_event:
                limit += 1
        elif timestamp is not None:
            limit = sum(1 for event in ordered if event["timestamp"] <= timestamp)
        else:
            limit = len(ordered)
        replayer = Replayer(self.candidate).replay(ordered[:limit])
        held_edges = replayer._held_edges()
        wait_edges = replayer._wait_edges()
        cycles = replayer.detect_cycles(wait_edges)
        confidences = [edge["confidence"] for edge in held_edges + wait_edges]
        if "high" in confidences:
            graph_confidence = "high"
        elif "medium" in confidences:
            graph_confidence = "medium"
        else:
            graph_confidence = "low"
        current_event = ordered[limit - 1] if limit else None
        return {
            "candidate": self.candidate,
            "timestamp": current_event["timestamp"] if current_event else 0.0,
            "event_index": limit,
            "current_event": event_brief(current_event) if current_event else None,
            "nodes": replayer._nodes(held_edges, wait_edges),
            "held_graph": {"nodes": replayer._nodes(held_edges, []), "edges": held_edges},
            "wait_graph": {"nodes": replayer._nodes([], wait_edges), "edges": wait_edges},
            "cycles": cycles,
            "confidence": graph_confidence,
            "assumptions": replayer.assumptions,
            "anomalies": replayer.anomalies,
            "thread_state": dict(replayer.thread_state),
            "locks": {
                lock: {
                    "type": info["type"],
                    "reentrant": info["reentrant"],
                    "declared": info["declared"],
                }
                for lock, info in replayer.locks.items()
            },
        }

    def _schedule_ready(self, release_event: Optional[Dict[str, Any]] = None) -> None:
        if self.candidate != "optimistic":
            return
        for key in list(self.pending):
            pending = self.pending.get(key)
            if not pending or pending.kind != "lock" or pending.phase != "waiting":
                continue
            request = pending.request
            blockers = self._blockers(request["thread"], request["lock"], pending.mode, upgrade=True)
            if not blockers:
                self._remove_pending(key)
                grant_event = dict(request)
                grant_event["id"] = f"inferred:{key}"
                grant_event["timestamp"] = (release_event or request)["timestamp"]
                grant_event["seq"] = (release_event or request)["seq"] + 0.5
                grant_event["stack"] = request.get("stack", [])
                grant_event["request_id"] = key
                self._grant(request["thread"], request["lock"], pending.mode, grant_event,
                            inferred=True, request_id=key)
                self._assume(
                    "scheduled_grant",
                    grant_event,
                    "阻塞消失后缺少显式成功；候选推断请求获得锁",
                    ["其他等待者可能先获得锁"],
                )

    def _timeout(self, event: Dict[str, Any]) -> None:
        request_id = event.get("request_id")
        pending = self._remove_pending(request_id) if request_id else None
        if pending is None:
            candidates = [
                (key, item)
                for key, item in list(self.pending.items())
                if item.request["thread"] == event["thread"]
                and (not event.get("lock") or item.request.get("lock") == event["lock"])
            ]
            if len(candidates) == 1:
                request_id, pending = candidates[0]
                self._remove_pending(request_id)
        if pending is None:
            self._anomaly("timeout_without_wait", event, "timeout 未匹配到等待请求", "medium")
            return
        self._group(event["id"], pending.request["id"], request_id)

    def _condition_wait_start(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        lock = event["lock"]
        condition = event["condition"]
        if not thread or not lock or not condition:
            self._anomaly("invalid_condition_wait", event, "condition wait 缺少线程、锁或条件变量", "high")
            return
        tokens = [token for token in self._thread_tokens(thread, lock) if token.count > 0]
        if not tokens:
            self._anomaly("condition_wait_without_lock", event, "condition wait 未持有关联锁", "high")
            return
        saved = []
        for token in tokens:
            saved.append(deepcopy(token))
            original_count = token.count
            for _ in range(original_count):
                self._release_once(token, event)
            self._group(event["id"], token.acquire_event["id"], token.request_id)
        pause = ConditionPause(condition=condition, lock=lock, tokens=saved, wait_event=event)
        self.pauses[thread] = pause
        pending = Pending("condition", event, saved[0].mode if saved else "write", condition=condition)
        self._add_pending(pending, event)
        self.condition_waiters.setdefault(condition, []).append(event["request_id"])
        self._schedule_ready(event)

    def _condition_notify(self, event: Dict[str, Any]) -> None:
        waiters = list(self.condition_waiters.get(event["condition"], []))
        active = [key for key in waiters if key in self.pending and self.pending[key].phase == "waiting"]
        if not active:
            self._anomaly("notify_without_waiter", event, "notify 时没有可唤醒等待者", "low")
            return
        for key in active:
            self.pending[key].notified_candidates = list(active)
        chosen = active[0]
        self.pending[chosen].notified = True
        self.pending[chosen].phase = "notified"
        self._group(event["id"], chosen)
        self._assume(
            "notify_target",
            event,
            "无法从单个 notify 确定被唤醒线程；保留并列候选",
            active,
        )

    def _condition_notify_all(self, event: Dict[str, Any]) -> None:
        waiters = self.condition_waiters.get(event["condition"], [])
        active = [key for key in waiters if key in self.pending and self.pending[key].phase == "waiting"]
        if not active:
            self._anomaly("notify_all_without_waiter", event, "notifyAll 时没有可唤醒等待者", "low")
            return
        for key in active:
            self.pending[key].notified = True
            self.pending[key].phase = "notified"
            self._group(event["id"], key)

    def _condition_wait_end(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        matches = [
            (key, pending)
            for key, pending in self.pending.items()
            if pending.kind == "condition" and pending.request["thread"] == thread
        ]
        if not matches:
            self._anomaly("condition_end_without_wait", event, "condition wait 结束缺少等待开始", "high")
            return
        key, pending = matches[-1]
        pause = self.pauses.pop(thread, None)
        self.condition_waiters[pending.condition] = [
            item for item in self.condition_waiters.get(pending.condition, []) if item != key
        ]
        self._remove_pending(key)
        self._group(event["id"], pending.request["id"], key)
        if str(event.get("outcome", "")).lower() in {"timeout", "timed_out", "cancelled"}:
            self._restore_pause(pause, pending, event, infer=True)
            return
        if pending.phase != "notified":
            self._assume(
                "condition_end_without_notify",
                event,
                "condition wait 结束前未观察到 notify；候选推断为伪唤醒或丢失通知",
                ["等待可能尚未结束"],
            )
        self._restore_pause(pause, pending, event, infer=self.candidate == "optimistic" and not pending.notified)

    def _restore_pause(self, pause: Optional[ConditionPause], pending: Pending,
                       event: Dict[str, Any], infer: bool = False) -> None:
        if pause is None:
            self._anomaly("condition_restore_without_pause", event, "无法恢复 condition wait 前持有的锁", "high")
            return
        blockers = self._blockers(event["thread"], pause.lock, pending.mode, upgrade=True)
        if blockers:
            re_request = dict(pending.request)
            re_request["id"] = f"reacquire:{key_placeholder(pending)}"
            re_request["timestamp"] = event["timestamp"]
            re_request["seq"] = event["seq"]
            re_request["mode"] = pending.mode
            re_pending = Pending("lock", re_request, pending.mode, condition=pending.condition, phase="waiting")
            self._add_pending(re_pending, re_request)
            self._assume(
                "condition_reacquire_blocked",
                event,
                "condition wait 返回后关联锁被他人持有，线程重新进入等待",
                ["可能已被唤醒但尚未重新获得锁"],
            )
            return
        for old_token in pause.tokens:
            restore_event = event
            inferred = False
            if infer:
                restore_event = dict(event)
                restore_event["id"] = f"inferred-restore:{event['id']}:{old_token.lock}"
                inferred = True
            self._grant(
                event["thread"],
                old_token.lock,
                old_token.mode,
                restore_event,
                inferred=inferred,
                request_id=old_token.request_id,
                stacks=old_token.stacks,
            )

    def _thread_end(self, event: Dict[str, Any]) -> None:
        thread = event["thread"]
        self.thread_state[thread] = "ended"
        for key in list(self.pending_by_thread.get(thread, set())):
            pending = self._remove_pending(key)
            if pending:
                if pending.kind == "condition":
                    self.condition_waiters[pending.condition] = [
                        item for item in self.condition_waiters.get(pending.condition, []) if item != key
                    ]
                self.pauses.pop(thread, None)
                self._group(event["id"], pending.request["id"], key)
        held = list(self._thread_tokens(thread))
        if held:
            self._anomaly(
                "thread_ended_holding_locks",
                event,
                f"线程终止时仍持有 {len(held)} 个锁；未推断 release",
                "high",
            )


def key_placeholder(pending: Pending) -> str:
    return pending.request.get("request_id") or pending.request["id"]
