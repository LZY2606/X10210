from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Iterable

from .models import (
    CANCEL_EVENTS,
    CONDITION_CANCEL_EVENTS,
    Event,
    LockDeclaration,
)


TERMINAL_WAIT_TYPES = CANCEL_EVENTS | CONDITION_CANCEL_EVENTS | {"thread_end"}


@dataclass
class Holder:
    thread: str
    mode: str
    depth: int = 1
    grant_event: int | None = None
    request_event: int | None = None
    stack: list[str] = field(default_factory=list)


@dataclass
class Waiter:
    thread: str
    lock: str | None = None
    condition: str | None = None
    mode: str = "write"
    request_event: int | None = None
    grant_event: int | None = None
    stack: list[str] = field(default_factory=list)
    state: str = "waiting"
    released_holders: list[dict[str, Any]] = field(default_factory=list)
    notified_by: int | None = None


@dataclass
class State:
    holders: dict[str, list[Holder]] = field(default_factory=dict)
    waiters: dict[str, Waiter] = field(default_factory=dict)
    condition_waiters: dict[str, list[Waiter]] = field(default_factory=dict)
    requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    condition_requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    threads: dict[str, dict[str, Any]] = field(default_factory=dict)
    warnings: list[dict[str, Any]] = field(default_factory=list)
    lock_order: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    lock_order_edges: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)


def normalize_mode(mode: str | None) -> str:
    value = (mode or "write").lower()
    if value in {"r", "read", "shared", "share", "s"}:
        return "read"
    return "write"


def lock_kind(declarations: dict[str, LockDeclaration], key: str | None) -> str:
    if key and key in declarations:
        return declarations[key].kind
    return "unknown"


def is_reentrant(kind: str) -> bool:
    return kind in {"reentrant", "recursive", "rwlock"}


def holder_for(state: State, lock: str, thread: str) -> Holder | None:
    for holder in state.holders.get(lock, []):
        if holder.thread == thread:
            return holder
    return None


def other_holders(state: State, lock: str, thread: str) -> list[Holder]:
    return [item for item in state.holders.get(lock, []) if item.thread != thread]


def edge(event: Event, kind: str, source: str, target: str, **extra: Any) -> dict[str, Any]:
    result = {
        "kind": kind,
        "source": source,
        "target": target,
        "event_id": event.id,
        "arrival_seq": event.arrival_seq,
        "timestamp": event.timestamp,
        "thread": event.thread_id,
        "stack": event.stack,
        "confidence": "inferred" if event.inferred else event.confidence,
    }
    result.update(extra)
    return result


def warn(state: State, event: Event, code: str, message: str, confidence: str = "high") -> None:
    state.warnings.append(
        {
            "code": code,
            "message": message,
            "event_id": event.id,
            "arrival_seq": event.arrival_seq,
            "timestamp": event.timestamp,
            "thread": event.thread_id,
            "lock": event.lock_key,
            "confidence": confidence,
        }
    )


def request_key(event: Event) -> str:
    return event.request_id or f"{event.thread_id}:{event.lock_key or event.condition_key}"


def waiter_key(event: Event) -> str:
    target = event.lock_key if event.type == "acquire_request" else event.lock_key
    return event.request_id or f"{event.thread_id}:{target}"


def process_event(
    state: State,
    event: Event,
    declarations: dict[str, LockDeclaration],
    lock_order: dict[str, set[str]],
) -> None:
    state.threads.setdefault(
        event.thread_id,
        {"thread": event.thread_id, "started": False, "ended": False, "events": []},
    )["events"].append(event.id)

    if event.type == "thread_start":
        state.threads[event.thread_id]["started"] = True
        return
    if event.type == "thread_end":
        thread = state.threads[event.thread_id]
        thread["ended"] = True
        _cancel_thread_waits(state, event)
        held = [(key, holders) for key, holders in state.holders.items() if any(h.thread == event.thread_id for h in holders)]
        if held:
            warn(
                state,
                event,
                "thread_ended_holding_locks",
                f"线程 {event.thread_id} 终止时仍持有 {len(held)} 把锁；不补造 release",
                "medium",
            )
        return

    if not state.threads[event.thread_id]["started"]:
        warn(state, event, "missing_thread_start", f"线程 {event.thread_id} 缺少 thread_start", "low")

    if event.type == "acquire_request":
        _process_acquire_request(state, event, declarations)
    elif event.type == "acquire_success":
        _process_acquire_success(state, event, declarations, lock_order)
    elif event.type == "release":
        _process_release(state, event)
    elif event.type == "condition_wait":
        _process_condition_wait(state, event)
    elif event.type in {"condition_notify", "notify"}:
        _process_condition_notify(state, event, all_waiters=False)
    elif event.type in {"condition_notify_all", "notify_all"}:
        _process_condition_notify(state, event, all_waiters=True)
    elif event.type in {"condition_wait_end", "wakeup"}:
        _process_condition_wait_end(state, event, declarations, lock_order)
    elif event.type in CANCEL_EVENTS:
        _process_cancel(state, event)
    elif event.type in CONDITION_CANCEL_EVENTS:
        _process_condition_cancel(state, event)
    else:
        warn(state, event, "unknown_event_type", f"未知事件类型 {event.type}", "high")


def _process_acquire_request(
    state: State, event: Event, declarations: dict[str, LockDeclaration]
) -> None:
    if not event.lock_key:
        warn(state, event, "missing_lock", "acquire_request 缺少 lock", "high")
        return
    mode = normalize_mode(event.mode)
    key = request_key(event)
    current = holder_for(state, event.lock_key, event.thread_id)
    kind = lock_kind(declarations, event.lock_key)
    if current:
        if is_reentrant(kind):
            if kind == "rwlock" and current.mode == "read" and mode == "write":
                if other_holders(state, event.lock_key, event.thread_id):
                    state.waiters[key] = Waiter(
                        event.thread_id,
                        lock=event.lock_key,
                        mode="upgrade",
                        request_event=event.id,
                        stack=event.stack,
                    )
                    state.requests[key] = {"event": event, "granted": False, "mode": "upgrade"}
                else:
                    current.mode = "write"
                    current.depth += 1
                    state.requests[key] = {"event": event, "granted": True, "mode": "upgrade"}
            else:
                current.depth += 1
                state.requests[key] = {"event": event, "granted": True, "mode": mode}
        else:
            kind_label = kind if kind != "unknown" else "未声明"
            warn(
                state,
                event,
                "non_reentrant_reacquire",
                f"{kind_label} 锁 {event.lock_key} 被同一线程重复请求，候选状态标记为自锁",
                "medium",
            )
            state.waiters[key] = Waiter(
                event.thread_id, lock=event.lock_key, mode=mode, request_event=event.id, stack=event.stack
            )
            state.requests[key] = {"event": event, "granted": False, "mode": mode}
        return

    state.waiters[key] = Waiter(
        event.thread_id, lock=event.lock_key, mode=mode, request_event=event.id, stack=event.stack
    )
    state.requests[key] = {"event": event, "granted": False, "mode": mode}


def _process_acquire_success(
    state: State,
    event: Event,
    declarations: dict[str, LockDeclaration],
    lock_order: dict[str, set[str]],
) -> None:
    if not event.lock_key:
        warn(state, event, "missing_lock", "acquire_success 缺少 lock", "high")
        return
    mode = normalize_mode(event.mode)
    key = request_key(event)
    request = state.requests.get(key)
    inferred_request = False
    if not request:
        inferred_request = True
        warn(
            state,
            event,
            "missing_acquire_request",
            f"锁 {event.lock_key} 的 acquire_success 缺少对应 request，按候选请求解释",
            "medium",
        )
    current = holder_for(state, event.lock_key, event.thread_id)
    kind = lock_kind(declarations, event.lock_key)
    if current:
        if is_reentrant(kind):
            if kind == "rwlock" and current.mode == "read" and mode == "write":
                if other_holders(state, event.lock_key, event.thread_id):
                    warn(state, event, "conflicting_upgrade_grant", "仍有其他读者时收到升级成功", "low")
                current.mode = "write"
            current.depth += 1
            current.stack = event.stack or current.stack
        else:
            warn(state, event, "duplicate_grant", f"非重入锁 {event.lock_key} 重复授予同线程", "medium")
    else:
        incompatible = False
        if state.holders.get(event.lock_key):
            if kind == "rwlock":
                holders = state.holders[event.lock_key]
                incompatible = mode == "write" or any(h.mode == "write" for h in holders)
            else:
                incompatible = kind != "unknown" or len(holders := state.holders[event.lock_key]) != 0
        if incompatible:
            warn(state, event, "conflicting_grant", f"锁 {event.lock_key} 在冲突持有下被授予", "low")
        state.holders.setdefault(event.lock_key, []).append(
            Holder(
                event.thread_id,
                mode,
                depth=1,
                grant_event=event.id,
                request_event=request["event"].id if request else None,
                stack=event.stack,
            )
        )

    for held_key, holders in list(state.holders.items()):
        if held_key == event.lock_key:
            continue
        held = next((item for item in holders if item.thread == event.thread_id), None)
        if held:
            state.lock_order[held_key].add(event.lock_key)
            lock_order[held_key].add(event.lock_key)
            state.lock_order_edges[(held_key, event.lock_key, event.thread_id)] = {
                "kind": "lock_order",
                "source": f"lock:{held_key}",
                "target": f"lock:{event.lock_key}",
                "held_lock": held_key,
                "requested_lock": event.lock_key,
                "thread": event.thread_id,
                "hold_event_id": held.grant_event or held.request_event,
                "wait_event_id": event.id,
                "hold_stack": held.stack,
                "wait_stack": event.stack,
            }

    state.waiters.pop(key, None)
    if request:
        request["granted"] = True


def _process_release(state: State, event: Event) -> None:
    if not event.lock_key:
        warn(state, event, "missing_lock", "release 缺少 lock", "high")
        return
    current = holder_for(state, event.lock_key, event.thread_id)
    if not current:
        if state.holders.get(event.lock_key):
            warn(state, event, "release_by_non_owner", f"线程未持有锁 {event.lock_key}，其他线程持有", "medium")
        else:
            warn(state, event, "release_without_holder", f"释放未持有的锁 {event.lock_key}，不补造历史", "medium")
        return
    current.depth -= 1
    if current.depth <= 0:
        state.holders[event.lock_key] = [
            item for item in state.holders[event.lock_key] if item.thread != event.thread_id
        ]
        if not state.holders[event.lock_key]:
            del state.holders[event.lock_key]


def _condition_waiter_key(event: Event) -> str:
    return event.request_id or f"{event.thread_id}:{event.condition_key}:{event.lock_key}"


def _process_condition_wait(state: State, event: Event) -> None:
    if not event.condition_key or not event.lock_key:
        warn(state, event, "missing_condition_or_lock", "condition_wait 需要 target/condition 和 lock", "high")
        return
    current = holder_for(state, event.lock_key, event.thread_id)
    released: list[dict[str, Any]] = []
    if current:
        released = [
            {
                "lock": event.lock_key,
                "mode": current.mode,
                "depth": current.depth,
                "grant_event": current.grant_event,
                "request_event": current.request_event,
                "stack": current.stack,
            }
        ]
        state.holders[event.lock_key] = [
            item for item in state.holders[event.lock_key] if item.thread != event.thread_id
        ]
        if not state.holders[event.lock_key]:
            del state.holders[event.lock_key]
    else:
        warn(state, event, "condition_wait_without_lock", "condition wait 开始时未记录到互斥锁持有", "medium")
    key = _condition_waiter_key(event)
    waiter = Waiter(
        event.thread_id,
        lock=event.lock_key,
        condition=event.condition_key,
        mode="write",
        request_event=event.id,
        stack=event.stack,
        released_holders=released,
    )
    state.waiters[key] = waiter
    state.condition_waiters.setdefault(event.condition_key, []).append(waiter)
    state.condition_requests[key] = {"event": event, "state": "waiting"}


def _process_condition_notify(state: State, event: Event, all_waiters: bool) -> None:
    if not event.condition_key:
        warn(state, event, "missing_condition", "condition notify 缺少 target/condition", "high")
        return
    queue = state.condition_waiters.get(event.condition_key, [])
    ready = [item for item in queue if item.state == "waiting"]
    if not ready:
        warn(state, event, "notify_without_waiter", "通知时没有等待者", "low")
        return
    selected = ready if all_waiters else ready[:1]
    selected_ids = {id(item) for item in selected}
    for item in selected:
        item.state = "reacquire"
        item.notified_by = event.id
        item.stack = event.stack or item.stack
    state.condition_waiters[event.condition_key] = [item for item in queue if id(item) not in selected_ids]
    if not state.condition_waiters[event.condition_key]:
        del state.condition_waiters[event.condition_key]


def _process_condition_wait_end(
    state: State,
    event: Event,
    declarations: dict[str, LockDeclaration],
    lock_order: dict[str, set[str]],
) -> None:
    if not event.condition_key or not event.lock_key:
        warn(state, event, "missing_condition_or_lock", "condition wait 结束缺少 condition 或 lock", "high")
        return
    key = _condition_waiter_key(event)
    waiter = state.waiters.get(key)
    if not waiter or waiter.condition != event.condition_key:
        warn(state, event, "condition_end_without_wait", "condition wait 结束缺少等待开始事件", "medium")
    success = Event(
        event.id,
        event.branch_id,
        event.arrival_seq,
        event.timestamp,
        event.generation,
        event.thread_id,
        "acquire_success",
        lock=event.lock,
        mode="write",
        request_id=event.request_id,
        stack=event.stack,
        raw=event.raw,
        confidence="medium" if not waiter else event.confidence,
        inferred=not waiter,
    )
    _process_acquire_success(state, success, declarations, lock_order)
    if waiter:
        restored = holder_for(state, event.lock_key, event.thread_id)
        if restored and waiter.released_holders:
            restored.depth = waiter.released_holders[0]["depth"]
            restored.request_event = waiter.released_holders[0]["request_event"]
            if not restored.stack:
                restored.stack = waiter.released_holders[0]["stack"]
        state.waiters.pop(key, None)
        state.condition_requests.pop(key, None)


def _process_cancel(state: State, event: Event) -> None:
    key = request_key(event)
    waiter = state.waiters.pop(key, None)
    state.requests.pop(key, None)
    if not waiter:
        warn(state, event, "cancel_without_wait", "取消/超时事件缺少等待中的 acquire request", "low")


def _process_condition_cancel(state: State, event: Event) -> None:
    key = _condition_waiter_key(event)
    waiter = state.waiters.pop(key, None)
    state.condition_requests.pop(key, None)
    if waiter and waiter.condition:
        queue = state.condition_waiters.get(waiter.condition, [])
        state.condition_waiters[waiter.condition] = [item for item in queue if item is not waiter]
    if not waiter:
        warn(state, event, "condition_cancel_without_wait", "condition 取消缺少等待开始", "low")


def _cancel_thread_waits(state: State, event: Event) -> None:
    for key, waiter in list(state.waiters.items()):
        if waiter.thread == event.thread_id:
            state.waiters.pop(key, None)
            state.requests.pop(key, None)
            state.condition_requests.pop(key, None)
            if waiter.condition:
                queue = state.condition_waiters.get(waiter.condition, [])
                state.condition_waiters[waiter.condition] = [item for item in queue if item is not waiter]


def clone_state(state: State) -> State:
    return deepcopy(state)


def build_graphs(state: State, declarations: dict[str, LockDeclaration]) -> dict[str, Any]:
    hold_edges: list[dict[str, Any]] = []
    wait_edges: list[dict[str, Any]] = []
    nodes = {"threads": set(state.threads), "locks": set(), "conditions": set()}

    for lock, holders in state.holders.items():
        nodes["locks"].add(lock)
        for holder in holders:
            evidence = holder.grant_event or holder.request_event
            hold_edges.append(
                {
                    "kind": "holds",
                    "source": f"thread:{holder.thread}",
                    "target": f"lock:{lock}",
                    "thread": holder.thread,
                    "lock": lock,
                    "mode": holder.mode,
                    "depth": holder.depth,
                    "event_id": evidence,
                    "request_event_id": holder.request_event,
                    "stack": holder.stack,
                    "confidence": "observed",
                }
            )

    for waiter in state.waiters.values():
        if waiter.condition:
            nodes["conditions"].add(waiter.condition)
        if waiter.lock:
            nodes["locks"].add(waiter.lock)
        if waiter.condition and waiter.state == "waiting":
            wait_edges.append(
                {
                    "kind": "waits_condition",
                    "source": f"thread:{waiter.thread}",
                    "target": f"condition:{waiter.condition}",
                    "thread": waiter.thread,
                    "condition": waiter.condition,
                    "lock": waiter.lock,
                    "event_id": waiter.request_event,
                    "stack": waiter.stack,
                    "confidence": "observed",
                }
            )
        elif waiter.condition and waiter.state != "reacquire":
            continue
        if not waiter.lock:
            continue
        holders = state.holders.get(waiter.lock, [])
        kind = lock_kind(declarations, waiter.lock)
        blockers = _blocking_holders(waiter, holders, kind)
        for holder in blockers:
            wait_edges.append(
                {
                    "kind": "waits_for",
                    "source": f"thread:{waiter.thread}",
                    "target": f"thread:{holder.thread}",
                    "thread": waiter.thread,
                    "blocker_thread": holder.thread,
                    "lock": waiter.lock,
                    "mode": waiter.mode,
                    "blocker_mode": holder.mode,
                    "event_id": waiter.request_event,
                    "hold_event_id": holder.grant_event or holder.request_event,
                    "wait_stack": waiter.stack,
                    "hold_stack": holder.stack,
                    "confidence": "medium" if kind == "unknown" else "observed",
                }
            )

    lock_order = build_lock_order_graph(state)
    cycles = detect_cycles(wait_edges)
    return {
        "nodes": {key: sorted(value) for key, value in nodes.items()},
        "hold_edges": hold_edges,
        "wait_edges": wait_edges,
        "lock_order_edges": lock_order,
        "cycles": cycles,
        "warnings": state.warnings,
        "confidence": _graph_confidence(state, declarations),
    }


def _blocking_holders(waiter: Waiter, holders: list[Holder], kind: str) -> list[Holder]:
    if waiter.mode == "read" and kind == "rwlock":
        return [item for item in holders if item.mode == "write"]
    if waiter.mode in {"write", "upgrade"} and kind == "rwlock":
        return [item for item in holders if item.thread != waiter.thread]
    return [item for item in holders if item.thread != waiter.thread]


def build_lock_order_graph(state: State) -> list[dict[str, Any]]:
    return list(state.lock_order_edges.values())


def _graph_confidence(state: State, declarations: dict[str, LockDeclaration]) -> dict[str, Any]:
    unknown_locks = {
        lock
        for lock in list(state.holders) + [w.lock for w in state.waiters.values() if w.lock]
        if lock_kind(declarations, lock) == "unknown"
    }
    warning_confidences = [item["confidence"] for item in state.warnings]
    return {
        "level": "candidate" if unknown_locks or warning_confidences else "confirmed",
        "unknown_locks": sorted(unknown_locks),
        "warning_count": len(state.warnings),
        "note": "存在缺事件、乱序或未声明锁类型；图为候选解释，不补造 release",
        if unknown_locks or state.warnings
        else "所有边都由当前分支事件直接支持",
    }


def detect_cycles(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    graph: dict[str, set[str]] = defaultdict(set)
    edge_index: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in edges:
        if item["kind"] not in {"waits_for"}:
            continue
        graph[item["source"]].add(item["target"])
        edge_index[(item["source"], item["target"])].append(item)
    raw_cycles = johnson_cycles(graph)
    cycles: list[dict[str, Any]] = []
    for index, nodes in enumerate(raw_cycles):
        cycle_edges = []
        threads = []
        locks = []
        for pos, source in enumerate(nodes):
            target = nodes[(pos + 1) % len(nodes)]
            evidence = edge_index[(source, target)][0]
            cycle_edges.append(evidence)
            threads.append(evidence["thread"])
            locks.append(evidence["lock"])
        cycles.append(
            {
                "id": f"cycle-{index + 1}",
                "threads": sorted(set(threads)),
                "locks": sorted(set(locks)),
                "nodes": nodes + [nodes[0]],
                "edges": cycle_edges,
                "signature": "->".join(nodes + [nodes[0]]),
                "evidence": [_cycle_evidence(item) for item in cycle_edges],
            }
        )
    return cycles


def _cycle_evidence(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "thread": item["thread"],
        "blocker_thread": item["blocker_thread"],
        "lock": item["lock"],
        "wait_stack": item.get("wait_stack", []),
        "hold_stack": item.get("hold_stack", []),
        "wait_event_id": item["event_id"],
        "hold_event_id": item.get("hold_event_id"),
    }


def johnson_cycles(graph: dict[str, set[str]]) -> list[list[str]]:
    nodes = set(graph)
    for targets in graph.values():
        nodes.update(targets)
    remaining = set(nodes)
    cycles: list[list[str]] = []
    while remaining:
        subgraph = {node: {target for target in graph.get(node, set()) if target in remaining} for node in remaining}
        sccs = strongly_connected_components(subgraph)
        nontrivial = [component for component in sccs if len(component) > 1 or any(node in subgraph.get(node, set()) for node in component)]
        if not nontrivial:
            break
        component = min(nontrivial, key=lambda value: min(value))
        start = min(component)
        component_graph = {node: {target for target in subgraph.get(node, set()) if target in component} for node in component}
        blocked: dict[str, bool] = defaultdict(bool)
        blocked_map: dict[str, set[str]] = defaultdict(set)
        stack: list[str] = []

        def unblock(node: str) -> None:
            blocked[node] = False
            for mapped in list(blocked_map[node]):
                blocked_map[node].discard(mapped)
                if blocked[mapped]:
                    unblock(mapped)

        def circuit(node: str) -> bool:
            found = False
            stack.append(node)
            blocked[node] = True
            for target in sorted(component_graph.get(node, set())):
                if target == start:
                    cycles.append(list(stack))
                    found = True
                elif not blocked[target]:
                    if circuit(target):
                        found = True
            if found:
                unblock(node)
            else:
                for target in component_graph.get(node, set()):
                    blocked_map[target].add(node)
            stack.pop()
            return found

        circuit(start)
        remaining.discard(start)
    return cycles


def strongly_connected_components(graph: dict[str, set[str]]) -> list[set[str]]:
    index = 0
    indices: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    result: list[set[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = low[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for target in graph.get(node, set()):
            if target not in indices:
                visit(target)
                low[node] = min(low[node], low[target])
            elif target in on_stack:
                low[node] = min(low[node], indices[target])
        if low[node] == indices[node]:
            component = set()
            while True:
                target = stack.pop()
                on_stack.remove(target)
                component.add(target)
                if target == node:
                    break
            result.append(component)

    for node in list(graph):
        if node not in indices:
            visit(node)
    return result


class TimelineAnalyzer:
    def __init__(self, events: Iterable[Event], declarations: Iterable[LockDeclaration]):
        self.events = sorted(
            events,
            key=lambda item: (item.timestamp, item.arrival_seq, item.id or 0),
        )
        self.declarations = {item.lock_key: item for item in declarations}
        self.states: list[State] = []
        self._rebuild_from(0)

    def _new_state(self) -> State:
        return State()

    def _rebuild_from(self, index: int) -> None:
        if index == 0:
            self.states = []
            state = self._new_state()
        else:
            del self.states[index:]
            state = clone_state(self.states[-1])
        for event in self.events[index:]:
            lock_order: dict[str, set[str]] = defaultdict(set)
            process_event(state, event, self.declarations, lock_order)
            self.states.append(clone_state(state))

    def append(self, events: Iterable[Event]) -> None:
        new_events = list(events)
        if not new_events:
            return
        had_events = bool(self.events)
        self.events.extend(new_events)
        self.events.sort(key=lambda item: (item.timestamp, item.arrival_seq, item.id or 0))
        new_arrivals = {item.arrival_seq for item in new_events}
        first_changed = min(
            pos
            for pos, event in enumerate(self.events)
            if event.arrival_seq in new_arrivals
        )
        if not had_events:
            self._rebuild_from(0)
        else:
            self._rebuild_from(first_changed)

    def snapshot(self, event_id: int | None = None, time: str | None = None) -> tuple[Event | None, State, int]:
        if event_id is not None:
            index = next((pos for pos, item in enumerate(self.events) if item.id == event_id), len(self.events) - 1)
        elif time is not None:
            index = -1
            for pos, item in enumerate(self.events):
                if item.timestamp <= time:
                    index = pos
                else:
                    break
            if index < 0:
                return None, self._new_state(), -1
        else:
            index = len(self.events) - 1
        if not self.events:
            return None, self._new_state(), -1
        return self.events[index], clone_state(self.states[index]), index

    def graph(self, event_id: int | None = None, time: str | None = None) -> dict[str, Any]:
        event, state, index = self.snapshot(event_id=event_id, time=time)
        result = build_graphs(state, self.declarations)
        result["timeline"] = {
            "total": len(self.events),
            "index": index,
            "selected_event_id": event.id if event else None,
            "selected_timestamp": event.timestamp if event else None,
        }
        return result

    def swimlanes(self) -> list[dict[str, Any]]:
        lanes: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for pos, event in enumerate(self.events):
            lanes[event.thread_id].append(
                {
                    "id": event.id,
                    "index": pos,
                    "timestamp": event.timestamp,
                    "arrival_seq": event.arrival_seq,
                    "type": event.type,
                    "lock": event.lock_key,
                    "condition": event.condition_key,
                    "mode": event.mode,
                    "request_id": event.request_id,
                    "stack": event.stack,
                    "confidence": "inferred" if event.inferred else event.confidence,
                }
            )
        return [
            {"thread": thread, "events": events}
            for thread, events in sorted(lanes.items(), key=lambda item: item[0])
        ]

    def minimize_cycle(self, cycle_id: str, event_id: int | None = None, time: str | None = None) -> dict[str, Any]:
        graph = self.graph(event_id=event_id, time=time)
        cycle = next((item for item in graph["cycles"] if item["id"] == cycle_id), None)
        if cycle is None:
            return {"cycle_id": cycle_id, "events": [], "groups": [], "message": "未找到指定环"}

        groups = self._semantic_groups()
        required_ids = {edge["event_id"] for edge in cycle["edges"]}
        required_ids.update(edge.get("hold_event_id") for edge in cycle["edges"] if edge.get("hold_event_id"))
        keep = {group_id for group_id, group in groups.items() if required_ids.intersection(group["events"])}
        for group_id in list(keep):
            keep.update(groups[group_id].get("prerequisites", set()))

        removable = [group_id for group_id in groups if group_id not in keep]
        for group_id in removable:
            candidate = set(keep)
            candidate.add(group_id)
            if self._groups_reproduce(candidate, groups, cycle["signature"]):
                keep = candidate

        selected = sorted(
            event
            for group_id in keep
            for event in groups[group_id]["events"]
            if event is not None
        )
        return {
            "cycle_id": cycle_id,
            "signature": cycle["signature"],
            "events": [self._event_payload(item) for item in selected],
            "group_count": len(keep),
            "event_count": len(selected),
            "message": "已保持线程配对、重入深度与 condition 释放/重竞争语义",
        }

    def _event_payload(self, event_id: int) -> dict[str, Any]:
        event = next(item for item in self.events if item.id == event_id)
        return {
            "id": event.id,
            "timestamp": event.timestamp,
            "arrival_seq": event.arrival_seq,
            "generation": event.generation,
            "thread_id": event.thread_id,
            "type": event.type,
            "lock": event.lock,
            "target": event.target,
            "mode": event.mode,
            "request_id": event.request_id,
            "stack": event.stack,
        }

    def _semantic_groups(self) -> dict[str, dict[str, Any]]:
        groups: dict[str, dict[str, Any]] = {}
        open_requests: dict[str, int] = {}
        open_conditions: dict[str, int] = {}
        open_locks: dict[tuple[str, str], int] = {}

        for event in self.events:
            if event.type == "acquire_request":
                group_id = f"req-{event.id}"
                groups[group_id] = {"events": [event.id], "prerequisites": set()}
                open_requests[request_key(event)] = group_id
            elif event.type == "acquire_success":
                key = request_key(event)
                group_id = open_requests.pop(key, f"grant-{event.id}")
                groups.setdefault(group_id, {"events": [], "prerequisites": set()})["events"].append(event.id)
                lock_key = (event.thread_id, event.lock_key or "")
                open_locks[lock_key] = group_id
            elif event.type == "release":
                lock_key = (event.thread_id, event.lock_key or "")
                owner_group = open_locks.pop(lock_key, None)
                if owner_group:
                    groups[owner_group]["events"].append(event.id)
                else:
                    groups[f"release-{event.id}"] = {"events": [event.id], "prerequisites": set()}
            elif event.type == "condition_wait":
                group_id = f"cond-{event.id}"
                groups[group_id] = {"events": [event.id], "prerequisites": set()}
                owner_group = open_locks.pop((event.thread_id, event.lock_key or ""), None)
                if owner_group:
                    groups[group_id]["prerequisites"].add(owner_group)
                open_conditions[_condition_waiter_key(event)] = group_id
            elif event.type in {"condition_wait_end", "wakeup"}:
                group_id = open_conditions.pop(_condition_waiter_key(event), f"condend-{event.id}")
                groups.setdefault(group_id, {"events": [], "prerequisites": set()})["events"].append(event.id)
            elif event.type in {"condition_notify", "notify", "condition_notify_all", "notify_all"}:
                groups[f"notify-{event.id}"] = {"events": [event.id], "prerequisites": set()}
            elif event.type in CANCEL_EVENTS | CONDITION_CANCEL_EVENTS | {"thread_start", "thread_end"}:
                groups[f"{event.type}-{event.id}"] = {"events": [event.id], "prerequisites": set()}

        return groups

    def _groups_reproduce(
        self, keep: set[str], groups: dict[str, dict[str, Any]], signature: str
    ) -> bool:
        event_ids = {
            event_id
            for group_id in keep
            for event_id in groups[group_id]["events"]
            if event_id is not None
        }
        selected = [event for event in self.events if event.id in event_ids]
        analyzer = TimelineAnalyzer(selected, self.declarations.values())
        graph = analyzer.graph()
        return any(cycle["signature"] == signature for cycle in graph["cycles"])


def compare_graphs(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    def edge_keys(graph: dict[str, Any]) -> set[tuple[Any, ...]]:
        result = set()
        for group in ("hold_edges", "wait_edges", "lock_order_edges"):
            for edge in graph[group]:
                result.add(
                    (
                        group,
                        edge.get("kind"),
                        edge.get("source"),
                        edge.get("target"),
                        edge.get("thread"),
                        edge.get("lock"),
                    )
                )
        return result

    left_keys = edge_keys(left)
    right_keys = edge_keys(right)
    return {
        "added": sorted(right_keys - left_keys),
        "removed": sorted(left_keys - right_keys),
        "shared": sorted(left_keys & right_keys),
        "left_cycles": left.get("cycles", []),
        "right_cycles": right.get("cycles", []),
        "left_confidence": left.get("confidence"),
        "right_confidence": right.get("confidence"),
    }
