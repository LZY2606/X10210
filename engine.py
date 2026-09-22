from __future__ import annotations

from bisect import bisect_left, bisect_right
from copy import deepcopy
from dataclasses import dataclass, field
from hashlib import sha1
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid


@dataclass(frozen=True)
class ResourceKey:
    kind: str
    process_id: str
    process_generation: str
    name: str

    def label(self) -> str:
        prefix = ""
        if self.process_generation:
            prefix = f"{self.process_generation}:{self.process_id}:"
        elif self.process_id and self.process_id != "proc":
            prefix = f"{self.process_id}:"
        return f"{prefix}{self.name}"

    def as_dict(self) -> Dict[str, str]:
        return {
            "kind": self.kind,
            "process_id": self.process_id,
            "process_generation": self.process_generation,
            "name": self.name,
            "label": self.label(),
        }


@dataclass(frozen=True)
class Event:
    id: str
    ts: float
    seq: int
    collection_gen: int
    process_id: str
    process_generation: str
    thread: str
    kind: str
    lock: str = ""
    condition: str = ""
    mode: str = "X"
    upgrade: bool = False
    notify_all: bool = False
    stack: Tuple[str, ...] = field(default_factory=tuple)
    source_event_id: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def key(self, kind: str = "lock", name: Optional[str] = None) -> ResourceKey:
        actual_name = name if name is not None else (self.lock if kind == "lock" else self.condition)
        return ResourceKey(kind, self.process_id, self.process_generation, actual_name or "")

    @property
    def lock_key(self) -> ResourceKey:
        return self.key("lock", self.lock)

    @property
    def condition_key(self) -> ResourceKey:
        return self.key("condition", self.condition)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "ts": self.ts,
            "seq": self.seq,
            "collection_gen": self.collection_gen,
            "process_id": self.process_id,
            "process_generation": self.process_generation,
            "thread": self.thread,
            "kind": self.kind,
            "lock": self.lock,
            "lock_label": self.lock_key.label() if self.lock else "",
            "condition": self.condition,
            "condition_label": self.condition_key.label() if self.condition else "",
            "mode": self.mode,
            "upgrade": self.upgrade,
            "notify_all": self.notify_all,
            "stack": list(self.stack),
            "source_event_id": self.source_event_id,
        }


KIND_ALIASES = {
    "acquire": "acquire_request",
    "lock_acquire": "acquire_request",
    "lock_request": "acquire_request",
    "wait_lock": "acquire_request",
    "request": "acquire_request",
    "acquired": "acquire_success",
    "lock_acquired": "acquire_success",
    "success": "acquire_success",
    "grant": "acquire_success",
    "unlock": "release",
    "lock_release": "release",
    "released": "release",
    "start": "thread_start",
    "thread_begin": "thread_start",
    "begin": "thread_start",
    "exit": "thread_end",
    "thread_exit": "thread_end",
    "stop": "thread_end",
    "terminate": "thread_end",
    "cond_wait": "condition_wait",
    "condition_await": "condition_wait",
    "await": "condition_wait",
    "wait_condition": "condition_wait",
    "cond_notify": "condition_notify",
    "condition_signal": "condition_notify",
    "notify": "condition_notify",
    "signal": "condition_notify",
    "notify_all": "condition_notify",
    "broadcast": "condition_notify",
    "timeout": "wait_timeout",
    "timed_out": "wait_timeout",
    "wait_timeout": "wait_timeout",
    "acquire_timeout": "wait_timeout",
    "cancel": "wait_timeout",
    "canceled": "wait_timeout",
    "cancelled": "wait_timeout",
}

MODE_ALIASES = {
    "": "X",
    "x": "X",
    "exclusive": "X",
    "write": "X",
    "w": "X",
    "writer": "X",
    "s": "S",
    "shared": "S",
    "read": "S",
    "r": "S",
    "reader": "S",
    "u": "X",
    "upgrade": "X",
}


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def event_from_dict(data: Dict[str, Any], seq_override: Optional[int] = None) -> Event:
    raw = dict(data)
    event_id = str(data.get("id") or _new_id("ev"))
    ts_value = data.get("ts", data.get("timestamp", 0))
    ts = float(ts_value if ts_value is not None else 0)
    seq = int(seq_override if seq_override is not None else data.get("seq", data.get("ingest_seq", 0)))
    collection_gen = int(data.get("generation", data.get("collection_gen", 0)) or 0)
    process_id = str(data.get("process_id", data.get("pid", "proc")) or "proc")
    process_generation = str(
        data.get("process_generation", data.get("boot_id", data.get("process_epoch", ""))) or ""
    )
    thread = str(data.get("thread", data.get("thread_id", data.get("tid", ""))) or "")
    kind_value = str(data.get("kind", data.get("type", "")) or "").lower()
    kind = KIND_ALIASES.get(kind_value, kind_value)
    mode_value = str(data.get("mode", data.get("lock_mode", "")) or "").lower()
    upgrade = bool(data.get("upgrade", False)) or mode_value in {"u", "upgrade"}
    mode = MODE_ALIASES.get(mode_value, "X")
    stack_value = data.get("stack", data.get("stack_trace", [])) or []
    if isinstance(stack_value, str):
        stack = tuple(line.strip() for line in stack_value.splitlines() if line.strip())
    else:
        stack = tuple(str(frame) for frame in stack_value)
    notify_all = bool(
        data.get("notify_all", False)
        or kind_value in {"notify_all", "broadcast"}
        or str(data.get("kind", "")).lower() == "notify_all"
    )
    return Event(
        id=event_id,
        ts=ts,
        seq=seq,
        collection_gen=collection_gen,
        process_id=process_id,
        process_generation=process_generation,
        thread=thread,
        kind=kind,
        lock=str(data.get("lock", data.get("lock_addr", data.get("mutex", ""))) or ""),
        condition=str(data.get("condition", data.get("cond", data.get("cv", ""))) or ""),
        mode=mode,
        upgrade=upgrade,
        notify_all=notify_all,
        stack=stack,
        source_event_id=str(data.get("source_event_id", data.get("corrected_from", "")) or ""),
        raw=raw,
    )


def declaration_key(declaration: Dict[str, Any]) -> ResourceKey:
    if "key" in declaration and isinstance(declaration["key"], dict):
        part = declaration["key"]
        return ResourceKey(
            str(part.get("kind", "lock")),
            str(part.get("process_id", "proc")),
            str(part.get("process_generation", "")),
            str(part.get("name", "")),
        )
    return ResourceKey(
        "lock",
        str(declaration.get("process_id", declaration.get("pid", "proc")) or "proc"),
        str(declaration.get("process_generation", declaration.get("boot_id", "")) or ""),
        str(declaration.get("lock", declaration.get("lock_addr", declaration.get("name", ""))) or ""),
    )


@dataclass
class Hold:
    mode: str
    count: int
    events: List[Event] = field(default_factory=list)
    upgraded_from: int = 0
    confidence: str = "exact"
    thread_ended: bool = False


@dataclass
class Wait:
    id: str
    thread: str
    target_kind: str
    key: ResourceKey
    request_event: Event
    mode: str = "X"
    upgrade: bool = False
    kind: str = "acquire"
    condition_event: Optional[Event] = None
    notify_event: Optional[Event] = None
    saved_hold: Optional[Hold] = None
    active: bool = True
    cancel_event: Optional[Event] = None
    grant_event: Optional[Event] = None
    synthetic: bool = False
    confidence: str = "exact"


@dataclass
class Issue:
    code: str
    message: str
    event_id: str = ""
    severity: str = "warning"
    confidence: str = "uncertain"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "event_id": self.event_id,
            "severity": self.severity,
            "confidence": self.confidence,
        }


@dataclass
class LockInfo:
    declared_type: str = ""
    inferred_type: str = ""
    confidence: str = "exact"

    @property
    def lock_type(self) -> str:
        return self.declared_type or self.inferred_type or "unknown"

    def as_dict(self, key: ResourceKey) -> Dict[str, Any]:
        data = key.as_dict()
        data.update(
            {
                "type": self.lock_type,
                "declared_type": self.declared_type,
                "inferred_type": self.inferred_type,
                "confidence": self.confidence,
            }
        )
        return data


@dataclass
class State:
    holds: Dict[str, Dict[ResourceKey, Hold]] = field(default_factory=dict)
    waits: Dict[str, Wait] = field(default_factory=dict)
    active_waits: Dict[str, List[str]] = field(default_factory=dict)
    thread_status: Dict[str, str] = field(default_factory=dict)
    lock_info: Dict[ResourceKey, LockInfo] = field(default_factory=dict)
    issues: List[Issue] = field(default_factory=list)
    request_for_success: Dict[str, str] = field(default_factory=dict)
    success_for_request: Dict[str, str] = field(default_factory=dict)
    releases_for_success: Dict[str, List[str]] = field(default_factory=dict)
    refs: Dict[str, List[str]] = field(default_factory=dict)
    processed: List[str] = field(default_factory=list)

    def issue(
        self,
        code: str,
        message: str,
        event: Optional[Event] = None,
        severity: str = "warning",
        confidence: str = "uncertain",
    ) -> None:
        self.issues.append(
            Issue(
                code=code,
                message=message,
                event_id=event.id if event else "",
                severity=severity,
                confidence=confidence,
            )
        )

    def lock_info_for(self, key: ResourceKey) -> LockInfo:
        return self.lock_info.setdefault(key, LockInfo())

    def active_wait_list(self, thread: str) -> List[Wait]:
        return [
            self.waits[wait_id]
            for wait_id in self.active_waits.get(thread, [])
            if wait_id in self.waits and self.waits[wait_id].active
        ]

    def find_lock_wait(self, thread: str, key: ResourceKey) -> Optional[Wait]:
        for wait in self.active_wait_list(thread):
            if wait.target_kind == "lock" and wait.key == key:
                return wait
        return None

    def find_condition_wait(self, thread: str, key: Optional[ResourceKey] = None) -> Optional[Wait]:
        for wait in self.active_wait_list(thread):
            if wait.kind == "condition_wait" and (key is None or wait.key == key):
                return wait
        return None

    def add_ref(self, source_id: str, target_id: str) -> None:
        refs = self.refs.setdefault(source_id, [])
        if target_id not in refs:
            refs.append(target_id)


class Reconstructor:
    SEGMENT_SIZE = 16

    def __init__(
        self,
        events: Sequence[Event],
        declarations: Optional[Dict[ResourceKey, str]] = None,
        policy: str = "inferred",
    ):
        self.events = sorted(events, key=lambda event: (event.ts, event.seq, event.id))
        self.declarations = dict(declarations or {})
        self.policy = policy
        self.order_keys = [(event.ts, event.seq, event.id) for event in self.events]
        self.reordered = any(
            self.events[index].seq < self.events[index - 1].seq for index in range(1, len(self.events))
        )
        self._snapshots: Dict[int, State] = {0: self._initial_state()}

    def _initial_state(self) -> State:
        state = State()
        for key, lock_type in self.declarations.items():
            state.lock_info[key] = LockInfo(declared_type=lock_type, confidence="exact")
        return state

    def append(self, new_events: Sequence[Event]) -> Tuple[float, int]:
        if not new_events:
            return 0.0, 0
        earliest = min((event.ts, event.seq, event.id) for event in new_events)
        affected_index = bisect_left(self.order_keys, earliest)
        old_len = len(self.events)
        self.events = sorted(
            self.events + list(new_events), key=lambda event: (event.ts, event.seq, event.id)
        )
        self.order_keys = [(event.ts, event.seq, event.id) for event in self.events]
        self.reordered = any(
            self.events[index].seq < self.events[index - 1].seq for index in range(1, len(self.events))
        )
        bucket = (affected_index // self.SEGMENT_SIZE) * self.SEGMENT_SIZE
        for snapshot_index in list(self._snapshots):
            if snapshot_index >= bucket and snapshot_index != 0:
                del self._snapshots[snapshot_index]
        return earliest[0], max(0, len(self.events) - old_len)

    def index_at_ts(self, ts: Optional[float]) -> int:
        if ts is None:
            return len(self.events)
        return bisect_right(self.order_keys, (float(ts), 10**18, ""))

    def state_at(self, index: Optional[int] = None, ts: Optional[float] = None) -> State:
        if ts is not None:
            index = self.index_at_ts(ts)
        if index is None:
            index = len(self.events)
        index = max(0, min(index, len(self.events)))
        bucket = (index // self.SEGMENT_SIZE) * self.SEGMENT_SIZE
        self._build_snapshot(bucket)
        state = deepcopy(self._snapshots[bucket])
        for event in self.events[bucket:index]:
            self.apply(state, event)
        return state

    def _build_snapshot(self, bucket: int) -> None:
        if bucket in self._snapshots:
            return
        previous = bucket - self.SEGMENT_SIZE
        self._build_snapshot(previous)
        state = deepcopy(self._snapshots[previous])
        for event in self.events[previous:bucket]:
            self.apply(state, event)
        self._snapshots[bucket] = state

    def apply(self, state: State, event: Event) -> None:
        if event.id in state.processed:
            return
        state.processed.append(event.id)
        if event.kind == "thread_start":
            state.thread_status[event.thread] = "running"
        elif event.kind == "thread_end":
            state.thread_status[event.thread] = "ended"
            self._cancel_thread_waits(state, event)
            for hold in state.holds.get(event.thread, {}).values():
                hold.thread_ended = True
                hold.confidence = "uncertain"
            state.issue(
                "thread_end_without_release",
                f"线程 {event.thread} 结束时仍持有锁；缺少 release，保留为不确定持有。",
                event,
                confidence="uncertain",
            )
        elif event.kind == "acquire_request":
            self._acquire_request(state, event)
        elif event.kind == "acquire_success":
            self._acquire_success(state, event)
        elif event.kind == "release":
            self._release(state, event)
        elif event.kind == "condition_wait":
            self._condition_wait(state, event)
        elif event.kind == "condition_notify":
            self._condition_notify(state, event)
        elif event.kind == "wait_timeout":
            self._wait_timeout(state, event)
        else:
            state.issue("unknown_event", f"未知事件类型 {event.kind}。", event, severity="error")

    def _add_wait(self, state: State, wait: Wait) -> None:
        state.waits[wait.id] = wait
        state.active_waits.setdefault(wait.thread, []).append(wait.id)

    def _acquire_request(self, state: State, event: Event) -> None:
        if not event.lock:
            state.issue("missing_lock", "acquire_request 缺少锁地址。", event, severity="error")
            return
        key = event.lock_key
        self._infer_lock_type(state, key, event, event.mode)
        if state.find_lock_wait(event.thread, key):
            state.issue("duplicate_request", "同一线程对同一锁已有活动请求，忽略重复请求。", event)
            return
        hold = state.holds.get(event.thread, {}).get(key)
        if hold and hold.mode == "S" and (event.mode == "X" or event.upgrade):
            wait = Wait(
                id=event.id,
                thread=event.thread,
                target_kind="lock",
                key=key,
                request_event=event,
                mode="X",
                upgrade=True,
                kind="upgrade",
            )
            self._add_wait(state, wait)
            info = state.lock_info_for(key)
            if not info.declared_type:
                info.inferred_type = "rwlock"
                info.confidence = "inferred"
            return
        if hold and event.mode == "S" and hold.mode == "S":
            state.issue(
                "recursive_shared_request",
                "同一线程重复请求共享锁，按读锁计数处理。",
                event,
                confidence="inferred",
            )
        wait = Wait(
            id=event.id,
            thread=event.thread,
            target_kind="lock",
            key=key,
            request_event=event,
            mode=event.mode,
            upgrade=event.upgrade,
            kind="acquire",
        )
        self._add_wait(state, wait)

    def _synthetic_request(self, event: Event) -> Event:
        return Event(
            id=f"synthetic_req_{event.id}",
            ts=event.ts,
            seq=event.seq,
            collection_gen=event.collection_gen,
            process_id=event.process_id,
            process_generation=event.process_generation,
            thread=event.thread,
            kind="acquire_request",
            lock=event.lock,
            condition=event.condition,
            mode=event.mode,
            upgrade=event.upgrade,
            notify_all=event.notify_all,
            stack=event.stack,
            source_event_id=event.id,
            raw={"synthetic": True, "from": event.id},
        )

    def _acquire_success(self, state: State, event: Event) -> None:
        if not event.lock:
            state.issue("missing_lock", "acquire_success 缺少锁地址。", event, severity="error")
            return
        key = event.lock_key
        wait = state.find_lock_wait(event.thread, key)
        synthetic = False
        if wait is None:
            condition_wait = state.find_condition_wait(event.thread)
            if condition_wait and condition_wait.saved_hold is not None:
                wait = condition_wait
            else:
                if self.policy == "strict":
                    state.issue(
                        "orphan_success",
                        "成功事件没有匹配的 acquire_request；严格重建不补请求，也不计入持有。",
                        event,
                        severity="error",
                        confidence="conflict",
                    )
                    return
                request = self._synthetic_request(event)
                wait = Wait(
                    id=request.id,
                    thread=event.thread,
                    target_kind="lock",
                    key=key,
                    request_event=request,
                    mode=event.mode,
                    upgrade=event.upgrade,
                    kind="acquire",
                    synthetic=True,
                    confidence="inferred",
                )
                self._add_wait(state, wait)
                synthetic = True
                state.issue(
                    "inferred_acquire_request",
                    "成功事件缺少 acquire_request；候选重建补入推断请求。",
                    event,
                    confidence="inferred",
                )
        self._grant(state, wait, event, synthetic=synthetic)

    def _grant(self, state: State, wait: Wait, event: Event, synthetic: bool = False) -> None:
        key = wait.key
        wait.active = False
        wait.grant_event = event
        state.add_ref(wait.id, event.id)
        if wait.kind in {"condition_wait", "condition_reacquire"}:
            saved = wait.saved_hold or Hold(mode="X", count=1)
            restored = Hold(
                mode=saved.mode,
                count=saved.count,
                events=list(saved.events) + [event],
                upgraded_from=saved.upgraded_from,
                confidence="exact" if saved.confidence == "exact" else saved.confidence,
            )
            state.holds.setdefault(wait.thread, {})[key] = restored
            state.request_for_success[event.id] = wait.id
            state.success_for_request[wait.id] = event.id
            return
        if wait.upgrade:
            hold = state.holds.setdefault(wait.thread, {}).get(key)
            if hold and hold.mode == "S":
                hold.upgraded_from = hold.count
                hold.mode = "X"
                hold.count = 1
                hold.events.append(event)
                state.request_for_success[event.id] = wait.id
                state.success_for_request[wait.id] = event.id
                return
            state.issue("upgrade_without_shared_hold", "升级成功时没有可见共享持有。", event, confidence="uncertain")
        hold = state.holds.setdefault(wait.thread, {}).get(key)
        if hold is None:
            hold = Hold(mode=wait.mode, count=1, events=[event])
            state.holds.setdefault(wait.thread, {})[key] = hold
        else:
            info = state.lock_info_for(key)
            if info.lock_type in {"reentrant", "unknown"}:
                hold.count += 1
                hold.events.append(event)
            else:
                state.issue(
                    "duplicate_acquire_success",
                    "锁已持有却再次成功；按非重入锁冲突记录。",
                    event,
                    severity="error",
                    confidence="conflict",
                )
                hold.events.append(event)
        state.request_for_success[event.id] = wait.id
        state.success_for_request[wait.id] = event.id
        if synthetic:
            state.add_ref(event.id, wait.id)

    def _release(self, state: State, event: Event) -> None:
        if not event.lock:
            state.issue("missing_lock", "release 缺少锁地址。", event, severity="error")
            return
        key = event.lock_key
        hold = state.holds.get(event.thread, {}).get(key)
        if hold is None:
            state.issue(
                "orphan_release",
                "release 没有匹配持有；不凭空补 acquire。",
                event,
                severity="error",
                confidence="conflict",
            )
            return
        success_event = hold.events[-1] if hold.events else None
        if success_event:
            state.releases_for_success.setdefault(success_event.id, []).append(event.id)
        if hold.mode == "X" and hold.upgraded_from:
            hold.mode = "S"
            hold.count = hold.upgraded_from
            hold.upgraded_from = 0
            hold.events.append(event)
            return
        hold.count -= 1
        hold.events.append(event)
        if hold.count <= 0:
            del state.holds[event.thread][key]
            if not state.holds[event.thread]:
                del state.holds[event.thread]

    def _condition_wait(self, state: State, event: Event) -> None:
        if not event.condition:
            state.issue("missing_condition", "condition_wait 缺少条件变量。", event, severity="error")
            return
        if not event.lock:
            state.issue("missing_lock", "condition_wait 缺少关联锁。", event, severity="error")
            return
        key = event.lock_key
        hold = state.holds.get(event.thread, {}).get(key)
        if hold is None:
            state.issue(
                "condition_wait_without_hold",
                "condition_wait 没有可见锁持有；等待保留，重新竞争状态不确定。",
                event,
                confidence="uncertain",
            )
            saved = Hold(mode="X", count=1, confidence="uncertain")
        else:
            saved = deepcopy(hold)
            del state.holds[event.thread][key]
            if not state.holds[event.thread]:
                del state.holds[event.thread]
        wait = Wait(
            id=event.id,
            thread=event.thread,
            target_kind="condition",
            key=event.condition_key,
            request_event=event,
            kind="condition_wait",
            condition_event=event,
            saved_hold=saved,
        )
        self._add_wait(state, wait)

    def _condition_notify(self, state: State, event: Event) -> None:
        if not event.condition:
            state.issue("missing_condition", "condition_notify 缺少条件变量。", event, severity="error")
            return
        key = event.condition_key
        candidates = [
            wait
            for wait in state.waits.values()
            if wait.active and wait.kind == "condition_wait" and wait.key == key
        ]
        if not candidates:
            state.issue("notify_without_waiter", "notify 没有匹配等待者。", event, severity="info", confidence="exact")
            return
        chosen = candidates if event.notify_all else candidates[:1]
        for wait in chosen:
            wait.kind = "condition_reacquire"
            wait.target_kind = "lock"
            wait.key = wait.condition_event.lock_key if wait.condition_event else wait.key
            wait.mode = wait.saved_hold.mode if wait.saved_hold else "X"
            wait.notify_event = event
            state.add_ref(wait.id, event.id)

    def _wait_timeout(self, state: State, event: Event) -> None:
        wait = None
        if event.condition:
            wait = state.find_condition_wait(event.thread, event.condition_key)
        if wait is None and event.lock:
            wait = state.find_lock_wait(event.thread, event.lock_key)
        if wait is None:
            wait = state.find_condition_wait(event.thread)
        if wait is None:
            state.issue("orphan_timeout", "超时事件没有匹配等待。", event, confidence="uncertain")
            return
        wait.active = False
        wait.cancel_event = event
        state.add_ref(wait.id, event.id)
        if wait.kind in {"condition_wait", "condition_reacquire"} and wait.saved_hold is not None:
            state.holds.setdefault(wait.thread, {})[wait.condition_event.lock_key] = deepcopy(wait.saved_hold)

    def _cancel_thread_waits(self, state: State, event: Event) -> None:
        for wait in state.active_wait_list(event.thread):
            wait.active = False
            wait.cancel_event = event
            state.add_ref(wait.id, event.id)
            if wait.kind in {"condition_wait", "condition_reacquire"} and wait.saved_hold is not None:
                lock_key = wait.condition_event.lock_key if wait.condition_event else wait.key
                state.holds.setdefault(wait.thread, {})[lock_key] = deepcopy(wait.saved_hold)

    def _infer_lock_type(self, state: State, key: ResourceKey, event: Event, mode: str) -> str:
        info = state.lock_info_for(key)
        if info.declared_type:
            return info.declared_type
        if mode == "S" or event.upgrade:
            info.inferred_type = "rwlock"
            info.confidence = "inferred"
        elif not info.inferred_type:
            info.inferred_type = "reentrant"
            info.confidence = "inferred"
        return info.lock_type

def _hold_edge_dict(thread: str, key: ResourceKey, hold: Hold) -> Dict[str, Any]:
    return {
        "id": f"hold:{thread}:{key.label()}",
        "thread": thread,
        "resource": key.label(),
        "mode": hold.mode,
        "count": hold.count,
        "confidence": hold.confidence,
        "thread_ended": hold.thread_ended,
        "event_ids": [event.id for event in hold.events],
        "events": [event.as_dict() for event in hold.events],
        "held_stack": [frame for event in hold.events for frame in event.stack] or ["<no stack captured>"],
    }


def _wait_edge_dict(wait: Wait) -> Dict[str, Any]:
    return {
        "id": f"wait:{wait.thread}:{wait.key.label()}:{wait.id}",
        "thread": wait.thread,
        "resource": wait.key.label(),
        "target_kind": wait.target_kind,
        "mode": wait.mode,
        "kind": wait.kind,
        "upgrade": wait.upgrade,
        "confidence": wait.confidence,
        "synthetic": wait.synthetic,
        "request_event_id": wait.request_event.id,
        "request_event": wait.request_event.as_dict(),
        "notify_event_id": wait.notify_event.id if wait.notify_event else None,
        "wait_stack": list(wait.request_event.stack) or ["<no stack captured>"],
    }


def _blockers_for_wait(state: State, wait: Wait) -> List[Tuple[str, ResourceKey, Hold, str]]:
    blockers: List[Tuple[str, ResourceKey, Hold, str]] = []
    if wait.target_kind == "condition":
        return blockers
    for thread, locks in state.holds.items():
        if thread == wait.thread:
            continue
        hold = locks.get(wait.key)
        if hold is None:
            continue
        if wait.mode == "S" and hold.mode == "S":
            continue
        reason = "exclusive_blocks" if hold.mode == "X" or wait.mode == "X" else "shared_blocks"
        if wait.upgrade:
            reason = "upgrade_blocked_by_shared"
        blockers.append((thread, wait.key, hold, reason))
    return blockers


def build_analysis(state: State, events: Sequence[Event], ts: Optional[float], reordered: bool) -> Dict[str, Any]:
    holds_edges: List[Dict[str, Any]] = []
    wait_edges: List[Dict[str, Any]] = []
    dependency_edges: List[Dict[str, Any]] = []
    threads = sorted(
        set(state.thread_status)
        | set(state.holds)
        | {wait.thread for wait in state.waits.values()}
        | {event.thread for event in events}
    )
    for thread in sorted(state.holds):
        for key, hold in sorted(state.holds[thread].items(), key=lambda item: item[0].label()):
            holds_edges.append(_hold_edge_dict(thread, key, hold))
    active_waits = [wait for wait in state.waits.values() if wait.active]
    for wait in sorted(active_waits, key=lambda item: (item.thread, item.key.label(), item.id)):
        wait_edges.append(_wait_edge_dict(wait))
        for blocker_thread, key, hold, reason in _blockers_for_wait(state, wait):
            dependency_edges.append(
                {
                    "id": f"dep:{wait.thread}:{blocker_thread}:{key.label()}:{wait.id}",
                    "from": wait.thread,
                    "to": blocker_thread,
                    "resource": key.label(),
                    "reason": reason,
                    "confidence": "inferred" if wait.confidence != "exact" or hold.confidence != "exact" else "exact",
                    "wait_event_id": wait.request_event.id,
                    "hold_event_ids": [event.id for event in hold.events],
                    "wait_edge_id": f"wait:{wait.thread}:{wait.key.label()}:{wait.id}",
                    "hold_edge_id": f"hold:{blocker_thread}:{key.label()}",
                }
            )
    cycles = _find_cycles(dependency_edges, state)
    return {
        "ts": ts,
        "event_count": len(events),
        "reordered": reordered,
        "threads": [
            {
                "id": thread,
                "status": state.thread_status.get(thread, "active"),
                "holds": [key.label() for key in state.holds.get(thread, {})],
                "waits": [wait.key.label() for wait in active_waits if wait.thread == thread],
            }
            for thread in threads
        ],
        "graphs": {
            "holds": holds_edges,
            "waits": wait_edges,
            "dependencies": dependency_edges,
        },
        "cycles": cycles,
        "locks": [
            state.lock_info[key].as_dict(key)
            for key in sorted(state.lock_info, key=lambda item: item.label())
        ],
        "issues": [issue.as_dict() for issue in state.issues],
    }


def _find_cycles(edges: List[Dict[str, Any]], state: State) -> List[Dict[str, Any]]:
    adjacency: Dict[str, List[Dict[str, Any]]] = {}
    for edge in edges:
        adjacency.setdefault(edge["from"], []).append(edge)
    cycles: List[List[Dict[str, Any]]] = []
    seen_signatures = set()

    def canonical(edge_cycle: List[Dict[str, Any]]) -> Tuple[str, ...]:
        ids = [edge["id"] for edge in edge_cycle]
        rotations = [tuple(ids[index:] + ids[:index]) for index in range(len(ids))]
        return min(rotations)

    def dfs(start: str, node: str, path: List[Dict[str, Any]], visited: set) -> None:
        for edge in adjacency.get(node, []):
            target = edge["to"]
            if target == start and path:
                signature = canonical(path + [edge])
                if signature not in seen_signatures:
                    seen_signatures.add(signature)
                    cycles.append(path + [edge])
            elif target not in visited and len(path) < 16:
                visited.add(target)
                dfs(start, target, path + [edge], visited)
                visited.remove(target)

    for node in sorted(adjacency):
        dfs(node, node, [], {node})
    return [_cycle_dict(index, cycle, state) for index, cycle in enumerate(cycles, start=1)]


def _cycle_dict(number: int, edges: List[Dict[str, Any]], state: State) -> Dict[str, Any]:
    participants: List[Dict[str, Any]] = []
    evidence_event_ids: List[str] = []
    for edge in edges:
        waiter = edge["from"]
        blocker = edge["to"]
        wait = state.waits.get(edge["wait_event_id"])
        hold = state.holds.get(blocker, {}).get(
            next((key for key in state.holds.get(blocker, {}) if key.label() == edge["resource"]), None)
        )
        held_stack = []
        hold_event_ids = []
        if hold:
            held_stack = [frame for event in hold.events for frame in event.stack]
            hold_event_ids = [event.id for event in hold.events]
        wait_stack = list(wait.request_event.stack) if wait else []
        participants.append(
            {
                "thread": waiter,
                "waiting_for": edge["resource"],
                "blocked_by": blocker,
                "held_stack": held_stack or ["<no stack captured>"],
                "wait_stack": wait_stack or ["<no stack captured>"],
                "wait_event_id": edge["wait_event_id"],
                "hold_event_ids": hold_event_ids,
                "dependency_edge_id": edge["id"],
            }
        )
        evidence_event_ids.append(edge["wait_event_id"])
        evidence_event_ids.extend(hold_event_ids)
    material = "|".join(edge["id"] for edge in edges)
    return {
        "id": "cycle_" + sha1(material.encode()).hexdigest()[:10],
        "number": number,
        "length": len(edges),
        "threads": [edge["from"] for edge in edges],
        "locks": [edge["resource"] for edge in edges],
        "edges": edges,
        "participants": participants,
        "evidence_event_ids": list(dict.fromkeys(evidence_event_ids)),
    }


def _cycle_signature(cycle: Dict[str, Any]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    return tuple(cycle["threads"]), tuple(cycle["locks"])


def _closure_ids(state: State, seed_ids: List[str]) -> List[str]:
    result = set(seed_ids)
    queue = list(seed_ids)
    while queue:
        current = queue.pop(0)
        related = set(state.refs.get(current, []))
        if current in state.request_for_success:
            related.add(state.request_for_success[current])
        if current in state.success_for_request:
            related.add(state.success_for_request[current])
        related.update(state.releases_for_success.get(current, []))
        for event_id in related:
            if event_id not in result:
                result.add(event_id)
                queue.append(event_id)
    return sorted(result)


def minimize_cycle(
    events: Sequence[Event],
    declarations: Optional[Dict[ResourceKey, str]],
    cycle_id: str,
    policy: str = "inferred",
) -> Dict[str, Any]:
    recon = Reconstructor(events, declarations, policy=policy)
    state = recon.state_at()
    analysis = build_analysis(state, recon.events, None, recon.reordered)
    target = next((cycle for cycle in analysis["cycles"] if cycle["id"] == cycle_id), None)
    if target is None:
        raise KeyError(f"cycle not found: {cycle_id}")
    signature = _cycle_signature(target)
    seed_ids = list(target["evidence_event_ids"])
    keep_ids = set(_closure_ids(state, seed_ids))
    by_id = {event.id: event for event in recon.events}

    def reproduces(candidate_events: List[Event]) -> Optional[Dict[str, Any]]:
        trial = Reconstructor(candidate_events, declarations, policy=policy)
        trial_state = trial.state_at()
        trial_analysis = build_analysis(trial_state, trial.events, None, trial.reordered)
        for cycle in trial_analysis["cycles"]:
            if _cycle_signature(cycle) == signature:
                return cycle
        return None

    current_events = [event for event in recon.events if event.id in keep_ids]
    current_cycle = reproduces(current_events)
    if current_cycle is None:
        current_events = list(recon.events)
        current_cycle = target

    def pair_group(event_id: str) -> set:
        group = {event_id}
        group.update(state.refs.get(event_id, []))
        if event_id in state.request_for_success:
            group.add(state.request_for_success[event_id])
        if event_id in state.success_for_request:
            group.add(state.success_for_request[event_id])
        group.update(state.releases_for_success.get(event_id, []))
        return group

    changed = True
    while changed:
        changed = False
        for event in list(current_events):
            group = pair_group(event.id) & {item.id for item in current_events}
            if group & set(seed_ids):
                continue
            trial_events = [item for item in current_events if item.id not in group]
            cycle = reproduces(trial_events)
            if cycle is not None:
                current_events = trial_events
                current_cycle = cycle
                changed = True
                break
    return {
        "cycle_id": cycle_id,
        "original_event_count": len(events),
        "reduced_event_count": len(current_events),
        "event_ids": [event.id for event in current_events],
        "events": [event.as_dict() for event in current_events],
        "cycle": current_cycle,
    }
