"""锁序解剖室核心引擎：事件重放、持有图/等待图、置信状态、环检测。"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Dict, List, Optional

CERTAIN = "certain"
UNCERTAIN = "uncertain"

MUTEX = "mutex"
RECURSIVE = "recursive"
RWLOCK = "rwlock"

EVENT_TYPES = (
    "thread_start", "thread_end",
    "acquire_request", "acquire_success", "release",
    "cond_wait_start", "cond_wait_end", "notify", "notify_all",
    "timeout",
)


def lock_key(generation: int, address: str) -> str:
    """同一地址在不同采集代次属于不同的锁。"""
    return "%s:%s" % (generation, address)


def conflicts(wait_mode: str, hold_mode: str) -> bool:
    return not (wait_mode == "shared" and hold_mode == "shared")


@dataclass
class Hold:
    lock: str
    mode: str
    depth: int
    acquired_event: int
    request_event: Optional[int]
    confidence: str = CERTAIN
    leaked: bool = False
    reacquired_event: Optional[int] = None


@dataclass
class Wait:
    lock: str
    mode: str
    kind: str  # acquire | upgrade | reacquire | cond
    request_event: int
    since: float
    confidence: str = CERTAIN
    cond: Optional[str] = None


@dataclass
class ThreadState:
    alive: bool = False
    holds: List[Hold] = field(default_factory=list)
    wait: Optional[Wait] = None
    cond_hold: Optional[Hold] = None


class State:
    def __init__(self) -> None:
        self.threads: Dict[str, ThreadState] = {}
        self.anomalies: List[dict] = []
        self.last_ts: Dict[str, float] = {}

    def thread(self, tid: str) -> ThreadState:
        if tid not in self.threads:
            self.threads[tid] = ThreadState()
        return self.threads[tid]

    def anomaly(self, event: dict, message: str) -> None:
        self.anomalies.append({
            "event_id": event.get("id"),
            "thread": event.get("thread"),
            "type": event.get("type"),
            "message": message,
        })

    def edges(self) -> List[dict]:
        """等待图边：等待者 -> 持有者，附产生它的事件与置信状态。"""
        out = []
        for tid, ts in self.threads.items():
            w = ts.wait
            if w is None or w.kind == "cond":
                continue
            for otid, ots in self.threads.items():
                if otid == tid:
                    continue
                for h in ots.holds:
                    if h.lock == w.lock and conflicts(w.mode, h.mode):
                        confidence = CERTAIN
                        if w.confidence == UNCERTAIN or h.confidence == UNCERTAIN:
                            confidence = UNCERTAIN
                        out.append({
                            "from": tid, "to": otid, "lock": w.lock,
                            "wait_kind": w.kind,
                            "wait_event": w.request_event,
                            "hold_event": h.acquired_event,
                            "confidence": confidence,
                        })
        return out

    def held_graph(self) -> List[dict]:
        out = []
        for tid, ts in self.threads.items():
            for h in ts.holds:
                out.append({
                    "thread": tid, "lock": h.lock, "mode": h.mode,
                    "depth": h.depth, "acquired_event": h.acquired_event,
                    "confidence": h.confidence, "leaked": h.leaked,
                })
        return out

    def waits(self) -> List[dict]:
        out = []
        for tid, ts in self.threads.items():
            if ts.wait is not None:
                w = ts.wait
                out.append({
                    "thread": tid, "lock": w.lock, "mode": w.mode,
                    "kind": w.kind, "request_event": w.request_event,
                    "since": w.since, "confidence": w.confidence,
                })
        return out


def _lock_type(lock_types: dict, key: str) -> str:
    return lock_types.get(key, MUTEX)


def apply_event(state: State, event: dict, lock_types: dict) -> None:
    etype = event["type"]
    tid = event.get("thread") or ""
    gen = event.get("generation", 0)
    key = lock_key(gen, event["lock"]) if event.get("lock") else None
    extra = event.get("extra") or {}
    ts = event.get("ts", 0.0)
    ts_state = state.thread(tid) if tid else None

    # 乱序到达：时间戳倒挂时不丢弃事件，降级其产生状态的置信度。
    out_of_order = False
    if tid:
        last = state.last_ts.get(tid)
        if last is not None and ts < last:
            out_of_order = True
            state.anomaly(event, "out-of-order arrival")
        state.last_ts[tid] = max(last if last is not None else ts, ts)

    if etype == "thread_start":
        ts_state.alive = True
        return
    if etype == "thread_end":
        # 线程终止撤销等待；持有的锁不凭空补 release，降级为不确定。
        if ts_state.wait is not None:
            ts_state.wait = None
        for h in ts_state.holds:
            h.leaked = True
            h.confidence = UNCERTAIN
        ts_state.alive = False
        return

    if etype == "acquire_request":
        mode = extra.get("mode", "exclusive")
        kind = "acquire"
        if any(h.lock == key and h.mode == "shared" for h in ts_state.holds) and mode == "exclusive":
            kind = "upgrade"
        ts_state.wait = Wait(lock=key, mode=mode, kind=kind,
                             request_event=event["id"], since=ts,
                             confidence=UNCERTAIN if out_of_order else CERTAIN)
        return

    if etype == "acquire_success":
        mode = extra.get("mode", "exclusive")
        ltype = _lock_type(lock_types, key)
        confidence = CERTAIN
        request_event = None
        if ts_state.wait is not None and ts_state.wait.lock == key:
            request_event = ts_state.wait.request_event
            mode = ts_state.wait.mode
            confidence = ts_state.wait.confidence
            ts_state.wait = None
        else:
            # 缺少对应的 acquire_request：不补请求，只降级置信。
            confidence = UNCERTAIN
            state.anomaly(event, "acquire_success without matching request")
        if out_of_order:
            confidence = UNCERTAIN
        for h in ts_state.holds:
            if h.lock == key:
                if ltype == RECURSIVE and h.mode == "exclusive" and mode == "exclusive":
                    h.depth += 1
                    return
                if ltype == RWLOCK and h.mode == "shared" and mode == "exclusive":
                    h.mode = "exclusive"
                    h.acquired_event = event["id"]
                    return
                if ltype == RWLOCK and h.mode == "shared" and mode == "shared":
                    h.depth += 1
                    return
                state.anomaly(event, "duplicate acquire on non-recursive lock")
                ts_state.holds.append(Hold(key, mode, 1, event["id"], request_event, UNCERTAIN))
                return
        ts_state.holds.append(Hold(key, mode, 1, event["id"], request_event, confidence))
        return

    if etype == "release":
        for i, h in enumerate(ts_state.holds):
            if h.lock == key:
                if h.depth > 1:
                    h.depth -= 1
                else:
                    del ts_state.holds[i]
                return
        # 缺事件的释放：记录异常，不凭空补任何状态。
        state.anomaly(event, "release without matching hold")
        return

    if etype == "cond_wait_start":
        cond = lock_key(gen, extra.get("cond", event.get("lock") or ""))
        hold = None
        for i, h in enumerate(ts_state.holds):
            if h.lock == key:
                hold = ts_state.holds.pop(i)
                break
        if hold is None:
            state.anomaly(event, "cond_wait_start without holding the mutex")
            hold = Hold(key, "exclusive", 1, event["id"], None, UNCERTAIN)
        ts_state.cond_hold = hold
        ts_state.wait = Wait(lock=cond, mode="exclusive", kind="cond",
                             request_event=event["id"], since=ts, cond=key)
        return

    if etype in ("notify", "notify_all"):
        cond = lock_key(gen, extra.get("cond", event.get("lock") or ""))
        woke = False
        for otid, ots in state.threads.items():
            w = ots.wait
            if w is not None and w.kind == "cond" and w.lock == cond:
                ots.wait = Wait(lock=w.cond, mode="exclusive", kind="reacquire",
                                request_event=w.request_event, since=ts)
                woke = True
                if etype == "notify":
                    break
        if not woke:
            state.anomaly(event, "notify with no waiter")
        return

    if etype == "cond_wait_end":
        hold = ts_state.cond_hold
        ts_state.cond_hold = None
        if ts_state.wait is not None and ts_state.wait.kind in ("cond", "reacquire"):
            ts_state.wait = None
        if hold is None:
            state.anomaly(event, "cond_wait_end without cond_wait_start")
            hold = Hold(key, "exclusive", 1, event["id"], None, UNCERTAIN)
        hold.reacquired_event = event["id"]
        ts_state.holds.append(hold)
        return

    if etype == "timeout":
        # 超时撤销等待；cond 场景下恢复持有语义由 cond_wait_end 处理。
        if ts_state.wait is not None:
            if ts_state.wait.kind == "reacquire" and ts_state.cond_hold is not None:
                ts_state.holds.append(ts_state.cond_hold)
                ts_state.cond_hold = None
            ts_state.wait = None
        else:
            state.anomaly(event, "timeout without pending wait")
        return


def find_cycles(edges: List[dict]) -> List[List[str]]:
    adj: Dict[str, List[str]] = {}
    for e in edges:
        adj.setdefault(e["from"], [])
        if e["to"] not in adj[e["from"]]:
            adj[e["from"]].append(e["to"])
    cycles: List[List[str]] = []
    seen = set()

    def dfs(start: str, node: str, path: List[str], visited: set) -> None:
        for nxt in adj.get(node, ()):
            if nxt == start and len(path) >= 2:
                canon = tuple(sorted(path))
                if canon not in seen:
                    seen.add(canon)
                    cycles.append(list(path))
            elif nxt not in visited and nxt >= start:
                visited.add(nxt)
                path.append(nxt)
                dfs(start, nxt, path, visited)
                path.pop()
                visited.discard(nxt)

    for node in sorted(adj):
        dfs(node, node, [node], {node})
    return cycles


def cycle_evidence(state: State, cycle: List[str]) -> dict:
    """环证据：具体线程、锁、持有栈与等待栈，附事件 id。"""
    n = len(cycle)
    edge_infos = []
    for i in range(n):
        waiter = cycle[i]
        holder = cycle[(i + 1) % n]
        w = state.threads[waiter].wait
        hold = None
        for h in state.threads[holder].holds:
            if w is not None and h.lock == w.lock:
                hold = h
                break
        edge_infos.append({
            "waiter": waiter,
            "holder": holder,
            "lock": w.lock if w else None,
            "wait_event": w.request_event if w else None,
            "hold_event": hold.acquired_event if hold else None,
            "wait_stack": [{
                "kind": w.kind, "lock": w.lock, "mode": w.mode,
                "event": w.request_event, "since": w.since,
                "confidence": w.confidence,
            }] if w else [],
            "held_stack": [{
                "lock": h.lock, "mode": h.mode, "depth": h.depth,
                "event": h.acquired_event, "confidence": h.confidence,
                "leaked": h.leaked,
            } for h in state.threads[holder].holds],
        })
    return {"threads": list(cycle), "locks": [e["lock"] for e in edge_infos],
            "edges": edge_infos}


def cycle_signature(state: State, cycle: List[str]) -> tuple:
    n = len(cycle)
    sig = []
    for i in range(n):
        waiter = cycle[i]
        holder = cycle[(i + 1) % n]
        w = state.threads[waiter].wait
        sig.append((waiter, w.lock if w else None, holder))
    return tuple(sorted(sig))


class Engine:
    """按采集代次与原始顺序重放事件，支持快照增量重算。"""

    SNAP_EVERY = 16

    def __init__(self, events: List[dict], lock_types: Optional[dict] = None):
        self.events = sorted(events, key=lambda e: (e["generation"], e["seq"]))
        self.lock_types = lock_types or {}
        self.replay_count = 0
        self._snapshots: List[dict] = []  # {index, max_ts, state}
        self._built_upto = 0
        self._built_state = State()
        self._built_max_ts = float("-inf")

    def reload(self, events: List[dict], lock_types: Optional[dict] = None) -> None:
        """流式追加/校正后重载：仅作废受影响下标之后的快照。"""
        events = sorted(events, key=lambda e: (e["generation"], e["seq"]))
        first_changed = len(events)
        old_ids = [e["id"] for e in self.events]
        new_ids = [e["id"] for e in events]
        common = 0
        for a, b in zip(old_ids, new_ids):
            if a != b:
                break
            common += 1
        if len(old_ids) != len(new_ids) and common == min(len(old_ids), len(new_ids)):
            first_changed = common
        elif common < min(len(old_ids), len(new_ids)):
            first_changed = common
        self.events = events
        if lock_types is not None:
            self.lock_types = lock_types
        self._invalidate_from(first_changed)

    def _invalidate_from(self, index: int) -> None:
        self._snapshots = [s for s in self._snapshots if s["index"] <= index]
        if self._built_upto > index:
            if self._snapshots:
                last = self._snapshots[-1]
                self._built_upto = last["index"]
                self._built_state = copy.deepcopy(last["state"])
                self._built_max_ts = last["max_ts"]
            else:
                self._built_upto = 0
                self._built_state = State()
                self._built_max_ts = float("-inf")

    def _ensure_built(self, end: int) -> None:
        while self._built_upto < end:
            e = self.events[self._built_upto]
            apply_event(self._built_state, e, self.lock_types)
            self.replay_count += 1
            self._built_upto += 1
            if e.get("ts", 0.0) > self._built_max_ts:
                self._built_max_ts = e.get("ts", 0.0)
            if self._built_upto % self.SNAP_EVERY == 0:
                self._snapshots.append({
                    "index": self._built_upto,
                    "max_ts": self._built_max_ts,
                    "state": copy.deepcopy(self._built_state),
                })

    def state_at(self, t: float) -> State:
        max_ts = max((e.get("ts", 0.0) for e in self.events), default=float("-inf"))
        if t >= max_ts:
            self._ensure_built(len(self.events))
            return copy.deepcopy(self._built_state)
        best = None
        for s in self._snapshots:
            if s["max_ts"] <= t and (best is None or s["index"] > best["index"]):
                best = s
        if best is not None:
            state = copy.deepcopy(best["state"])
            start = best["index"]
        else:
            state = State()
            start = 0
        for i in range(start, len(self.events)):
            e = self.events[i]
            if e.get("ts", 0.0) <= t:
                apply_event(state, e, self.lock_types)
                self.replay_count += 1
        return state

    def cycles_at(self, t: float) -> List[dict]:
        state = self.state_at(t)
        edges = state.edges()
        cycles = find_cycles(edges)
        return [dict(cycle_evidence(state, c), signature=cycle_signature(state, c))
                for c in cycles]
