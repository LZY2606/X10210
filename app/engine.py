"""事件回放引擎：持有图、等待图、异常候选与死锁环检测。

核心约束
--------
* 每个事件处理完产出一个不可变快照；流式追加只重算受影响后缀段。
* 缺事件不凭空补 release；所有推断帧都带 ``low`` 置信度。
* 相同锁地址按 ``(gen, address)`` 区分代次。
* ``repair_choices`` 指定每个异常采用的修复，形成一条候选重建分支。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .models import (
    ANOM_CONDWAIT_WITHOUT_HOLD,
    ANOM_GRANT_WITHOUT_REQUEST,
    ANOM_NOTIFY_NO_WAITER,
    ANOM_RELEASE_WITHOUT_HOLD,
    ANOM_THREAD_ENDED,
    EV_ACQ_GRANTED,
    EV_ACQ_REQUEST,
    EV_ACQ_TIMEOUT,
    EV_COND_NOTIFY,
    EV_COND_WAIT,
    EV_RELEASE,
    EV_THREAD_END,
    EV_THREAD_START,
    MODE_S,
    MODE_X,
    REPAIR_DROP_GRANT,
    REPAIR_INFERR_ACQUIRE,
    REPAIR_LITERAL,
    REPAIR_REJECT_EVENT,
    Anomaly,
    Event,
    Frame,
    HoldEdge,
    Wait,
    WaitEdge,
)

# 字面执行是缺省策略：记录异常但不发明释放，保留候选事实。
PRIMARY_CHOICES: Dict[str, str] = {
    ANOM_RELEASE_WITHOUT_HOLD: REPAIR_LITERAL,
    ANOM_GRANT_WITHOUT_REQUEST: REPAIR_INFERR_ACQUIRE,
    ANOM_CONDWAIT_WITHOUT_HOLD: REPAIR_LITERAL,
    ANOM_NOTIFY_NO_WAITER: REPAIR_LITERAL,
    ANOM_THREAD_ENDED: REPAIR_REJECT_EVENT,
}


class EngineError(Exception):
    pass


@dataclass
class CondWaiter:
    """condition 队列上的一次等待，底层也是一次锁的重新竞争。"""

    cond: str
    wait: Wait


@dataclass
class ThreadState:
    name: str = ""
    alive: bool = False
    started: bool = False
    stack: List[Frame] = field(default_factory=list)
    waits: List[Wait] = field(default_factory=list)
    cond_waits: List[CondWaiter] = field(default_factory=list)

    def active_wait(self, lock: Optional[str] = None) -> Optional[Wait]:
        for wait in reversed(self.waits):
            if wait.status != "waiting":
                continue
            if lock is None or wait.lock == lock:
                return wait
        return None

    def held_modes(self, lock: str) -> List[Frame]:
        return [frame for frame in self.stack if frame.lock == lock]

    def clone(self) -> "ThreadState":
        state = ThreadState(name=self.name, alive=self.alive, started=self.started)
        state.stack = [frame.clone() for frame in self.stack]
        state.waits = [wait.clone() for wait in self.waits]
        state.cond_waits = [
            CondWaiter(cond=cw.cond, wait=cw.wait.clone()) for cw in self.cond_waits
        ]
        return state


@dataclass
class LockMeta:
    kind: str = "mutex"
    declared: bool = False
    confidence: str = "confirmed"


def anomaly_key(code: str, event_id: Optional[int]) -> str:
    return f"{code}:{event_id}"


class Replay:
    """按时间序回放一个分支上的事件流。"""

    def __init__(
        self,
        events: Sequence[Event],
        lock_types: Optional[Dict[str, str]] = None,
        repair_choices: Optional[Dict[str, str]] = None,
    ) -> None:
        self.events = list(events)
        self.lock_types: Dict[str, LockMeta] = {}
        for uid, kind in (lock_types or {}).items():
            self.lock_types[uid] = LockMeta(kind=kind, declared=True)
        self.repair_choices = dict(repair_choices or {})
        self.threads: Dict[str, ThreadState] = {}
        self.anomalies: List[Anomaly] = []
        self._anom_seen: set = set()
        self.snapshots: List[Dict[str, Any]] = []
        self.processed: List[bool] = []
        self.event_index: List[int] = []

    # ------------------------------------------------------------------
    def tstate(self, name: str) -> ThreadState:
        state = self.threads.get(name)
        if state is None:
            state = ThreadState(name=name)
            self.threads[name] = state
        return state

    def lock_kind_for(self, uid: str, mode: Optional[str]) -> str:
        meta = self.lock_types.get(uid)
        if meta is None:
            kind = "rwlock" if mode == MODE_S else "mutex"
            meta = LockMeta(kind=kind, declared=False, confidence="low")
            self.lock_types[uid] = meta
        return meta.kind

    def choice(self, code: str, event_id: Optional[int]) -> str:
        return self.repair_choices.get(
            anomaly_key(code, event_id), PRIMARY_CHOICES[code]
        )

    def add_anomaly(self, anomaly: Anomaly) -> None:
        if anomaly.key in self._anom_seen:
            return
        self._anom_seen.add(anomaly.key)
        self.anomalies.append(anomaly)

    def lock_holders(self, uid: str) -> List[Tuple[str, Frame]]:
        return [
            (name, frame)
            for name, state in self.threads.items()
            for frame in state.stack
            if frame.lock == uid
        ]

    def compute_blockers(
        self, waiter_name: str, uid: str, mode: str
    ) -> List[str]:
        blockers: List[str] = []
        for name, frame in self.lock_holders(uid):
            if name == waiter_name:
                continue
            if mode == MODE_S and frame.mode == MODE_S:
                continue
            blockers.append(name)
        kind_meta = self.lock_types.get(uid)
        if kind_meta is not None and kind_meta.kind == "rwlock":
            for name, state in self.threads.items():
                if name == waiter_name:
                    continue
                wait = state.active_wait(uid)
                if wait and wait.mode == MODE_X and name not in blockers:
                    blockers.append(name)
        return blockers

    # ---- 事件处理 ------------------------------------------------------
    def run(self) -> None:
        ordered = order_events(self.events)
        for event in ordered:
            before = len(self.snapshots)
            self.process_event(event)
            # 每个原始事件至少占一个快照位（被拒绝/乱序也可回溯）
            if len(self.snapshots) == before:
                self.snapshots.append(self.snapshot(event, processed=False))
                self.event_index.append(event.raw_id)  # type: ignore[arg-type]

    def process_event(self, event: Event) -> None:
        if event.deleted:
            self.snapshots.append(self.snapshot(event, processed=False))
            self.event_index.append(event.raw_id)  # type: ignore[arg-type]
            self.processed.append(False)
            return
        handler = {
            EV_THREAD_START: self.on_thread_start,
            EV_THREAD_END: self.on_thread_end,
            EV_ACQ_REQUEST: self.on_acq_request,
            EV_ACQ_GRANTED: self.on_acq_granted,
            EV_RELEASE: self.on_release,
            EV_COND_WAIT: self.on_cond_wait,
            EV_COND_NOTIFY: self.on_cond_notify,
            EV_ACQ_TIMEOUT: self.on_acq_timeout,
        }.get(event.kind)
        if handler is None:
            raise EngineError(f"unknown event kind: {event.kind}")
        accepted = handler(event)
        self.processed.append(bool(accepted))
        self.snapshots.append(self.snapshot(event, processed=bool(accepted)))
        self.event_index.append(event.raw_id)  # type: ignore[arg-type]

    def _check_alive(self, event: Event) -> bool:
        state = self.tstate(event.thread)
        if state.started and not state.alive:
            self.add_anomaly(
                Anomaly(
                    code=ANOM_THREAD_ENDED,
                    event_id=event.raw_id,
                    thread=event.thread,
                    lock=event.lock_uid,
                    message=f"事件到达时线程 {event.thread} 已终止",
                    confidence="low",
                )
            )
            if self.choice(ANOM_THREAD_ENDED, event.raw_id) == REPAIR_REJECT_EVENT:
                return False
        return True

    def on_thread_start(self, event: Event) -> bool:
        state = self.tstate(event.thread)
        state.alive = True
        state.started = True
        return True

    def on_thread_end(self, event: Event) -> bool:
        state = self.tstate(event.thread)
        # 线程终止撤销所有在途等待（超时之外的自然取消）。
        for wait in state.waits:
            if wait.status == "waiting":
                wait.status = "thread_exit"
                wait.end_event_id = event.raw_id
        state.cond_waits = [
            cw for cw in state.cond_waits if cw.wait.status == "waiting"
        ]
        state.alive = False
        # 注意：不凭空补 release。仍持有锁的帧保持 stale，交候选重建决策。
        for frame in state.stack:
            if frame.confidence == "confirmed":
                frame.confidence = "stale"
        return True

    def on_acq_request(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        state = self.tstate(event.thread)
        uid = event.lock_uid or ""
        mode = event.mode or MODE_X
        self.lock_kind_for(uid, mode)

        held = state.held_modes(uid)
        if held:
            # 可重入：同线程重复请求同一把锁。
            kind = self.lock_types.get(uid)
            if kind is not None and kind.kind == "rwlock" and mode == MODE_X:
                if any(frame.mode == MODE_S for frame in held) and not any(
                    frame.mode == MODE_X for frame in held
                ):
                    # 持有 S 请求 X => 升级，需要等待其他 S 持有者离开。
                    blockers = self.compute_blockers(event.thread, uid, MODE_X)
                    if blockers:
                        self._begin_wait(event, uid, MODE_X, "upgrade", blockers)
                        return True
            frame = Frame(
                lock=uid,
                mode=mode if not any(f.mode == MODE_X for f in held) else MODE_X,
                acquire_event_id=event.raw_id,  # type: ignore[arg-type]
                acquire_ts=event.ts,
                acquire_stack=list(event.stack),
                reason="reentrant",
                reentrant=True,
            )
            state.stack.append(frame)
            return True

        blockers = self.compute_blockers(event.thread, uid, mode)
        if blockers:
            self._begin_wait(event, uid, mode, "acquire", blockers, event.timeout)
        else:
            state.stack.append(
                Frame(
                    lock=uid,
                    mode=mode,
                    acquire_event_id=event.raw_id,  # type: ignore[arg-type]
                    acquire_ts=event.ts,
                    acquire_stack=list(event.stack),
                )
            )
        return True

    def _begin_wait(
        self,
        event: Event,
        uid: str,
        mode: str,
        reason: str,
        blockers: List[str],
        timeout: Optional[int] = None,
        saved_frame: Optional[Frame] = None,
        cond_wait_event_id: Optional[int] = None,
    ) -> Wait:
        wait = Wait(
            event_id=event.raw_id,  # type: ignore[arg-type]
            ts=event.ts,
            lock=uid,
            mode=mode,
            stack=list(event.stack),
            reason=reason,
            timeout=timeout,
            deadline=(event.ts + timeout) if timeout is not None else None,
            cond_wait_event_id=cond_wait_event_id,
            saved_frame=saved_frame,
        )
        state = self.tstate(event.thread)
        state.waits.append(wait)
        return wait

    def on_acq_granted(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        state = self.tstate(event.thread)
        uid = event.lock_uid or ""
        mode = event.mode or MODE_X
        self.lock_kind_for(uid, mode)
        wait = state.active_wait(uid)
        if wait is None:
            anomaly = Anomaly(
                code=ANOM_GRANT_WITHOUT_REQUEST,
                event_id=event.raw_id,
                thread=event.thread,
                lock=uid,
                message=f"线程 {event.thread} 收到锁 {event.lock} 的授予但无在途请求",
                confidence="low",
            )
            self.add_anomaly(anomaly)
            repair = self.choice(ANOM_GRANT_WITHOUT_REQUEST, event.raw_id)
            if repair == REPAIR_DROP_GRANT:
                return False
            # 推断：存在一条缺失的 acquire，补一帧但保持 low 置信度。
            state.stack.append(
                Frame(
                    lock=uid,
                    mode=mode,
                    acquire_event_id=event.raw_id,  # type: ignore[arg-type]
                    acquire_ts=event.ts,
                    acquire_stack=list(event.stack),
                    reason="inferred",
                    confidence="low",
                )
            )
            return True

        wait.status = "granted"
        wait.end_event_id = event.raw_id
        # 升级场景：把原有 S 帧提升为 X（同一帧，保留证据链）。
        if wait.reason == "upgrade":
            for frame in reversed(state.stack):
                if frame.lock == uid and frame.mode == MODE_S:
                    frame.mode = MODE_X
                    frame.acquire_event_id = event.raw_id  # type: ignore[assignment]
                    frame.acquire_ts = event.ts
                    frame.acquire_stack = list(event.stack)
                    break
            return True

        new_frame: Optional[Frame] = None
        if wait.saved_frame is not None:
            # condition 唤醒后重新拿到锁：恢复原持有帧。
            new_frame = wait.saved_frame.clone()
            new_frame.reason = "cond_reacquire"
            new_frame.acquire_event_id = event.raw_id  # type: ignore[assignment]
            new_frame.acquire_ts = event.ts
            new_frame.acquire_stack = list(event.stack)
        else:
            new_frame = Frame(
                lock=uid,
                mode=wait.mode,
                acquire_event_id=event.raw_id,  # type: ignore[arg-type]
                acquire_ts=event.ts,
                acquire_stack=list(event.stack or wait.stack),
            )
        state.stack.append(new_frame)
        # 关闭对应的 condition 等待登记。
        state.cond_waits = [
            cw for cw in state.cond_waits if cw.wait is not wait
        ]
        return True

    def on_release(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        state = self.tstate(event.thread)
        uid = event.lock_uid or ""
        for index in range(len(state.stack) - 1, -1, -1):
            if state.stack[index].lock == uid:
                del state.stack[index]
                return True
        anomaly = Anomaly(
            code=ANOM_RELEASE_WITHOUT_HOLD,
            event_id=event.raw_id,
            thread=event.thread,
            lock=uid,
            message=f"线程 {event.thread} 释放未持有的锁 {event.lock}（可能缺 acquire）",
            confidence="low",
        )
        self.add_anomaly(anomaly)
        repair = self.choice(ANOM_RELEASE_WITHOUT_HOLD, event.raw_id)
        if repair == REPAIR_REJECT_EVENT:
            return False
        if repair == REPAIR_INFERR_ACQUIRE:
            # 候选重建：推断一个缺失 acquire 后再释放（净持有不变，仅修正配对）。
            return True
        # literal：保留异常事实，状态不变，不发明 release。
        return True

    def on_cond_wait(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        state = self.tstate(event.thread)
        uid = event.lock_uid or ""
        saved: Optional[Frame] = None
        for index in range(len(state.stack) - 1, -1, -1):
            if state.stack[index].lock == uid:
                saved = state.stack.pop(index)
                break
        if saved is None:
            anomaly = Anomaly(
                code=ANOM_CONDWAIT_WITHOUT_HOLD,
                event_id=event.raw_id,
                thread=event.thread,
                lock=uid,
                message=f"线程 {event.thread} 在未持有锁 {event.lock} 时进入 condition 等待",
                confidence="low",
            )
            self.add_anomaly(anomaly)
            if self.choice(ANOM_CONDWAIT_WITHOUT_HOLD, event.raw_id) == REPAIR_REJECT_EVENT:
                return False
        # 重新竞争：与普通等待一样进入等待图，保留持有帧以便唤醒恢复。
        blockers = self.compute_blockers(event.thread, uid, saved.mode if saved else MODE_X)
        wait = self._begin_wait(
            event,
            uid,
            saved.mode if saved else MODE_X,
            "cond_reacquire",
            blockers,
            saved_frame=saved,
            cond_wait_event_id=event.raw_id,
        )
        state.cond_waits.append(CondWaiter(cond=event.gen_from or uid, wait=wait))
        return True

    def on_cond_notify(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        cond = event.gen_from or event.lock_uid or ""
        candidates = [
            cw
            for name, state in self.threads.items()
            for cw in state.cond_waits
            if cw.cond == cond and cw.wait.status == "waiting"
        ]
        if not candidates:
            self.add_anomaly(
                Anomaly(
                    code=ANOM_NOTIFY_NO_WAITER,
                    event_id=event.raw_id,
                    thread=event.thread,
                    lock=cond,
                    message=f"notify 到达时 condition {cond} 上没有等待者",
                    confidence="low",
                )
            )
            return True
        target = candidates[0]
        # notify 本身不授予锁；等待者在随后的 acq_granted 中重新竞争成功。
        target.wait.confidence = "low" if event.kind == EV_COND_NOTIFY else "confirmed"
        return True

    def on_acq_timeout(self, event: Event) -> bool:
        if not self._check_alive(event):
            return False
        state = self.tstate(event.thread)
        uid = event.lock_uid or ""
        wait = state.active_wait(uid)
        if wait is None:
            wait = state.active_wait()
        if wait is None:
            self.add_anomaly(
                Anomaly(
                    code=ANOM_GRANT_WITHOUT_REQUEST,
                    event_id=event.raw_id,
                    thread=event.thread,
                    lock=uid,
                    message="超时事件没有对应的在途等待",
                    confidence="low",
                )
            )
            return False
        wait.status = "timeout"
        wait.end_event_id = event.raw_id
        # 超时取消等待；condition 超时若曾释放锁，保持释放状态（无凭空重获）。
        state.cond_waits = [cw for cw in state.cond_waits if cw.wait is not wait]
        return True

    # ---- 快照与图 ------------------------------------------------------
    def _refresh_wait_confidence(self) -> None:
        """根据当前持有者重算每条等待的阻塞者（等待图随状态变化）。"""
        for name, state in self.threads.items():
            for wait in state.waits:
                if wait.status != "waiting":
                    continue
                blockers = self.compute_blockers(name, wait.lock, wait.mode)
                wait._blockers = blockers  # type: ignore[attr-defined]

    def hold_edges(self) -> List[HoldEdge]:
        edges: List[HoldEdge] = []
        for name, state in self.threads.items():
            depth = 0
            for frame in state.stack:
                depth += 1
                edges.append(
                    HoldEdge(
                        thread=name,
                        lock=frame.lock,
                        mode=frame.mode,
                        depth=depth,
                        since_event_id=frame.acquire_event_id,
                        since_ts=frame.acquire_ts,
                        stack=list(frame.acquire_stack),
                        confidence=frame.confidence,
                        reason=frame.reason,
                        reentrant=frame.reentrant,
                    )
                )
        return edges

    def wait_edges(self) -> List[WaitEdge]:
        edges: List[WaitEdge] = []
        for name, state in self.threads.items():
            for wait in state.waits:
                if wait.status != "waiting":
                    continue
                blockers = self.compute_blockers(name, wait.lock, wait.mode)
                edges.append(
                    WaitEdge(
                        thread=name,
                        lock=wait.lock,
                        mode=wait.mode,
                        event_id=wait.event_id,
                        ts=wait.ts,
                        stack=list(wait.stack),
                        reason=wait.reason,
                        blockers=blockers,
                        confidence=wait.confidence,
                        deadline=wait.deadline,
                    )
                )
        return edges

    def snapshot(
        self, event: Optional[Event] = None, processed: bool = True
    ) -> Dict[str, Any]:
        holds = [edge.to_dict() for edge in self.hold_edges()]
        waits = [edge.to_dict() for edge in self.wait_edges()]
        cycles = detect_cycles(holds, waits)
        thread_summary = {
            name: {
                "alive": state.alive,
                "held": [frame.lock for frame in state.stack],
                "waiting_on": (
                    state.active_wait().lock if state.active_wait() else None
                ),
            }
            for name, state in self.threads.items()
        }
        return {
            "event_id": None if event is None else event.raw_id,
            "event_seq": None if event is None else event.seq,
            "ts": None if event is None else event.ts,
            "processed": processed,
            "holds": holds,
            "waits": waits,
            "cycles": cycles,
            "threads": thread_summary,
            "anomalies": [anom.to_dict() for anom in self.anomalies],
            "lock_types": {
                uid: {"kind": meta.kind, "declared": meta.declared}
                for uid, meta in self.lock_types.items()
            },
        }

    def final_report(self) -> Dict[str, Any]:
        if not self.snapshots:
            self.run()
        snapshot = self.snapshots[-1]
        return {
            "snapshots": self.snapshots,
            "event_index": list(self.event_index),
            "anomalies": [anom.to_dict() for anom in self.anomalies],
            "final": snapshot,
            "lock_types": {
                uid: {"kind": meta.kind, "declared": meta.declared}
                for uid, meta in self.lock_types.items()
            },
            "repair_choices": dict(self.repair_choices),
        }


def order_events(events: Sequence[Event]) -> List[Event]:
    """分析顺序：同代次按 (ts, 到达序 seq) 稳定排序；代次边界保留原始顺序。

    采集按代次保留原始顺序，因此排序只在同一 ``gen`` 内进行；不同代次按
    该代最早 seq 出现的先后排列，保证进程重启后的地址复用不会跨代串联。
    """
    gen_first_seq: Dict[str, int] = {}
    for event in events:
        if event.gen not in gen_first_seq or event.seq < gen_first_seq[event.gen]:
            gen_first_seq[event.gen] = event.seq
    return sorted(
        events,
        key=lambda ev: (gen_first_seq.get(ev.gen, ev.seq), ev.gen, ev.ts, ev.seq),
    )


# ---- 环检测 --------------------------------------------------------------
def _simple_cycles(graph: Dict[str, List[str]]) -> List[List[str]]:
    """枚举有向图中的简单环（去旋转等价，限制规模避免爆炸）。"""
    cycles: List[Tuple[str, ...]] = set()
    nodes = sorted(graph)

    def normalize(cycle: List[str]) -> Tuple[str, ...]:
        if not cycle:
            return tuple()
        pivot = min(range(len(cycle)), key=lambda idx: cycle[idx])
        rotated = cycle[pivot:] + cycle[:pivot]
        return tuple(rotated)

    def dfs(start: str, node: str, path: List[str], visited: set) -> None:
        if len(path) > 12:
            return
        for nxt in graph.get(node, []):
            if nxt == start and len(path) >= 1:
                key = normalize(path)
                cycles.add(key)
            elif nxt not in visited and nxt >= start:
                visited.add(nxt)
                dfs(start, nxt, path + [nxt], visited)
                visited.remove(nxt)

    for node in nodes:
        dfs(node, node, [node], {node})
    return [list(cycle) for cycle in sorted(cycles, key=lambda item: (len(item), item))]


def detect_cycles(
    holds: List[Dict[str, Any]], waits: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """同时检测线程 wait-for 环与锁序环，边都带证据事件与栈。"""
    results: List[Dict[str, Any]] = []

    wait_by_thread = {edge["thread"]: edge for edge in waits}
    # 线程 -> 它阻塞的线程：wait_for[t] = blockers
    thread_graph: Dict[str, List[str]] = {}
    for edge in waits:
        thread_graph.setdefault(edge["thread"], [])
        for blocker in edge["blockers"]:
            thread_graph.setdefault(blocker, [])
            # 只有 blocker 自己也在等待时才构成 wait-for 环。
            if blocker in wait_by_thread:
                thread_graph[edge["thread"]].append(blocker)

    for cycle in _simple_cycles(thread_graph):
        legs = []
        for idx, thread in enumerate(cycle):
            blocker = cycle[(idx + 1) % len(cycle)]
            edge = wait_by_thread.get(thread)
            if edge is None or blocker not in edge["blockers"]:
                legs = []
                break
            holder_edge = next(
                (
                    hold
                    for hold in holds
                    if hold["thread"] == blocker and hold["lock"] == edge["lock"]
                ),
                None,
            )
            legs.append(
                {
                    "thread": thread,
                    "waits_for_thread": blocker,
                    "lock": edge["lock"],
                    "mode": edge["mode"],
                    "wait_event_id": edge["event_id"],
                    "wait_stack": edge["stack"],
                    "wait_reason": edge["reason"],
                    "held_lock_by_blocker": None
                    if holder_edge is None
                    else holder_edge["lock"],
                    "holder_stack": None if holder_edge is None else holder_edge["stack"],
                    "holder_acquire_event_id": None
                    if holder_edge is None
                    else holder_edge["since_event_id"],
                }
            )
        if legs:
            confidence = "confirmed"
            if any(
                wait_by_thread[t]["confidence"] != "confirmed" for t in cycle
            ):
                confidence = "low"
            results.append(
                {
                    "kind": "thread_wait_cycle",
                    "threads": cycle,
                    "locks": sorted({leg["lock"] for leg in legs}),
                    "legs": legs,
                    "confidence": confidence,
                    "evidence_event_ids": sorted(
                        {leg["wait_event_id"] for leg in legs}
                        | {
                            leg["holder_acquire_event_id"]
                            for leg in legs
                            if leg["holder_acquire_event_id"] is not None
                        }
                    ),
                }
            )

    # 锁序环：线程 t 持 A 等 B => A -> B
    held_by_thread: Dict[str, List[Dict[str, Any]]] = {}
    for edge in holds:
        held_by_thread.setdefault(edge["thread"], []).append(edge)
    lock_graph: Dict[str, List[str]] = {}
    lock_leg_meta: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for edge in waits:
        for held in held_by_thread.get(edge["thread"], []):
            a_lock = held["lock"]
            b_lock = edge["lock"]
            if a_lock == b_lock:
                continue
            lock_graph.setdefault(a_lock, [])
            if b_lock not in lock_graph[a_lock]:
                lock_graph[a_lock].append(b_lock)
            lock_graph.setdefault(b_lock, [])
            lock_leg_meta[(a_lock, b_lock)] = {
                "thread": edge["thread"],
                "held_lock": a_lock,
                "waited_lock": b_lock,
                "hold_event_id": held["since_event_id"],
                "hold_stack": held["stack"],
                "wait_event_id": edge["event_id"],
                "wait_stack": edge["stack"],
            }
    for cycle in _simple_cycles(lock_graph):
        legs = []
        ok = True
        for idx, lock in enumerate(cycle):
            nxt = cycle[(idx + 1) % len(cycle)]
            meta = lock_leg_meta.get((lock, nxt))
            if meta is None:
                ok = False
                break
            legs.append(meta)
        if not ok:
            continue
        results.append(
            {
                "kind": "lock_order_cycle",
                "threads": sorted({leg["thread"] for leg in legs}),
                "locks": cycle,
                "legs": legs,
                "confidence": "confirmed",
                "evidence_event_ids": sorted(
                    {leg["hold_event_id"] for leg in legs}
                    | {leg["wait_event_id"] for leg in legs}
                ),
            }
        )
    return results
