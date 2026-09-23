"""锁序解剖室核心引擎。

负责：
- 事件归一化与按采集代次保留原始顺序；
- 可重入锁 / 读写锁（共享持有、升级）/ condition wait/notify 的状态机回放；
- 任意时点的持有图与等待图，边可回溯到事件；
- 环检测（给出线程、锁、持有栈、等待栈）与近失（环出现后又解除）；
- 缺事件 / 乱序下的带置信候选状态（绝不凭空补 release）；
- 事件校正分支、锁类型声明、最小化复现片段。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

REQUEST = "request"
ACQUIRED = "acquired"
RELEASED = "released"
THREAD_START = "thread_start"
THREAD_END = "thread_end"
COND_WAIT = "cond_wait"
COND_NOTIFY = "cond_notify"
COND_ACQUIRED = "cond_acquired"
TIMEOUT = "timeout"

EVENT_TYPES = {
    REQUEST, ACQUIRED, RELEASED, THREAD_START, THREAD_END,
    COND_WAIT, COND_NOTIFY, COND_ACQUIRED, TIMEOUT,
}
EX = "exclusive"
SH = "shared"
MODES = {EX, SH}

PLAIN = "plain"
REENTRANT = "reentrant"
RW = "rw"
LOCK_TYPES = {PLAIN, REENTRANT, RW}


def normalize_mode(mode: Any) -> str:
    value = (mode or EX).lower()
    if value in ("x", "w", "write", "exclusive"):
        return EX
    if value in ("s", "r", "read", "shared"):
        return SH
    return EX


def normalize_event(raw: Dict[str, Any], arrival: int) -> Dict[str, Any]:
    """将外部事件字典归一化。不丢弃字段，补齐规范字段。"""
    event_type = str(raw.get("type", "")).lower()
    thread = raw.get("thread")
    lock = raw.get("lock")
    cond = raw.get("cond")
    ts = raw.get("ts")
    if ts is not None:
        try:
            ts = float(ts)
        except (TypeError, ValueError):
            ts = None
    seq = raw.get("seq", arrival)
    try:
        seq = int(seq)
    except (TypeError, ValueError):
        seq = arrival
    gen = int(raw.get("gen", raw.get("generation", 0)) or 0)
    return {
        "id": raw.get("id"),
        "type": event_type,
        "thread": None if thread is None else str(thread),
        "lock": None if lock is None else str(lock),
        "cond": None if cond is None else str(cond),
        "mode": normalize_mode(raw.get("mode")),
        "ts": ts,
        "seq": seq,
        "gen": gen,
        "arrival": arrival,
        "target": None if raw.get("target") is None else str(raw.get("target")),
        "broadcast": bool(raw.get("broadcast", False)),
        "stack": list(raw.get("stack") or []),
        "raw": raw,
    }


def canonical_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """逻辑顺序：采集代次 -> 序列号 -> 到达序（保留原始顺序）。"""
    return sorted(events, key=lambda e: (e["gen"], e["seq"], e["arrival"]))


def lock_id(gen: int, lock: Optional[str]) -> str:
    return f"{lock}@g{gen}"


def cond_id(gen: int, cond: Optional[str]) -> str:
    return f"{cond}@g{gen}"


def thread_id(gen: int, thread: Optional[str]) -> str:
    return f"{thread}@g{gen}"


@dataclass
class Frame:
    lock: str
    mode: str
    acquired_event: int
    cond_stashed: Optional[Dict[str, Any]] = None
    reentry_depth: int = 1


@dataclass
class Pending:
    lock: str
    mode: str
    request_event: int
    stack: List[str] = field(default_factory=list)
    kind: str = "lock"          # lock | cond
    cond: Optional[str] = None
    notified_by: Optional[int] = None
    upgrade: bool = False


@dataclass
class Sleep:
    cond: str
    wait_event: int
    lock: str
    stack: List[str] = field(default_factory=list)


@dataclass
class ThreadState:
    name: str
    alive: bool = False
    started: bool = False
    frames: List[Frame] = field(default_factory=list)
    pending: Optional[Pending] = None
    sleep: Optional[Sleep] = None


@dataclass
class LockState:
    name: str
    holders: List[Frame] = field(default_factory=list)
    queue: List[Pending] = field(default_factory=list)
    observed_modes: set = field(default_factory=set)
    observed_recursive: bool = False
    upgraded: bool = False


class ReplayError(Exception):
    pass


def compatible(mode: str, held: str) -> bool:
    return mode == SH and held == SH


@dataclass
class State:
    declared: Dict[str, str] = field(default_factory=dict)
    threads: Dict[str, ThreadState] = field(default_factory=dict)
    locks: Dict[str, LockState] = field(default_factory=list)
    conds: Dict[str, List[Sleep]] = field(default_factory=dict)
    anomalies: List[Dict[str, Any]] = field(default_factory=list)
    positions: int = 0
    inferred: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not self.threads:
            self.threads = {}
        if not self.locks:
            self.locks = {}
        if not self.conds:
            self.conds = {}

    def thread(self, name: str) -> ThreadState:
        st = self.threads.get(name)
        if st is None:
            st = ThreadState(name=name)
            self.threads[name] = st
        return st

    def lock(self, name: str) -> LockState:
        st = self.locks.get(name)
        if st is None:
            st = LockState(name=name)
            self.locks[name] = st
        return st

    def lock_type(self, name: str) -> str:
        if name in self.declared:
            return self.declared[name]
        if name in self.inferred:
            return self.inferred[name]
        lock = self.locks.get(name)
        if lock is None:
            return PLAIN
        if lock.observed_modes == {SH}:
            self.inferred[name] = RW
        elif lock.observed_recursive:
            self.inferred[name] = REENTRANT
        else:
            self.inferred[name] = PLAIN
        return self.inferred[name]

    def report(self, code: str, event_index: int, message: str,
               thread: Optional[str] = None, lock: Optional[str] = None,
               cond: Optional[str] = None, severity: str = "warning") -> None:
        self.anomalies.append({
            "code": code,
            "event": event_index,
            "message": message,
            "thread": thread,
            "lock": lock,
            "cond": cond,
            "severity": severity,
        })


def _frame_for(ts: ThreadState, lock: str) -> Optional[Frame]:
    for frame in reversed(ts.frames):
        if frame.lock == lock:
            return frame
    return None


def apply_event(state: State, event: Dict[str, Any]) -> None:
    """对归一化后的单个事件推进状态。事件索引由调用方写入 anomaly。"""
    idx = state.positions
    etype = event["type"]
    gen = event["gen"]
    tname = thread_id(gen, event["thread"]) if event["thread"] else None
    lname = lock_id(gen, event["lock"]) if event["lock"] else None
    cname = cond_id(gen, event["cond"]) if event["cond"] else None
    mode = event["mode"]

    if etype not in EVENT_TYPES:
        state.report("UNKNOWN_EVENT", idx, f"未知事件类型 {etype!r}，已忽略",
                     tname, lname, cname, "info")
        return

    if etype == THREAD_START:
        ts = state.thread(tname)
        if ts.started:
            state.report("THREAD_RESTART", idx,
                         f"线程 {tname} 重复开始", tname, severity="warning")
        ts.started = True
        ts.alive = True
        return

    ts = state.thread(tname) if tname else None
    if tname is not None and not ts.started:
        state.report("MISSING_THREAD_START", idx,
                     f"线程 {tname} 缺少 thread_start，按已启动处理",
                     tname, lname, cname, "warning")
        ts.started = True
        ts.alive = True
    if tname is not None and not ts.alive and etype != THREAD_END:
        state.report("EVENT_AFTER_END", idx,
                     f"线程 {tname} 已结束后仍有事件", tname, lname, cname,
                     "warning")
        ts.alive = True

    if etype == REQUEST:
        _do_request(state, idx, event, ts, lname, mode)
    elif etype == ACQUIRED:
        _do_acquired(state, idx, event, ts, lname, mode, kind="lock",
                     notified_by=None)
    elif etype == RELEASED:
        _do_released(state, idx, event, ts, lname, mode)
    elif etype == COND_WAIT:
        _do_cond_wait(state, idx, event, ts, lname, cname)
    elif etype == COND_NOTIFY:
        _do_cond_notify(state, idx, event, ts, lname, cname)
    elif etype == COND_ACQUIRED:
        _do_acquired(state, idx, event, ts, lname, mode, kind="cond",
                     notified_by=event.get("notified_by_event"))
    elif etype == TIMEOUT:
        _do_timeout(state, idx, event, ts, lname, cname)
    elif etype == THREAD_END:
        _do_thread_end(state, idx, event, ts)


def _do_request(state, idx, event, ts, lname, mode):
    lock = state.lock(lname)
    lock.observed_modes.add(mode)
    if ts.pending is not None:
        state.report("OVERLAPPING_REQUEST", idx,
                     f"线程 {ts.name} 在等待 {ts.pending.lock} 时又请求 {lname}",
                     ts.name, lname, severity="warning")
    frame = _frame_for(ts, lname)
    ltype = state.lock_type(lname)
    if frame is not None:
        if ltype == PLAIN:
            state.report("RECURSION_ON_PLAIN_LOCK", idx,
                         f"线程 {ts.name} 重复获取未声明可重入的锁 {lname}，"
                         "推断为可重入；若实为自锁会形成自环",
                         ts.name, lname, severity="warning")
            lock.observed_recursive = True
            state.inferred[lname] = REENTRANT
            ltype = REENTRANT
        if ltype in (REENTRANT, PLAIN):
            frame.reentry_depth += 1
            return
        if ltype == RW:
            if frame.mode == SH and mode == EX:
                _enqueue(state, idx, event, ts, lock, mode, upgrade=True)
                return
            frame.reentry_depth += 1
            return
    _enqueue(state, idx, event, ts, lock, mode, upgrade=False)


def _enqueue(state, idx, event, ts, lock, mode, upgrade):
    pending = Pending(
        lock=lock.name, mode=mode, request_event=idx,
        stack=list(event.get("stack") or []), upgrade=upgrade,
    )
    ts.pending = pending
    lock.queue.append(pending)
