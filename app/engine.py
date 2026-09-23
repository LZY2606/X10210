"""锁序解剖室核心引擎。

纯函数式重放：输入一批按采集代次/时间戳/到达顺序排序的事件，
输出任意时点的持有图、等待图、候选图、置信状态与环路证据。
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# 事件模型
# ---------------------------------------------------------------------------

THREAD_START = "thread_start"
THREAD_END = "thread_end"
ACQUIRE_REQ = "acquire_req"
ACQUIRED = "acquired"
RELEASED = "released"
COND_WAIT = "cond_wait"
COND_WAITED = "cond_waited"
COND_NOTIFY = "cond_notify"
COND_ACQUIRED = "cond_acquired"
CANCELLED = "cancelled"

ALIASES = {
    "start": THREAD_START,
    "thread_begin": THREAD_START,
    "end": THREAD_END,
    "thread_exit": THREAD_END,
    "exit": THREAD_END,
    "request": ACQUIRE_REQ,
    "acquire": ACQUIRE_REQ,
    "acq_req": ACQUIRE_REQ,
    "wait_acquire": ACQUIRE_REQ,
    "grant": ACQUIRED,
    "acquire_ok": ACQUIRED,
    "acquire_success": ACQUIRED,
    "success": ACQUIRED,
    "release": RELEASED,
    "unlock": RELEASED,
    "wait": COND_WAIT,
    "condition_wait": COND_WAIT,
    "cond_wait_end": COND_WAITED,
    "signaled": COND_WAITED,
    "notify": COND_NOTIFY,
    "signal": COND_NOTIFY,
    "notify_one": COND_NOTIFY,
    "notify_all": COND_NOTIFY,
    "broadcast": COND_NOTIFY,
    "wakeup": COND_ACQUIRED,
    "reacquire": COND_ACQUIRED,
    "reacquired": COND_ACQUIRED,
    "timeout": CANCELLED,
    "cancel": CANCELLED,
    "cancelled_wait": CANCELLED,
    "wait_cancelled": CANCELLED,
}

MUTEX = "mutex"
RWLOCK = "rwlock"
READ = "S"
WRITE = "X"
UPGRADE = "U"

HIGH = "high"
MEDIUM = "medium"
LOW = "low"


@dataclass
class Event:
    id: Optional[int]
    gen: str
    ts: float
    seq: int
    type: str
    thread: str
    lock: Optional[str] = None
    mode: str = WRITE
    cond: Optional[str] = None
    notify_all: bool = False
    stack: tuple = ()
    raw: dict = field(default_factory=dict)

    def to_public(self) -> dict:
        return {
            "id": self.id,
            "gen": self.gen,
            "ts": self.ts,
            "seq": self.seq,
            "type": self.type,
            "thread": self.thread,
            "lock": self.lock,
            "mode": self.mode,
            "cond": self.cond,
            "notify_all": self.notify_all,
            "stack": list(self.stack),
        }


def normalize_type(name: str) -> str:
    key = (name or "").strip().lower()
    if key in ALIASES:
        return ALIASES[key]
    allowed = {
        THREAD_START, THREAD_END, ACQUIRE_REQ, ACQUIRED, RELEASED,
        COND_WAIT, COND_WAITED, COND_NOTIFY, COND_ACQUIRED, CANCELLED,
    }
    if key in allowed:
        return key
    raise ValueError(f"未知事件类型: {name!r}")


def normalize_mode(mode: Any) -> str:
    m = (mode or WRITE)
    m = str(m).upper()
    if m in {"R", "RD", "READ", "SHARED", "SHR"}:
        return READ
    if m in {"W", "WR", "WRITE", "EXCLUSIVE", "EXC"}:
        return WRITE
    if m in {"U", "UP", "UPGRADE"}:
        return UPGRADE
    if m in (READ, WRITE, UPGRADE):
        return m
    return WRITE


def lkey(gen: str, lock: str) -> str:
    return f"{gen}::{lock}"


def ckey(gen: str, cond: str) -> str:
    return f"{gen}::{cond}"


# ---------------------------------------------------------------------------
# 重放状态
# ---------------------------------------------------------------------------

@dataclass
class Frame:
    gen: str
    lock: str
    mode: str
    req_event: int
    grant_event: int
    req_stack: tuple = ()
    grant_stack: tuple = ()
    suspended: bool = False
    upgrade: bool = False

    @property
    def key(self) -> str:
        return lkey(self.gen, self.lock)


@dataclass
class LockState:
    gen: str
    lock: str
    type: str
    holders: list = field(default_factory=list)  # list[(thread, Frame)]（X 至多一条，S 多条）
    queue: list = field(default_factory=list)    # list[WaitRec]

    @property
    def key(self) -> str:
        return lkey(self.gen, self.lock)

    def held_threads(self) -> set:
        return {t for t, _f in self.holders}


@dataclass
class WaitRec:
    wait_id: int
    gen: str
    lock: str
    thread: str
    mode: str
    kind: str                       # lock / cond
    req_event: int
    req_stack: tuple
    cond: Optional[str] = None
    cond_event: Optional[int] = None
    suspended_frames: list = field(default_factory=list)
    blocker_event: Optional[int] = None

    @property
    def key(self) -> str:
        return lkey(self.gen, self.lock)


@dataclass
class WaitSpan:
    wait_id: int
    gen: str
    lock: str
    thread: str
    mode: str
    kind: str
    cond: Optional[str]
    req_event: int
    start_ts: float
    start_seq: int
    end_event: Optional[int] = None
    end_ts: Optional[float] = None
    outcome: str = "waiting"       # granted / cancelled / waiting
    blockers: list = field(default_factory=list)


@dataclass
class HoldSpan:
    gen: str
    lock: str
    thread: str
    mode: str
    grant_event: int
    start_ts: float
    start_seq: int
    end_event: Optional[int] = None
    end_ts: Optional[float] = None


@dataclass
class Anomaly:
    code: str
    severity: str
    message: str
    event: Optional[int]
    gen: Optional[str]
    thread: Optional[str]
    lock: Optional[str]


SEVERITY_OF = {
    "unknown_event_type": HIGH,
    "lock_type_conflict": HIGH,
    "release_without_hold": MEDIUM,
    "grant_not_requested": MEDIUM,
    "incompatible_grant": MEDIUM,
    "cond_without_hold": MEDIUM,
    "cond_acquired_without_wait": MEDIUM,
    "cancel_unknown_wait": LOW,
    "request_after_thread_end": MEDIUM,
    "missing_release": MEDIUM,
    "ambiguous_order": LOW,
}


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def ordered(events: list[Event]) -> list[Event]:
    return sorted(events, key=lambda e: (e.gen, e.ts, e.seq))


# ---------------------------------------------------------------------------
# 重放器
# ---------------------------------------------------------------------------

SNAPSHOT_BLOCK = 32


class Replayer:
    def __init__(
        self,
        events: list[Event],
        declared_types: Optional[dict] = None,
        include_ambiguity: bool = False,
    ):
        self.events = ordered(events)
        self.declared_types = declared_types or {}
        self.include_ambiguity = include_ambiguity
        self.lock_types: dict[str, str] = {}
        for key, typ in self.declared_types.items():
            self.lock_types[key] = typ
        for ev in self.events:
            if ev.lock is None:
                continue
            k = lkey(ev.gen, ev.lock)
            if ev.type in (ACQUIRE_REQ, ACQUIRED) and ev.mode in (READ, UPGRADE):
                inferred = RWLOCK
            elif ev.type in (ACQUIRE_REQ, ACQUIRED, RELEASED):
                inferred = MUTEX
            else:
                inferred = MUTEX
            if k in self.lock_types:
                declared = self.lock_types[k]
                if (
                    inferred == RWLOCK
                    and declared == MUTEX
                ):
                    pass
            else:
                self.lock_types[k] = inferred

        self.threads: dict[str, dict] = {}
        self.locks: dict[str, LockState] = {}
        self.frames: dict[str, list[Frame]] = {}
        self.waits: dict[int, WaitRec] = {}
        self.wait_spans: dict[int, WaitSpan] = {}
        self.hold_spans: list[HoldSpan] = []
        self.anomalies: list[Anomaly] = []
        self._wid = 0
        self.next_i = 0
        self._checkpoints: dict[int, dict] = {}

    # -- 基础帮助 --------------------------------------------------------

    def _anomaly(self, code: str, ev: Optional[Event], **kw):
        self.anomalies.append(
            Anomaly(
                code=code,
                severity=SEVERITY_OF.get(code, LOW),
                message=kw.pop("message", code),
                event=ev.id if ev else None,
                gen=kw.get("gen", ev.gen if ev else None),
                thread=kw.get("thread", ev.thread if ev else None),
                lock=kw.get("lock", ev.lock if ev else None),
            )
        )

    def _thread(self, t: str) -> dict:
        return self.threads.setdefault(t, {"alive": True, "started": False})

    def _lock(self, ev: Event) -> LockState:
        k = lkey(ev.gen, ev.lock)
        ls = self.locks.get(k)
        if ls is None:
            ls = LockState(ev.gen, ev.lock, self.lock_types.get(k, MUTEX))
            self.locks[k] = ls
        return ls

    def _new_wait(self, ev: Event, kind: str, **kw) -> WaitRec:
        self._wid += 1
        rec = WaitRec(
            wait_id=self._wid,
            gen=ev.gen,
            lock=ev.lock,
            thread=ev.thread,
            mode=ev.mode,
            kind=kind,
            req_event=ev.id,
            req_stack=ev.stack,
            **kw,
        )
        self.wait_spans[self._wid] = WaitSpan(
            wait_id=self._wid,
            gen=ev.gen,
            lock=ev.lock,
            thread=ev.thread,
            mode=ev.mode,
            kind=kind,
            cond=kw.get("cond"),
            req_event=ev.id,
            start_ts=ev.ts,
            start_seq=ev.seq,
        )
        return rec

    def _close_wait(self, rec: WaitRec, ev: Event, outcome: str):
        span = self.wait_spans[rec.wait_id]
        span.outcome = outcome
        span.end_event = ev.id
        span.end_ts = ev.ts

    def _pop_waiter(self, ls: LockState, thread: str, modes=None) -> Optional[WaitRec]:
        for idx, rec in enumerate(ls.queue):
            if rec.thread == thread and (modes is None or rec.mode in modes):
                return ls.queue.pop(idx)
        return None

    def _find_wait(self, thread: str, gen: str, lock: str) -> Optional[WaitRec]:
        for rec in self.waits.values():
            if rec.thread == thread and rec.gen == gen and rec.lock == lock:
                return rec
        return None

    # -- 持有帧帮助 ------------------------------------------------------

    def _holder_frame(self, ls: LockState, thread: str, mode=None) -> Optional[tuple]:
        for pair in ls.holders:
            t, fr = pair
            if t == thread and (mode is None or fr.mode == mode):
                return pair
        return None

    def _add_frame(self, ls: LockState, ev: Event, mode: str, upgrade=False) -> Frame:
        frame = Frame(
            gen=ev.gen,
            lock=ev.lock,
            mode=mode,
            req_event=-1,
            grant_event=ev.id,
            grant_stack=ev.stack,
        )
        frame.upgrade = upgrade
        ls.holders.append((ev.thread, frame))
        self.frames.setdefault(ev.thread, []).append(frame)
        self.hold_spans.append(
            HoldSpan(
                gen=ev.gen,
                lock=ev.lock,
                thread=ev.thread,
                mode="U->X" if upgrade else mode,
                grant_event=ev.id,
                start_ts=ev.ts,
                start_seq=ev.seq,
            )
        )
        return frame

    def _remove_frame(self, ls: LockState, frame: Frame, ev: Event):
        for idx, pair in enumerate(ls.holders):
            if pair[1] is frame:
                ls.holders.pop(idx)
                break
        fl = self.frames.get(ev.thread)
        if frame in fl:
            fl.remove(frame)
        for span in reversed(self.hold_spans):
            if span.grant_event == frame.grant_event and span.thread == ev.thread and span.end_event is None:
                span.end_event = ev.id
                span.end_ts = ev.ts
                break

    def _suspend_frames(self, ls: LockState, thread: str) -> list:
        suspended = []
        for pair in list(ls.holders):
            t, fr = pair
            if t == thread:
                ls.holders.remove(pair)
                if fr in self.frames.get(thread, []):
                    self.frames[thread].remove(fr)
                fr.suspended = True
                suspended.append(fr)
        return suspended

    def _restore_frames(self, ls: LockState, thread: str, frames: list):
        for fr in frames:
            fr.suspended = False
            ls.holders.append((thread, fr))
            self.frames.setdefault(thread, []).append(fr)

    # -- 阻塞计算 --------------------------------------------------------

    def _blockers(self, ls: LockState, thread: str, mode: str) -> list:
        """返回阻塞当前请求的 (holder_thread, frame) 列表。"""
        if ls.type == MUTEX:
            return [(t, fr) for t, fr in ls.holders if t != thread]
        own = [fr for t, fr in ls.holders if t == thread]
        other = [(t, fr) for t, fr in ls.holders if t != thread]
        if mode == READ:
            return [(t, fr) for t, fr in other if fr.mode == WRITE]
        if mode == UPGRADE:
            other_readers = [(t, fr) for t, fr in other if fr.mode == READ]
            other_writers = [(t, fr) for t, fr in other if fr.mode == WRITE]
            has_s = any(fr.mode == READ for fr in own)
            if not has_s or other_readers or other_writers:
                return other_writers + other_readers
            return []
        return list(other)

    # -- 主入口 ----------------------------------------------------------

    def run_to(self, i: int) -> None:
        i = max(0, min(i, len(self.events)))
        if i < self.next_i:
            cp = max(k for k in self._checkpoints if k <= i)
            self._restore(cp)
        while self.next_i < i:
            ev = self.events[self.next_i]
            self._apply(ev)
            self.next_i += 1
            if self.next_i % SNAPSHOT_BLOCK == 0 and self.next_i < len(self.events):
                self._checkpoints[self.next_i] = self._snapshot()

    def run_all(self) -> None:
        self.run_to(len(self.events))

    def _apply(self, ev: Event):
        th = self._thread(ev.thread)
        if ev.type == THREAD_START:
            th["started"] = True
            th["alive"] = True
            return
        if ev.type == THREAD_END:
            self._thread_end(ev)
            return
        if not th.get("started"):
            th["started"] = True
        if not th["alive"] and ev.type != THREAD_START:
            self._anomaly("request_after_thread_end", ev)
            th["alive"] = True
        handler = {
            ACQUIRE_REQ: self._apply_acquire_req,
            ACQUIRED: self._apply_acquired,
            RELEASED: self._apply_released,
            COND_WAIT: self._apply_cond_wait,
            COND_WAITED: self._apply_cond_waited,
            COND_NOTIFY: self._apply_notify,
            COND_ACQUIRED: self._apply_cond_acquired,
            CANCELLED: self._apply_cancel,
        }.get(ev.type)
        if handler:
            handler(ev)

    # -- 事件转移 --------------------------------------------------------

    def _apply_acquire_req(self, ev: Event):
        ls = self._lock(ev)
        mode = WRITE if ls.type == MUTEX else ev.mode
        pair = self._holder_frame(ls, ev.thread)
        if ls.type == MUTEX and pair is not None:
            frame = pair[1]
            frame.reentry = getattr(frame, "reentry", 1) + 1
            return
        if ls.type == RWLOCK:
            own_s = self._holder_frame(ls, ev.thread, READ)
            own_x = self._holder_frame(ls, ev.thread, WRITE)
            if mode == READ and (own_s or own_x):
                if own_s is not None:
                    own_s[1].reentry = getattr(own_s[1], "reentry", 1) + 1
                return
            if mode == WRITE and own_x:
                own_x[1].reentry = getattr(own_x[1], "reentry", 1) + 1
                return
        blockers = self._blockers(ls, ev.thread, mode)
        rec = self._new_wait(ev, kind="lock")
        rec.blocker_event = blockers[0][1].grant_event if blockers else None
        self.waits[rec.wait_id] = rec
        # 请求只登记等待；必须等显式成功事件（acquired）才授予，
        # 保证每条持有边都能回到产生它的成功事件。
        ls.queue.append(rec)

    def _grant(self, ls: LockState, rec: WaitRec, ev: Event):
        self.waits.pop(rec.wait_id, None)
        if rec in ls.queue:
            ls.queue.remove(rec)
        frame = Frame(
            gen=rec.gen,
            lock=rec.lock,
            mode=WRITE if ls.type == MUTEX else rec.mode,
            req_event=rec.req_event,
            grant_event=ev.id,
            req_stack=rec.req_stack,
            grant_stack=ev.stack,
        )
        ls.holders.append((rec.thread, frame))
        self.frames.setdefault(rec.thread, []).append(frame)
        self.hold_spans.append(
            HoldSpan(
                gen=rec.gen,
                lock=rec.lock,
                thread=rec.thread,
                mode=frame.mode,
                grant_event=ev.id,
                start_ts=ev.ts,
                start_seq=ev.seq,
            )
        )
        span = self.wait_spans[rec.wait_id]
        span.outcome = "granted"
        span.end_event = ev.id
        span.end_ts = ev.ts

    def _apply_acquired(self, ev: Event):
        ls = self._lock(ev)
        mode = WRITE if ls.type == MUTEX else ev.mode
        if ls.type == RWLOCK and mode == UPGRADE:
            own = self._holder_frame(ls, ev.thread, READ)
            if own is not None and not self._blockers(ls, ev.thread, UPGRADE):
                frame = own[1]
                frame.mode = WRITE
                frame.upgrade = True
                for span in reversed(self.hold_spans):
                    if (
                        span.grant_event == frame.grant_event
                        and span.thread == ev.thread
                        and span.end_event is None
                    ):
                        span.mode = "U->X"
                        break
                rec = self._pop_waiter(ls, ev.thread)
                if rec:
                    self.waits.pop(rec.wait_id, None)
                    self._close_wait(rec, ev, "granted")
                return
        rec = self._pop_waiter(ls, ev.thread)
        if rec is None:
            if self._holder_frame(ls, ev.thread) is not None and ls.type == MUTEX:
                return
            self._anomaly(
                "grant_not_requested",
                ev,
                message=f"线程 {ev.thread} 对锁 {ev.lock} 的授予事件没有对应等待",
            )
            rec = self._new_wait(ev, kind="lock")
        blockers = self._blockers(ls, ev.thread, rec.mode)
        if blockers and not self._grant_compatible(ls, rec):
            self._anomaly(
                "incompatible_grant",
                ev,
                message=f"锁 {ev.lock} 在仍被占用时授予给 {ev.thread}",
            )
        self._grant(ls, rec, ev)

    def _grant_compatible(self, ls: LockState, rec: WaitRec) -> bool:
        """授予时与现有持有者是否兼容（读写锁共享读）。"""
        return not self._blockers(ls, rec.thread, rec.mode)

    def _apply_released(self, ev: Event):
        ls = self._lock(ev)
        pair = self._holder_frame(ls, ev.thread)
        if pair is None:
            self._anomaly(
                "release_without_hold",
                ev,
                message=f"线程 {ev.thread} 释放未持有的锁 {ev.lock}（不凭空补 release）",
            )
            return
        _thread, frame = pair
        depth = getattr(frame, "reentry", 1)
        if depth > 1:
            frame.reentry = depth - 1
            return
        self._remove_frame(ls, frame, ev)
        self._pump_queue(ls, ev)

    def _pump_queue(self, ls: LockState, ev: Event):
        for rec in list(ls.queue):
            blockers = self._blockers(ls, rec.thread, rec.mode)
            rec.blocker_event = blockers[0][1].grant_event if blockers else None

    def _apply_cond_wait(self, ev: Event):
        ls = self._lock(ev)
        if self._holder_frame(ls, ev.thread) is None:
            self._anomaly(
                "cond_without_hold",
                ev,
                message=f"线程 {ev.thread} 在未持有锁 {ev.lock} 时进入 condition 等待",
            )
        suspended = self._suspend_frames(ls, ev.thread)
        rec = self._new_wait(
            ev,
            kind="cond",
            cond=ev.cond,
            cond_event=ev.id,
            suspended_frames=suspended,
        )
        rec.blocker_event = None
        self.waits[rec.wait_id] = rec
        self._pump_queue(ls, ev)

    def _apply_cond_waited(self, ev: Event):
        rec = self._find_wait(ev.thread, ev.gen, ev.lock)
        if rec is not None and rec.kind == "cond" and rec.cond == ev.cond:
            return
        self._apply_cond_wait(ev)

    def _apply_notify(self, ev: Event):
        waiters = [
            rec for rec in self.waits.values()
            if rec.kind == "cond" and ckey(rec.gen, rec.cond) == ckey(ev.gen, ev.cond)
        ]
        waiters.sort(key=lambda r: (r.req_event, r.wait_id))
        chosen = waiters if ev.notify_all else waiters[:1]
        for rec in chosen:
            ls = self.locks.get(lkey(rec.gen, rec.lock))
            if ls is None:
                continue
            rec.kind = "cond_reacquire"
            blockers = self._blockers(ls, rec.thread, rec.mode)
            rec.blocker_event = blockers[0][1].grant_event if blockers else None
            if rec not in ls.queue:
                ls.queue.append(rec)
            self.wait_spans[rec.wait_id].blockers.append({"event": ev.id, "ts": ev.ts})

    def _apply_cond_acquired(self, ev: Event):
        rec = self._find_wait(ev.thread, ev.gen, ev.lock)
        ls = self._lock(ev)
        if rec is None or rec.kind not in ("cond", "cond_reacquire"):
            self._anomaly(
                "cond_acquired_without_wait",
                ev,
                message=f"线程 {ev.thread} 的 condition 重获事件没有对应等待",
            )
            self._grant(ls, self._new_wait(ev, kind="lock"), ev)
            return
        self.waits.pop(rec.wait_id, None)
        if rec in ls.queue:
            ls.queue.remove(rec)
        self._restore_frames(ls, rec.thread, rec.suspended_frames)
        span = self.wait_spans[rec.wait_id]
        span.outcome = "granted"
        span.end_event = ev.id
        span.end_ts = ev.ts

    def _apply_cancel(self, ev: Event):
        rec = self._find_wait(ev.thread, ev.gen, ev.lock)
        if rec is None:
            self._anomaly(
                "cancel_unknown_wait",
                ev,
                message=f"取消了线程 {ev.thread} 不存在的等待（锁 {ev.lock}）",
            )
            return
        ls = self.locks.get(lkey(rec.gen, rec.lock))
        self.waits.pop(rec.wait_id, None)
        if ls and rec in ls.queue:
            ls.queue.remove(rec)
        if rec.kind in ("cond", "cond_reacquire") and ls is not None:
            self._restore_frames(ls, rec.thread, rec.suspended_frames)
        self._close_wait(rec, ev, "cancelled")
        if ls is not None:
            self._pump_queue(ls, ev)

    def _thread_end(self, ev: Event):
        th = self._thread(ev.thread)
        th["alive"] = False
        pending = [r for r in list(self.waits.values()) if r.thread == ev.thread]
        for rec in pending:
            ls = self.locks.get(lkey(rec.gen, rec.lock))
            self.waits.pop(rec.wait_id, None)
            if ls and rec in ls.queue:
                ls.queue.remove(rec)
            if rec.kind in ("cond", "cond_reacquire") and ls is not None:
                self._restore_frames(ls, rec.thread, rec.suspended_frames)
            self._close_wait(rec, ev, "thread_exit")
        for frame in list(self.frames.get(ev.thread, [])):
            if not frame.suspended:
                self._anomaly(
                    "missing_release",
                    ev,
                    lock=frame.lock,
                    message=f"线程 {ev.thread} 终止时仍持有锁 {frame.lock}（缺 release，不自动补全）",
                )

    # -- 快照与回滚 ------------------------------------------------------

    def _snapshot(self) -> dict:
        import copy
        return {
            "threads": copy.deepcopy(self.threads),
            "locks": copy.deepcopy(self.locks),
            "frames": copy.deepcopy(self.frames),
            "waits": copy.deepcopy(self.waits),
            "wait_spans": copy.deepcopy(self.wait_spans),
            "hold_spans": copy.deepcopy(self.hold_spans),
            "anomalies": copy.deepcopy(self.anomalies),
            "wid": self._wid,
        }

    def _restore(self, cp_index: int):
        import copy
        snap = copy.deepcopy(self._checkpoints[cp_index])
        self.threads = snap["threads"]
        self.locks = snap["locks"]
        self.frames = snap["frames"]
        self.waits = snap["waits"]
        self.wait_spans = snap["wait_spans"]
        self.hold_spans = snap["hold_spans"]
        self.anomalies = snap["anomalies"]
        self._wid = snap["wid"]
        self.next_i = cp_index

    # -- 图构建 ----------------------------------------------------------

    def _frame_stack(self, frame: Frame, kind: str) -> list:
        stack = frame.grant_stack if kind == "hold" else frame.req_stack
        return list(stack)

    def holding_graph(self) -> dict:
        nodes_t, nodes_l, edges = set(), set(), []
        for key, ls in self.locks.items():
            for thread, frame in ls.holders:
                nodes_t.add(thread)
                nodes_l.add(key)
                edges.append({
                    "source": thread,
                    "source_kind": "thread",
                    "target": key,
                    "target_kind": "lock",
                    "kind": "holds",
                    "mode": "U->X" if frame.upgrade else frame.mode,
                    "lock_type": ls.type,
                    "gen": ls.gen,
                    "lock": ls.lock,
                    "thread": thread,
                    "req_event": frame.req_event if frame.req_event >= 0 else None,
                    "event": frame.grant_event,
                    "events": [e for e in (frame.req_event, frame.grant_event) if e is not None and e >= 0],
                    "stack": list(frame.grant_stack),
                    "confidence": HIGH,
                    "suspended": False,
                })
        return {
            "nodes": sorted(nodes_t) + sorted(nodes_l),
            "threads": sorted(nodes_t),
            "locks": sorted(nodes_l),
            "edges": edges,
        }

    def waiting_graph(self) -> dict:
        nodes_t, nodes_l, edges = set(), set(), []
        for rec in self.waits.values():
            lk = rec.key
            nodes_t.add(rec.thread)
            nodes_l.add(lk)
            ls = self.locks.get(lk)
            blockers = self._blockers(ls, rec.thread, rec.mode) if ls else []
            holders = [(t, fr) for t, fr in ls.holders] if ls else []
            # 等待边：thread -> lock（回到产生等待的 acquire_req/cond_wait 事件）
            wait_kind = "cond_sleep" if rec.kind == "cond" else "wait"
            edges.append({
                "source": rec.thread,
                "source_kind": "thread",
                "target": lk,
                "target_kind": "lock",
                "kind": wait_kind,
                "mode": rec.mode,
                "lock_type": ls.type if ls else self.lock_types.get(lk, MUTEX),
                "gen": rec.gen,
                "lock": rec.lock,
                "thread": rec.thread,
                "cond": rec.cond,
                "event": rec.req_event,
                "events": [rec.req_event] + ([rec.cond_event] if rec.cond_event and rec.cond_event != rec.req_event else []),
                "stack": list(rec.req_stack),
                "confidence": LOW if rec.kind == "cond" else HIGH,
                "blocker_event": rec.blocker_event,
            })
            # 阻塞边：lock(被持有) -> waiting thread（每条边可回到持有者授予事件）
            for holder_thread, frame in blockers:
                nodes_t.add(holder_thread)
                edges.append({
                    "source": lk,
                    "source_kind": "lock",
                    "target": holder_thread,
                    "target_kind": "thread",
                    "kind": "held_by",
                    "mode": "U->X" if frame.upgrade else frame.mode,
                    "gen": rec.gen,
                    "lock": rec.lock,
                    "thread": holder_thread,
                    "waiter": rec.thread,
                    "event": frame.grant_event,
                    "events": [frame.grant_event],
                    "stack": list(frame.grant_stack),
                    "confidence": HIGH,
                })
        return {
            "nodes": sorted(nodes_t) + sorted(nodes_l),
            "threads": sorted(nodes_t),
            "locks": sorted(nodes_l),
            "edges": edges,
        }

    def wait_for_edges(self) -> list:
        """thread -> thread 等待边（用于死锁环检测；condition 睡眠边不参与）。"""
        out = []
        for rec in self.waits.values():
            if rec.kind == "cond":
                continue
            ls = self.locks.get(rec.key)
            if ls is None:
                continue
            blockers = self._blockers(ls, rec.thread, rec.mode)
            for holder_thread, frame in blockers:
                out.append({
                    "waiter": rec.thread,
                    "holder": holder_thread,
                    "gen": rec.gen,
                    "lock": rec.lock,
                    "mode": rec.mode,
                    "held_mode": "U->X" if frame.upgrade else frame.mode,
                    "wait_event": rec.req_event,
                    "wait_stack": list(rec.req_stack),
                    "hold_event": frame.grant_event,
                    "hold_stack": list(frame.grant_stack),
                    "confidence": HIGH,
                })
        return out

    # -- 环检测 ----------------------------------------------------------

    def detect_cycles(self, cap: int = 50) -> list:
        edges = self.wait_for_edges()
        adj: dict[str, list[dict]] = {}
        for e in edges:
            adj.setdefault(e["waiter"], []).append(e)
        cycles_raw = _enumerate_cycles(adj, cap=cap)
        result, seen = [], set()
        for cycle in cycles_raw:
            signature = _cycle_signature(cycle)
            if signature in seen:
                continue
            seen.add(signature)
            nodes = []
            locks_in_cycle = set()
            for e in cycle:
                locks_in_cycle.add(lkey(e["gen"], e["lock"]))
            involved = {e["waiter"] for e in cycle} | {e["holder"] for e in cycle}
            for e in cycle:
                t = e["waiter"]
                holds = []
                for lk in sorted(locks_in_cycle):
                    ls = self.locks.get(lk)
                    if not ls:
                        continue
                    for ht, frame in ls.holders:
                        if ht == t:
                            holds.append({
                                "gen": ls.gen,
                                "lock": ls.lock,
                                "mode": "U->X" if frame.upgrade else frame.mode,
                                "event": frame.grant_event,
                                "req_event": frame.req_event if frame.req_event >= 0 else None,
                                "stack": list(frame.grant_stack),
                            })
                nodes.append({
                    "thread": t,
                    "waits_for": e["holder"],
                    "wait": {
                        "gen": e["gen"],
                        "lock": e["lock"],
                        "mode": e["mode"],
                        "held_mode": e["held_mode"],
                        "event": e["wait_event"],
                        "stack": list(e["wait_stack"]),
                    },
                    "holds": holds,
                })
            result.append({
                "signature": signature,
                "length": len(cycle),
                "threads": sorted(involved),
                "nodes": nodes,
                "edges": [
                    {
                        "waiter": e["waiter"],
                        "holder": e["holder"],
                        "gen": e["gen"],
                        "lock": e["lock"],
                        "wait_event": e["wait_event"],
                        "hold_event": e["hold_event"],
                    }
                    for e in cycle
                ],
            })
        return result

    # -- 状态导出 --------------------------------------------------------

    def confidence(self) -> str:
        severities = {a.severity for a in self.anomalies}
        if HIGH in severities:
            return LOW
        if MEDIUM in severities:
            return MEDIUM
        return HIGH

    def thread_states(self) -> dict:
        out = {}
        for t, info in self.threads.items():
            out[t] = {
                "alive": info["alive"],
                "started": info.get("started", False),
                "holds": [
                    {
                        "gen": frame.gen,
                        "lock": frame.lock,
                        "mode": "U->X" if frame.upgrade else frame.mode,
                        "reentry": getattr(frame, "reentry", 1),
                        "event": frame.grant_event,
                        "suspended": frame.suspended,
                    }
                    for frame in self.frames.get(t, [])
                ],
                "waiting": [
                    {
                        "wait_id": rec.wait_id,
                        "gen": rec.gen,
                        "lock": rec.lock,
                        "mode": rec.mode,
                        "kind": rec.kind,
                        "cond": rec.cond,
                        "event": rec.req_event,
                    }
                    for rec in self.waits.values() if rec.thread == t
                ],
            }
        return out

    def lock_states(self) -> dict:
        out = {}
        for key, ls in self.locks.items():
            out[key] = {
                "gen": ls.gen,
                "lock": ls.lock,
                "type": ls.type,
                "holders": [
                    {
                        "thread": t,
                        "mode": "U->X" if fr.upgrade else fr.mode,
                        "reentry": getattr(fr, "reentry", 1),
                        "event": fr.grant_event,
                        "suspended": fr.suspended,
                    }
                    for t, fr in ls.holders
                ],
                "queue": [
                    {
                        "thread": rec.thread,
                        "mode": rec.mode,
                        "kind": rec.kind,
                        "cond": rec.cond,
                        "event": rec.req_event,
                        "blocker_event": rec.blocker_event,
                    }
                    for rec in ls.queue
                ],
            }
        return out

    def timeline(self) -> dict:
        return {
            "wait_spans": [
                {
                    "wait_id": s.wait_id,
                    "gen": s.gen,
                    "lock": s.lock,
                    "thread": s.thread,
                    "mode": s.mode,
                    "kind": s.kind,
                    "cond": s.cond,
                    "req_event": s.req_event,
                    "start_ts": s.start_ts,
                    "end_ts": s.end_ts,
                    "end_event": s.end_event,
                    "outcome": s.outcome,
                }
                for s in self.wait_spans.values()
            ],
            "hold_spans": [
                {
                    "gen": s.gen,
                    "lock": s.lock,
                    "thread": s.thread,
                    "mode": s.mode,
                    "grant_event": s.grant_event,
                    "start_ts": s.start_ts,
                    "end_ts": s.end_ts,
                    "end_event": s.end_event,
                }
                for s in self.hold_spans
            ],
        }

    def anomalies_public(self) -> list:
        return [
            {
                "code": a.code,
                "severity": a.severity,
                "message": a.message,
                "event": a.event,
                "gen": a.gen,
                "thread": a.thread,
                "lock": a.lock,
            }
            for a in self.anomalies
        ]

    def state_at(self, i: int) -> dict:
        self.run_to(i)
        hg = self.holding_graph()
        wg = self.waiting_graph()
        cycles = self.detect_cycles()
        return {
            "index": i,
            "total": len(self.events),
            "confidence": self.confidence(),
            "anomalies": self.anomalies_public(),
            "cycles": cycles,
            "holding_graph": hg,
            "waiting_graph": wg,
            "threads": self.thread_states(),
            "locks": self.lock_states(),
            "timeline": self.timeline(),
        }


def _enumerate_cycles(adj: dict, cap: int = 50) -> list:
    cycles = []
    nodes = sorted(set(adj) | {v["holder"] for vs in adj.values() for v in vs})

    def dfs(start: str, current: str, path: list, visited: set):
        if len(cycles) >= cap:
            return
        for edge in adj.get(current, []):
            nxt = edge["holder"]
            if nxt == start and len(path) >= 1:
                cycles.append(path + [edge])
                if len(cycles) >= cap:
                    return
            elif nxt not in visited and nxt > start:
                visited.add(nxt)
                dfs(start, nxt, path + [edge], visited)
                visited.discard(nxt)

    for start in nodes:
        dfs(start, start, [], {start})
        if len(cycles) >= cap:
            break
    return cycles


def _cycle_signature(cycle: list) -> str:
    pairs = []
    for e in cycle:
        pairs.append((e["waiter"], lkey(e["gen"], e["lock"]), e["holder"]))
    rotations = []
    n = len(pairs)
    for k in range(n):
        rotations.append(tuple(pairs[k:] + pairs[:k]))
    return "|".join(f"{a}->{lk}->{b}" for a, lk, b in min(rotations))


# ---------------------------------------------------------------------------
# 事件解析与候选重放
# ---------------------------------------------------------------------------

def make_event(raw: dict, seq: int, event_id: Optional[int] = None) -> Event:
    typ = normalize_type(raw.get("type", raw.get("kind", "")))
    ts = raw.get("ts", raw.get("timestamp", raw.get("time", 0.0)))
    if ts is None:
        ts = 0.0
    stack = raw.get("stack") or raw.get("stacktrace") or ()
    if isinstance(stack, str):
        stack = tuple(s.strip() for s in stack.split("\n") if s.strip())
    else:
        stack = tuple(str(s) for s in stack)
    return Event(
        id=raw.get("id", event_id),
        gen=str(raw.get("gen", raw.get("generation", "g0"))),
        ts=float(ts),
        seq=int(raw.get("seq") if raw.get("seq") is not None else seq),
        type=typ,
        thread=str(raw["thread"]),
        lock=(str(raw["lock"]) if raw.get("lock") is not None else None),
        mode=normalize_mode(raw.get("mode")),
        cond=(str(raw["cond"]) if raw.get("cond") is not None else None),
        notify_all=bool(raw.get("notify_all", typ == COND_NOTIFY and str(raw.get("mode", "")).lower() in {"all", "broadcast"})),
        stack=stack,
        raw=raw,
    )


def build_effective(events: list[Event], correction: Optional[dict] = None) -> list:
    """应用一条事件校正，返回有效事件列表（不修改原事件）。"""
    correction = correction or {}
    out: list[Event] = []
    drop_id = correction.get("drop_event")
    override = correction.get("override", {})
    for ev in events:
        if ev.id is not None and str(ev.id) == str(drop_id):
            continue
        if ev.id is not None and str(ev.id) in {str(k) for k in override}:
            patch = override[str(ev.id)]
            merged = dict(ev.raw)
            merged.update(patch)
            new_ev = make_event(merged, ev.seq, event_id=ev.id)
            out.append(new_ev)
        else:
            out.append(ev)
    for added in correction.get("insert", []):
        out.append(make_event(added, len(events) + len(out), added.get("id")))
    return out


def ambiguous_groups(events: list[Event]) -> list[list[int]]:
    """同代次同时间戳的事件组（乱序到达的候选排列来源）。"""
    ordered_events = ordered(events)
    groups, run = [], []
    prev = None
    for idx, ev in enumerate(ordered_events):
        key = (ev.gen, ev.ts)
        if prev is not None and key != prev:
            if len(run) > 1:
                groups.append(run)
            run = []
        run.append(idx)
        prev = key
    if len(run) > 1:
        groups.append(run)
    return groups


def replay_candidates(
    events: list[Event],
    index: Optional[int] = None,
    declared_types: Optional[dict] = None,
    max_candidates: int = 8,
) -> dict:
    """生成候选重放。包含原始顺序 + 同时间戳交换排列（去重）。"""
    ordered_events = ordered(events)
    index = len(ordered_events) if index is None else max(0, min(index, len(ordered_events)))
    groups = ambiguous_groups(ordered_events)
    orderings = [ordered_events]
    if groups:
        perms = _group_permutations(groups, max_candidates)
        for perm in perms:
            rebuilt = list(ordered_events)
            # perm 是 {组内 idx: 新位置的事件}
            for group, mapping in perm.items():
                sub = [ordered_events[j] for j in group]
                for pos, src in enumerate(mapping):
                    rebuilt[group[pos]] = sub[src]
            orderings.append(rebuilt)
    candidates, seen_hashes = [], set()
    ambiguous = bool(groups)
    for n, ords in enumerate(orderings):
        rep = Replayer(ords[:index], declared_types=declared_types)
        rep.run_all()
        state = rep.state_at(index)
        digest = _state_digest(state)
        if digest in seen_hashes:
            continue
        seen_hashes.add(digest)
        state["candidate_index"] = len(candidates)
        state["ordering"] = n
        state["ambiguous_groups"] = [
            [ordered_events[j].to_public() for j in g] for g in groups
        ]
        if ambiguous and n > 0:
            rep.anomalies.append(
                Anomaly("ambiguous_order", None, severity=LOW,
                        message="存在同时间戳事件，本候选采用另一种合法到达顺序",
                        event=None, gen=None, thread=None, lock=None)
            )
            state["anomalies"] = rep.anomalies_public()
            state["confidence"] = rep.confidence()
        candidates.append(state)
    canonical = max(
        candidates,
        key=lambda s: (s["confidence"] == HIGH, s["confidence"] == MEDIUM, -len(s["anomalies"])),
    )
    canonical["canonical"] = True
    return {
        "canonical_index": canonical["candidate_index"],
        "ambiguous": ambiguous,
        "count": len(candidates),
        "candidates": candidates,
    }


def _group_permutations(groups: list[list[int]], cap: int) -> list:
    results = []
    # 为控制爆炸，对每组最多取若干排列，整体不超过 cap 个
    options_per_group = []
    for g in groups[:3]:
        perms = list(itertools.permutations(range(len(g))))
        perms = [p for p in perms if p != tuple(range(len(g)))]
        options_per_group.append(perms[: max(1, cap - 1)])
    if not options_per_group:
        return results
    for combo in itertools.product(*options_per_group):
        mapping = {}
        for gi, perm in zip(range(len(options_per_group)), combo):
            mapping[tuple(groups[gi])] = perm
        results.append(mapping)
        if len(results) >= cap - 1:
            break
    return results


def _state_digest(state: dict) -> str:
    import hashlib
    import json
    payload = {
        "cycles": [c["signature"] for c in state["cycles"]],
        "holders": sorted(
            (lk, h["thread"], h["mode"])
            for lk, info in state["locks"].items()
            for h in info["holders"]
        ),
        "waiting": sorted(
            (lk, q["thread"], q["kind"])
            for lk, info in state["locks"].items()
            for q in info["queue"]
        ),
        "anomalies": sorted((a["code"], str(a["event"]), a["thread"], a["lock"]) for a in state["anomalies"]),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


# ---------------------------------------------------------------------------
# 最小事件片段（保持每线程配对与 condition 语义）
# ---------------------------------------------------------------------------

def build_units(events: list[Event]) -> list[list[Event]]:
    """把事件切分为不可拆散的配对单元：
    - 线程起止各自成单元
    - 同一线程同一把锁的 request -> grant/cond -> release 成一个单元
      （递归锁与读写锁按栈配对，release 关闭栈顶单元）
    - condition wait/notify/acquire/cancel 依附在对应锁单元上
    """
    ordered_events = ordered(events)
    stacks: dict[tuple, list[list[Event]]] = {}
    units: list[list[Event]] = []

    def top(key):
        st = stacks.get(key)
        return st[-1] if st else None

    for ev in ordered_events:
        key = (ev.thread, ev.lock) if ev.lock is not None else None
        if ev.type in (THREAD_START, THREAD_END):
            units.append([ev])
            continue
        if ev.type == ACQUIRE_REQ and key:
            st = stacks.setdefault(key, [])
            if st:
                # 递归获取：并入当前单元保持配对
                st[-1].append(ev)
            else:
                unit = [ev]
                units.append(unit)
                st.append(unit)
            continue
        unit = top(key) if key else None
        if unit is not None and ev.type in (ACQUIRED, COND_WAIT, COND_WAITED,
                                            COND_ACQUIRED, CANCELLED, RELEASED):
            unit.append(ev)
            if ev.type == RELEASED:
                st = stacks.get(key)
                if st:
                    st.pop()
            continue
        if ev.type == COND_NOTIFY and key:
            unit = top(key)
            if unit is not None:
                unit.append(ev)
                continue
        # 无法归属的孤立事件单独成单元
        units.append([ev])
    return units


def cycle_present(event_list: list[Event], target: Optional[str],
                  declared_types: Optional[dict] = None) -> tuple[bool, Optional[str]]:
    result = replay_candidates(event_list, declared_types=declared_types, max_candidates=4)
    for cand in result["candidates"]:
        for cycle in cand["cycles"]:
            if target is None or cycle["signature"] == target:
                return True, cycle["signature"]
    return False, None


def minimize_events(events: list[Event], target_signature: Optional[str] = None,
                    declared_types: Optional[dict] = None) -> dict:
    """ddmin：在保持单元配对与全局顺序的前提下，缩减出仍复现同一环的最小片段。"""
    units = build_units(events)

    def flatten(unit_indexes) -> list[Event]:
        chosen = [u for k, u in enumerate(units) if k in unit_indexes]
        out: list[Event] = []
        for u in chosen:
            out.extend(u)
        return ordered(out)

    full = flatten(range(len(units)))
    if target_signature is None:
        ok, target_signature = cycle_present(full, None, declared_types)
        if not ok:
            return {
                "found": False,
                "target": None,
                "event_ids": [e.id for e in full if e.id is not None],
                "events": [e.to_public() for e in full],
                "unit_count": len(units),
                "kept_units": len(units),
            }

    current = set(range(len(units)))

    def reproduces(indexes) -> bool:
        ok, _ = cycle_present(flatten(indexes), target_signature, declared_types)
        return ok

    n = 2
    while len(current) >= 2:
        items = sorted(current)
        chunk = max(1, len(items) // n)
        subsets = [set(items[i:i + chunk]) for i in range(0, len(items), chunk)]
        reduced = False
        for sub in subsets:
            trial = current - sub
            if trial and reproduces(trial):
                current = trial
                n = max(n - 1, 2)
                reduced = True
                break
        if reduced:
            continue
        if n < len(current):
            n = min(n * 2, len(current))
            continue
        # 补元到 2 的 ddmin：尝试单个单元删除
        any_single = False
        for idx in list(current):
            trial = current - {idx}
            if trial and reproduces(trial):
                current = trial
                any_single = True
                break
        if not any_single:
            break

    kept = flatten(current)
    return {
        "found": True,
        "target": target_signature,
        "event_ids": [e.id for e in kept if e.id is not None],
        "events": [e.to_public() for e in kept],
        "unit_count": len(units),
        "kept_units": len(current),
        "removed_events": len(events) - len(kept),
    }
