"""确定性事件回放引擎。

输入规范化事件流（已按代次、时间戳、到达序号排序），逐事件维护：

* 持有图：线程 -> 锁 的持有边（含栈与产生它的 acquired 事件）；
* 等待图：等待线程 -> 持有者线程，边上挂所等待的锁、等待栈与请求事件；
* condition 等待、锁顺序边、配对区间、异常与置信状态。

引擎不凭空补 release：缺失配对只会产生带 ``low``/``medium`` 置信度的状态与异常。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .model import (
    ACQUIRED, ACQ_REQUEST, CANCEL, COND_NOTIFY, COND_NOTIFY_ALL,
    COND_TIMEOUT, COND_WAIT, EXCLUSIVE, HIGH, LOW, MEDIUM, MUTEX,
    READ, RECURSIVE, RELEASED, RWLOCK, T_END, T_START, TIMEOUT, UPGRADE,
    WRITE, Event,
)


@dataclass
class Hold:
    """线程对一把锁的一次持有（重入时 depth>1）。"""
    thread: str
    lock: str
    mode: str                 # shared | exclusive
    depth: int = 1
    read_depth: int = 1       # 共享持有时的读层数
    acquire_event: int = 0
    release_event: Optional[int] = None
    hold_stack: Optional[list] = None
    confidence: str = HIGH


@dataclass
class Waiter:
    """一次未完成或已完成的锁等待。"""
    thread: str
    lock: str
    mode: str                 # read | write | upgrade
    request_event: int
    wait_stack: Optional[list] = None
    since_ts: float = 0.0
    kind: str = "lock"        # lock | reacquire（condition 后重新竞争）
    cv: Optional[str] = None
    cond_wait_event: Optional[int] = None
    granted_event: Optional[int] = None
    end_event: Optional[int] = None
    end_reason: Optional[str] = None   # granted | timeout | cancel | thread_end
    confidence: str = HIGH
    inferred: bool = False


@dataclass
class CVWait:
    """一次 condition wait：等待期间锁已暂时释放。"""
    thread: str
    cv: str
    lock: str
    mode: str
    cond_event: int
    wait_stack: Optional[list]
    since_ts: float
    notified_event: Optional[int] = None
    end_event: Optional[int] = None
    end_reason: Optional[str] = None   # notified | timeout | cancel | thread_end
    waiter_id: Optional[int] = None    # 重新竞争锁的 Waiter id
    confidence: str = HIGH


@dataclass
class Interval:
    kind: str                 # hold | wait | cond_wait
    thread: str
    lock: Optional[str]
    cv: Optional[str]
    start_event: int
    start_ts: float
    end_event: Optional[int] = None
    end_ts: Optional[float] = None
    mode: Optional[str] = None
    confidence: str = HIGH


@dataclass
class Anomaly:
    code: str
    message: str
    event_id: int
    generation: str
    thread: str = ""
    lock: Optional[str] = None
    confidence: str = MEDIUM


def _edge_id(a: str, b: str, lock: str, req: int) -> str:
    return f"{a}->{b}|{lock}|{req}"


class GenState:
    """单个采集代次的回放状态。"""

    def __init__(self, generation: str):
        self.generation = generation

        # 锁状态
        self.holders: dict[str, list[Hold]] = {}     # lock -> 当前持有者
        self.waiters: dict[str, list[Waiter]] = {}   # lock -> FIFO 等待者
        self.cv_waits: dict[str, list[CVWait]] = {}  # cv -> 当前等待者
        self.lock_type: dict[str, str] = {}          # lock -> 已确定类型
        self.lock_type_ev: dict[str, int] = {}

        # 线程状态
        self.thread_locks: dict[str, list[Hold]] = {}  # thread -> 持有栈（LIFO）
        self.alive: dict[str, bool] = {}

        # 等待边快照：waiter_id -> edge dict（每步重建以反映持有者变化）
        # 区间与历史
        self.intervals: list[Interval] = []
        self.waiter_log: list[Waiter] = []
        self.cv_log: list[CVWait] = []
        self.hold_log: list[Hold] = []
        self.anomalies: list[Anomaly] = []

        # 锁顺序：(thread, first_lock, second_lock) 第一次嵌套获取
        self.lock_order: dict[tuple[str, str, str], int] = {}
        # (lock_a, lock_b) -> 出现过的线程与事件，用于检测反向近失
        self.order_edges: dict[tuple[str, str], dict] = {}

        # 配对最小化用：episode 栈 [(event_ids...)]，键 (thread, lock)
        self.ep_stack: dict[tuple[str, str], list[list[int]]] = {}
        # 单个可独立删除的事件
        self.singletons: list[int] = []

        self._wid = 0
        self._events_by_id: dict[int, Event] = {}

    # -- 小工具 -------------------------------------------------------------

    def _new_wid(self) -> int:
        self._wid += 1
        return self._wid

    def current_holds(self, thread: str) -> list[Hold]:
        return [h for h in self.thread_locks.get(thread, [])]

    def find_hold(self, thread: str, lock: str) -> Optional[Hold]:
        for h in self.thread_locks.get(thread, []):
            if h.lock == lock:
                return h
        return None

    def active_waiters(self, lock: str) -> list[Waiter]:
        return [w for w in self.waiters.get(lock, []) if w.end_event is None]

    def active_cv(self, cv: str) -> list[CVWait]:
        return [c for c in self.cv_waits.get(cv, []) if c.end_event is None]

    def infer_type(self, ev: Event) -> str:
        """根据用法推断锁类型；显式声明在引擎外覆盖。"""
        if ev.lock and ev.lock in self.lock_type:
            return self.lock_type[ev.lock]
        if ev.mode in (READ, WRITE, UPGRADE):
            lt = RWLOCK
        elif ev.type == ACQ_REQUEST and ev.mode == EXCLUSIVE:
            lt = MUTEX
        else:
            lt = MUTEX
        if ev.lock:
            self.lock_type[ev.lock] = lt
            self.lock_type_ev[ev.lock] = ev.id
        return lt

    def set_explicit_type(self, lock: str, lock_type: str) -> None:
        self.lock_type[lock] = lock_type

    def anomaly(self, code: str, message: str, ev: Event,
                confidence: str = MEDIUM, lock: Optional[str] = None) -> None:
        self.anomalies.append(Anomaly(
            code=code, message=message, event_id=ev.id,
            generation=self.generation, thread=ev.thread,
            lock=lock or ev.lock, confidence=confidence))

    def _push_episode(self, thread: str, lock: str, ev_id: int) -> list[int]:
        key = (thread, lock)
        ep = [ev_id]
        self.ep_stack.setdefault(key, []).append(ep)
        return ep

    def _top_episode(self, thread: str, lock: str) -> Optional[list[int]]:
        stack = self.ep_stack.get((thread, lock))
        return stack[-1] if stack else None

    def _pop_episode(self, thread: str, lock: str) -> list[int]:
        stack = self.ep_stack.get((thread, lock))
        return stack.pop() if stack else []

    def _singleton(self, ev_id: int) -> None:
        self.singletons.append(ev_id)

    # -- 授权判定 -----------------------------------------------------------

    def _grantable(self, lock: str, waiter: Waiter) -> tuple[bool, bool]:
        """返回 (可否授权, 是否被其他持有者阻挡)。

        忽略自身读持有（升级由 :meth:`_upgrade_blocked` 专门处理）。
        """
        others = [h for h in self.holders.get(lock, [])
                  if h.thread != waiter.thread]
        if waiter.mode == READ:
            blocked_by_write = any(h.mode == EXCLUSIVE for h in others)
            return (not blocked_by_write, blocked_by_write)
        # write / upgrade：任何他人持有都阻挡
        return (len(others) == 0, len(others) > 0)

    def _upgrade_blocked(self, lock: str, waiter: Waiter) -> bool:
        """升级请求还被其他读持有者阻挡（自身读不算）。"""
        return any(h.thread != waiter.thread for h in self.holders.get(lock, []))

    def active_edges(self) -> list[dict]:
        """当前时点的等待边：等待线程 -> 阻挡它的持有者线程。"""
        edges: dict[str, dict] = []
        seen: set[str] = set()
        for lock, ws in self.waiters.items():
            for w in ws:
                if w.end_event is not None:
                    continue
                if w.mode == UPGRADE and self.find_hold(w.thread, lock):
                    # 有自身读持有；他人持有才构成环边
                    blockers = [h for h in self.holders.get(lock, [])
                                if h.thread != w.thread]
                else:
                    if w.mode == READ:
                        blockers = [h for h in self.holders.get(lock, [])
                                    if h.mode == EXCLUSIVE]
                    else:
                        blockers = [h for h in self.holders.get(lock, [])
                                    if h.thread != w.thread]
                for h in blockers:
                    eid = _edge_id(w.thread, h.thread, lock, w.request_event)
                    if eid in seen:
                        continue
                    seen.add(eid)
                    conf = HIGH
                    if w.inferred or h.confidence != HIGH:
                        conf = LOW
                    edges.append({
                        "id": eid,
                        "waiter": w.thread,
                        "holder": h.thread,
                        "lock": lock,
                        "mode": w.mode,
                        "kind": w.kind,
                        "cv": w.cv,
                        "request_event": w.request_event,
                        "wait_stack": w.wait_stack,
                        "hold_event": h.acquire_event,
                        "hold_stack": h.hold_stack,
                        "confidence": conf,
                        "waiter_id": id(w),
                    })
        return edges

    # -- 事件处理 -----------------------------------------------------------

    def apply(self, ev: Event) -> None:
        self._events_by_id[ev.id] = ev
        handler = {
            T_START: self._ev_start,
            T_END: self._ev_end,
            ACQ_REQUEST: self._ev_request,
            ACQUIRED: self._ev_acquired,
            RELEASED: self._ev_released,
            TIMEOUT: self._ev_cancel,
            CANCEL: self._ev_cancel,
            COND_WAIT: self._ev_cond_wait,
            COND_NOTIFY: self._ev_notify,
            COND_NOTIFY_ALL: self._ev_notify,
            COND_TIMEOUT: self._ev_cond_cancel,
        }.get(ev.type)
        if handler:
            handler(ev)

    def _ev_start(self, ev: Event) -> None:
        self.alive[ev.thread] = True
        self._singleton(ev.id)

    def _ev_end(self, ev: Event) -> None:
        self.alive[ev.thread] = False
        # 线程终止撤销该线程所有等待
        for lock, ws in self.waiters.items():
            for w in ws:
                if w.thread == ev.thread and w.end_event is None:
                    w.end_event = ev.id
                    w.end_reason = "thread_end"
                    self._close_interval(w, ev, "thread_end")
        # 撤销 condition 等待
        for cv, cs in self.cv_waits.items():
            for c in cs:
                if c.thread == ev.thread and c.end_event is None:
                    c.end_event = ev.id
                    c.end_reason = "thread_end"
        left = [h.lock for h in self.current_holds(ev.thread)]
        if left:
            self.anomaly(
                "thread_exit_with_holds",
                f"线程 {ev.thread} 终止时仍持有锁 {left}，等待与持有状态按终止保留"
                "为低置信（不凭空补 release）", ev, LOW)
        for hold in left:
            h = self.find_hold(ev.thread, hold)
            if h:
                h.confidence = LOW
                h.release_event = ev.id
                self.holders[hold] = [x for x in self.holders[hold]
                                      if x is not h]
                self.thread_locks[ev.thread] = [
                    x for x in self.thread_locks[ev.thread] if x is not h]
                self._close_hold_interval(h, ev, LOW)
        self._singleton(ev.id)

    def _mode_for(self, ev: Event) -> str:
        return ev.mode if self.infer_type(ev) == RWLOCK else EXCLUSIVE

    def _ev_request(self, ev: Event, kind: str = "lock",
                    cv: Optional[str] = None,
                    cond_wait_event: Optional[int] = None,
                    confidence: str = HIGH,
                    inferred: bool = False) -> Waiter:
        mode = self._mode_for(ev)
        waiter = Waiter(
            thread=ev.thread, lock=ev.lock, mode=mode,
            request_event=ev.id, wait_stack=ev.wait_stack or ev.stack,
            since_ts=ev.ts, kind=kind, cv=cv,
            cond_wait_event=cond_wait_event,
            confidence=confidence, inferred=inferred)
        self.waiters.setdefault(ev.lock, []).append(waiter)
        self.waiter_log.append(waiter)
        self.intervals.append(Interval(
            kind="wait", thread=ev.thread, lock=ev.lock, cv=cv,
            start_event=ev.id, start_ts=ev.ts, mode=mode,
            confidence=confidence))
        if kind == "lock":
            # 若该线程刚被 notify 唤醒、等待重新竞争同一把锁，归入该 condition episode
            linked = self._link_notified(waiter)
            if not linked:
                self._push_episode(ev.thread, ev.lock, ev.id)
        return waiter

    def _link_notified(self, waiter: Waiter) -> bool:
        """把一个显式重竞争请求关联到已通知的 condition 等待。"""
        for cs in self.cv_waits.values():
            for c in cs:
                if (c.thread == waiter.thread and c.lock == waiter.lock
                        and c.end_reason == "notified"
                        and c.waiter_id is None):
                    waiter.kind = "reacquire"
                    waiter.cv = c.cv
                    waiter.cond_wait_event = c.cond_event
                    waiter.confidence = waiter.confidence
                    c.waiter_id = id(waiter)
                    return True
        return False

    def _ev_acquired(self, ev: Event, waiter: Optional[Waiter] = None,
                     confidence: str = HIGH) -> Hold:
        mode = self._mode_for(ev)
        existing = self.find_hold(ev.thread, ev.lock)
        if existing is not None:
            # 重入
            existing.depth += 1
            if mode == READ:
                existing.read_depth += 1
            else:
                existing.mode = EXCLUSIVE
            if waiter is not None:
                waiter.end_event = ev.id
                waiter.end_reason = "granted"
                waiter.granted_event = ev.id
                self._close_interval(waiter, ev, "granted")
            return existing

        # 找到匹配的活动等待者（优先 reacquire，其次普通）
        if waiter is None:
            for w in self.active_waiters(ev.lock):
                if w.thread == ev.thread and w.kind == "reacquire":
                    waiter = w
                    break
        if waiter is None:
            for w in self.active_waiters(ev.lock):
                if w.thread == ev.thread:
                    waiter = w
                    break
        notified = waiter is None
        inferred = waiter is None or waiter.inferred
        if waiter is None:
            # 缺 acquire_request：不能凭空补，只能以低/中置信记录推断等待
            waiter = self._ev_request(
                ev, confidence=MEDIUM, inferred=True)
            self._link_notified(waiter)
            if waiter.kind != "reacquire":
                self.anomaly(
                    "missing_acquire_request",
                    f"线程 {ev.thread} 获取锁 {ev.lock} 前缺少 acquire_request，"
                    "已按到达即完成推断（低置信）", ev, LOW)
            waiter.end_event = ev.id
            waiter.end_reason = "granted"
            waiter.granted_event = ev.id
            self.anomaly(
                "missing_acquire_request",
                f"线程 {ev.thread} 获取锁 {ev.lock} 前缺少 acquire_request，"
                "已按到达即完成推断（低置信）", ev, LOW)
            self._close_interval(waiter, ev, "granted")
        else:
            waiter.end_event = ev.id
            waiter.end_reason = "granted"
            waiter.granted_event = ev.id
            self._close_interval(waiter, ev, "granted")

        hold_mode = SHARED if mode == READ else EXCLUSIVE
        hold = Hold(
            thread=ev.thread, lock=ev.lock, mode=hold_mode, depth=1,
            read_depth=1 if mode == READ else 0,
            acquire_event=ev.id,
            hold_stack=ev.hold_stack or ev.stack,
            confidence=LOW if inferred else HIGH)
        self.holders.setdefault(ev.lock, []).append(hold)
        self.thread_locks.setdefault(ev.thread, []).append(hold)
        self.hold_log.append(hold)
        self.intervals.append(Interval(
            kind="hold", thread=ev.thread, lock=ev.lock, cv=None,
            start_event=ev.id, start_ts=ev.ts, mode=hold_mode,
            confidence=hold.confidence))

        # 锁顺序边：获取该锁时线程仍持有的其它锁 -> 当前锁
        for outer in self.current_holds(ev.thread):
            if outer.lock == ev.lock:
                continue
            self._record_order(ev.thread, outer.lock, ev.lock, ev)

        # 关联 episode：reacquire 归入原 cond episode；lock 归入最顶层 episode
        if waiter.kind == "reacquire":
            ep = self._top_episode(ev.thread, ev.lock)
            if ep is None:
                ep = self._push_episode(ev.thread, ev.lock, ev.id)
            ep.append(ev.id)
        else:
            ep = self._top_episode(ev.thread, ev.lock)
            if ep is None:
                ep = self._push_episode(ev.thread, ev.lock, ev.id)
            ep.append(ev.id)
        return hold

    def _record_order(self, thread: str, a: str, b: str, ev: Event) -> None:
        key = (thread, a, b)
        self.lock_order.setdefault(key, ev.id)
        oe = self.order_edges.setdefault(
            (a, b), {"threads": set(), "event": ev.id})
        oe["threads"].add(thread)

    def _close_interval(self, waiter: Waiter, ev: Event,
                        reason: str) -> None:
        for itv in reversed(self.intervals):
            if (itv.kind == "wait" and itv.thread == waiter.thread
                    and itv.lock == waiter.lock and itv.end_event is None
                    and itv.start_event == waiter.request_event):
                itv.end_event = ev.id
                itv.end_ts = ev.ts
                return

    def _close_hold_interval(self, hold: Hold, ev: Event,
                             confidence: str = HIGH) -> None:
        for itv in reversed(self.intervals):
            if (itv.kind == "hold" and itv.thread == hold.thread
                    and itv.lock == hold.lock and itv.end_event is None
                    and itv.start_event == hold.acquire_event):
                itv.end_event = ev.id
                itv.end_ts = ev.ts
                itv.confidence = confidence
                return

    def _ev_released(self, ev: Event) -> None:
        hold = self.find_hold(ev.thread, ev.lock)
        if hold is None:
            self.anomaly(
                "unmatched_release",
                f"线程 {ev.thread} 释放了未持有的锁 {ev.lock}，已忽略"
                "（不凭空补 release）", ev, MEDIUM)
            return
        # 读写锁读持有按一次 release 退一层
        if hold.mode == SHARED and hold.read_depth > 1:
            hold.read_depth -= 1
            ep = self._top_episode(ev.thread, ev.lock)
            if ep is not None:
                ep.append(ev.id)
            return
        # 完全释放
        hold.release_event = ev.id
        self.holders[ev.lock] = [h for h in self.holders.get(ev.lock, [])
                                 if h is not hold]
        self.thread_locks[ev.thread] = [
            h for h in self.thread_locks[ev.thread] if h is not hold]
        self._close_hold_interval(hold, ev)
        ep = self._pop_episode(ev.thread, ev.lock)
        ep.append(ev.id)

    def _find_active_waiter(self, thread: str, lock: str,
                            kind: Optional[str] = None
                            ) -> Optional[Waiter]:
        for w in self.active_waiters(lock):
            if w.thread == thread and (kind is None or w.kind == kind):
                return w
        return None

    def _ev_cancel(self, ev: Event) -> None:
        waiter = self._find_active_waiter(ev.thread, ev.lock)
        if waiter is None:
            self.anomaly(
                "cancel_without_wait",
                f"线程 {ev.thread} 的超时/取消事件找不到活动等待 {ev.lock}",
                ev, LOW)
            self._singleton(ev.id)
            return
        waiter.end_event = ev.id
        waiter.end_reason = "timeout" if ev.type == TIMEOUT else "cancel"
        self._close_interval(waiter, ev, waiter.end_reason)
        # 从等待队列移除，撤销等待边
        self.waiters[ev.lock] = [
            w for w in self.waiters[ev.lock]
            if not (w.thread == ev.thread and w.lock == ev.lock
                    and w.end_event is not None
                    and w.request_event == waiter.request_event)]
        # timeout/cancel 与它对应的 request 同在一个 episode
        ep = self._top_episode(ev.thread, ev.lock)
        if ep is None:
            self._singleton(ev.id)
        else:
            ep.append(ev.id)
            self._pop_episode(ev.thread, ev.lock)

    def _ev_cond_wait(self, ev: Event) -> None:
        """condition wait：暂时释放锁，加入 cv 等待。

        事件序列约定：... acquired(L), cond_wait(cv, L) ... cond_notify(cv),
        acquire_request(L, kind=reacquire), acquired(L) ... released(L)
        cond_wait 本身只负责“暂时释放 + 挂到 cv”。
        """
        hold = self.find_hold(ev.thread, ev.lock) if ev.lock else None
        mode = hold.mode if hold else EXCLUSIVE
        if hold is None:
            self.anomaly(
                "cond_wait_without_hold",
                f"线程 {ev.thread} 在未持有锁 {ev.lock} 时进入 cond_wait，"
                "按低置信记录", ev, LOW)
        cvw = CVWait(
            thread=ev.thread, cv=ev.cv, lock=ev.lock,
            mode=mode, cond_event=ev.id,
            wait_stack=ev.wait_stack or ev.stack, since_ts=ev.ts,
            confidence=HIGH if hold else LOW)
        self.cv_waits.setdefault(ev.cv, []).append(cvw)
        self.cv_log.append(cvw)
        self.intervals.append(Interval(
            kind="cond_wait", thread=ev.thread, lock=ev.lock, cv=ev.cv,
            start_event=ev.id, start_ts=ev.ts, mode=mode,
            confidence=cvw.confidence))
        # 暂时释放锁（真实释放语义，但不是 acquired 的配对 release）
        if hold is not None:
            self.holders[ev.lock] = [
                h for h in self.holders.get(ev.lock, []) if h is not hold]
            self.thread_locks[ev.thread] = [
                h for h in self.thread_locks[ev.thread] if h is not hold]
            self._close_hold_interval(hold, ev, HIGH)
            # 注意：hold 保留在 hold_log，reacquire 后会新建 hold
        ep = self._top_episode(ev.thread, ev.lock) if ev.lock else None
        if ep is None and ev.lock:
            ep = self._push_episode(ev.thread, ev.lock, ev.id)
        elif ep is not None:
            ep.append(ev.id)
        if ev.lock and ep is None:
            self._singleton(ev.id)

    def _ev_notify(self, ev: Event) -> None:
        """signal / broadcast：唤醒 cv 上等待者，它们随后重新竞争锁。

        唤醒时若锁空闲（例如通知线程已释放），也不直接构造成功获取——
        必须由后续显式 acquire_request + acquired 完成，不凭空补事件。
        """
        cvs = self.cv_waits.get(ev.cv, [])
        targets = [c for c in cvs if c.end_event is None]
        if not targets:
            self.anomaly(
                "notify_without_waiter",
                f"通知 {ev.cv} 时没有活动 condition 等待者", ev, LOW)
        if ev.type == COND_NOTIFY:
            targets = targets[:1]
        for c in targets:
            c.notified_event = ev.id
            c.end_event = ev.id
            c.end_reason = "notified"
            self._close_cond_interval(c, ev)
            # 唤醒后重新竞争由后续显式 acquire_request/acquired 完成；
            # 若只有 acquired，引擎在那时以中置信推断 reacquire。
        self._singleton(ev.id)

    def _close_cond_interval(self, c: CVWait, ev: Event) -> None:
        for itv in reversed(self.intervals):
            if (itv.kind == "cond_wait" and itv.thread == c.thread
                    and itv.cv == c.cv and itv.end_event is None
                    and itv.start_event == c.cond_event):
                itv.end_event = ev.id
                itv.end_ts = ev.ts
                return

    def _ev_cond_cancel(self, ev: Event) -> None:
        # cond_timeout：撤销 cv 等待以及可能已存在的 reacquire 等待
        found = None
        for cv, cs in self.cv_waits.items():
            for c in cs:
                if (c.thread == ev.thread and c.end_event is None
                        and (ev.lock is None or c.lock == ev.lock)):
                    found = c
                    break
            if found:
                break
        if found:
            found.end_event = ev.id
            found.end_reason = "timeout"
            self._close_cond_interval(found, ev)
        waiter = None
        if ev.lock:
            waiter = self._find_active_waiter(
                ev.thread, ev.lock, kind="reacquire")
            if waiter is not None:
                waiter.end_event = ev.id
                waiter.end_reason = "timeout"
                self._close_interval(waiter, ev, "timeout")
                self.waiters[ev.lock] = [
                    w for w in self.waiters[ev.lock] if w is not waiter]
        if found is None and waiter is None:
            self.anomaly(
                "cond_timeout_without_wait",
                f"线程 {ev.thread} 的 cond_timeout 找不到活动 condition 等待",
                ev, LOW)
        self._singleton(ev.id)
