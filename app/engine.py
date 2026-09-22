"""锁序重建引擎。

输入是已经规范化的事件（按采集序号到达），输出是每个时间点的
“持有图 / 等待图”视图。引擎是纯函数式折叠：每个事件之后产出一个
不可变快照，分支校正通过重新折叠实现。

设计要点：
- 线程每把锁维护 level 栈，支持可重入锁与读写锁共享/升级；
- condition wait 暂时释放锁并登记 condition 等待；notify 后重新竞争，
  记录“恢复深度”，重获成功后一次性还原原持有栈；
- 超时（显式 timeout 事件或 deadline 到期）与线程结束都会撤销等待，
  从不凭空补 release；
- 缺事件/乱序只影响置信度与候选标注，不伪造边。
"""
from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

# 严重度
OK = "ok"
WARN = "warn"
ERROR = "error"
CONFIDENCE = {OK: "high", WARN: "medium", ERROR: "low"}


class Level:
    """一次成功获取（重入一次产生一个 level）。"""

    __slots__ = ("mode", "acquired", "restored_at")

    def __init__(self, mode: str, acquired: Dict[str, Any]):
        self.mode = mode
        self.acquired = acquired
        self.restored_at: Optional[Dict[str, Any]] = None

    def as_hold(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "acquired_event": self.acquired["id"],
            "acquired_ts": self.acquired["ts"],
            "restored_event": self.restored_at["id"] if self.restored_at else None,
        }


class WaitRec:
    """一条活动等待（锁等待 或 condition 等待后的重竞争）。"""

    __slots__ = (
        "kind",
        "lock",
        "mode",
        "request",
        "upgrading",
        "cond",
        "cond_wait",
        "restore_levels",
        "deadline",
        "cancelled",
        "cancel_reason",
    )

    def __init__(
        self,
        kind: str,
        lock: Optional[str],
        mode: str,
        request: Dict[str, Any],
        upgrading: bool = False,
        cond: Optional[str] = None,
        cond_wait: Optional[Dict[str, Any]] = None,
        restore_levels: Optional[List[Level]] = None,
        deadline: Optional[float] = None,
    ):
        self.kind = kind  # "lock" | "cond"
        self.lock = lock
        self.mode = mode
        self.request = request
        self.upgrading = upgrading
        self.cond = cond
        self.cond_wait = cond_wait
        self.restore_levels = restore_levels
        self.deadline = deadline
        self.cancelled = False
        self.cancel_reason: Optional[str] = None


class ThreadState:
    __slots__ = ("tid", "started", "ended", "start_event", "levels", "waiting", "last_event")

    def __init__(self, tid: str):
        self.tid = tid
        self.started = False
        self.ended = False
        self.start_event: Optional[Dict[str, Any]] = None
        self.levels: Dict[str, List[Level]] = {}
        self.waiting: Optional[WaitRec] = None
        self.last_event: Optional[Dict[str, Any]] = None


class LockState:
    __slots__ = ("address", "declared_kind", "inferred_reentrant", "inferred_shared")

    def __init__(self, address: str, declared_kind: Optional[str] = None):
        self.address = address
        self.declared_kind = declared_kind
        self.inferred_reentrant = False
        self.inferred_shared = False

    def effective_kind(self) -> str:
        if self.declared_kind:
            return self.declared_kind
        if self.inferred_shared:
            return "rwlock"
        if self.inferred_reentrant:
            return "reentrant"
        return "mutex"


class Reconstructor:
    """把事件流折叠成快照序列。"""

    def __init__(
        self,
        events: Optional[List[Dict[str, Any]]] = None,
        lock_declarations: Optional[Dict[str, str]] = None,
    ):
        self.threads: Dict[str, ThreadState] = {}
        self.locks: Dict[str, LockState] = {}
        self.anomalies: List[Dict[str, Any]] = []
        self.snapshots: List[Dict[str, Any]] = []
        self.events: List[Dict[str, Any]] = []
        self.lock_declarations: Dict[str, str] = dict(lock_declarations or {})
        self.rebuild_count = 0
        self._max_ts = None
        for address, kind in self.lock_declarations.items():
            self.locks[address] = LockState(address, kind)
        for event in events or []:
            self.append(event)

    # ------------------------------------------------------------------ utils

    def _thread(self, tid: str) -> ThreadState:
        state = self.threads.get(tid)
        if state is None:
            state = ThreadState(tid)
            self.threads[tid] = state
        return state

    def _lock(self, address: str) -> LockState:
        state = self.locks.get(address)
        if state is None:
            state = LockState(address, self.lock_declarations.get(address))
            self.locks[address] = state
        return state

    def _anomaly(
        self,
        code: str,
        severity: str,
        message: str,
        event: Dict[str, Any],
        suggestion: Optional[Dict[str, Any]] = None,
    ) -> None:
        record = {
            "code": code,
            "severity": severity,
            "message": message,
            "event_id": event["id"],
            "seq": event["seq"],
            "ts": event["ts"],
            "tid": event.get("tid"),
            "lock": event.get("lock"),
            "suggestion": suggestion,
        }
        self.anomalies.append(record)

    def _cancel_wait(self, thread: ThreadState, reason: str) -> None:
        wait = thread.waiting
        if wait is not None and not wait.cancelled:
            wait.cancelled = True
            wait.cancel_reason = reason
        thread.waiting = None

    def _expire_waits(self, now: float, context: Dict[str, Any]) -> None:
        """deadline 到期（没有显式 timeout 事件时）。"""
        for state in self.threads.values():
            wait = state.waiting
            if wait is None or wait.cancelled or wait.deadline is None:
                continue
            if wait.deadline <= now:
                if wait.kind == "lock":
                    self._anomaly(
                        "deadline_expired",
                        WARN,
                        "线程 %s 等待锁 %s 超过 deadline，请求撤销" % (state.tid, wait.lock),
                        wait.request,
                    )
                    self._cancel_wait(state, "deadline")
                else:
                    # condition 等待超时：按“重新竞争”处理
                    wait.kind = "lock"
                    wait.cond = None
                    wait.cond_wait = None
                    wait.deadline = None
                    self._anomaly(
                        "cond_timeout_inferred",
                        WARN,
                        "线程 %s 的 condition 等待超时，重新竞争锁 %s" % (state.tid, wait.lock),
                        context,
                    )

    # ------------------------------------------------------------------ append

    def append(self, event: Dict[str, Any]) -> None:
        out_of_order = self._max_ts is not None and event["ts"] < self._max_ts
        if self._max_ts is None or event["ts"] > self._max_ts:
            self._max_ts = event["ts"]
        self.events.append(event)
        if out_of_order:
            self._refold(event)
        else:
            self._fold_one(event)
            self.snapshots.append(self._build_snapshot(event, out_of_order=False))

    def _refold(self, late_event: Dict[str, Any]) -> None:
        """乱序到达：按时间戳稳定排序后从头重建（代次顺序保留为次序键）。"""
        self.rebuild_count += 1
        ordered = sorted(self.events, key=lambda e: (e["ts"], e["seq"]))
        self.threads = {}
        self.locks = {}
        for address, kind in self.lock_declarations.items():
            self.locks[address] = LockState(address, kind)
        self.snapshots = []
        self._max_ts = None
        for event in ordered:
            if self._max_ts is None or event["ts"] > self._max_ts:
                self._max_ts = event["ts"]
            self._fold_one(event, late=event is late_event)
            self.snapshots.append(self._build_snapshot(event, out_of_order=True))

    # ------------------------------------------------------------------ fold

    def _fold_one(self, event: Dict[str, Any], late: bool = False) -> None:
        kind = event["kind"]
        tid = event.get("tid")
        thread = self._thread(tid) if tid else None
        thread.last_event = event

        if late:
            self._anomaly(
                "out_of_order",
                WARN,
                "事件 %s 时间戳早于已处理事件，已按时间重建候选图" % event["id"],
                event,
            )

        self._expire_waits(event["ts"], event)
        if thread is not None and thread.ended and kind != "thread_end":
            self._anomaly(
                "event_after_end",
                WARN,
                "线程 %s 已结束又出现事件 %s" % (tid, kind),
                event,
            )

        if kind == "thread_start":
            self._on_thread_start(thread, event)
        elif kind == "thread_end":
            self._on_thread_end(thread, event)
        elif kind == "acquire_request":
            self._on_request(thread, event)
        elif kind == "acquired":
            self._on_acquired(thread, event)
        elif kind == "released":
            self._on_released(thread, event)
        elif kind == "cond_wait":
            self._on_cond_wait(thread, event)
        elif kind in ("cond_notify", "notify_all"):
            self._on_notify(event, all_waiters=(kind == "notify_all"))
        elif kind == "timeout":
            self._on_timeout(thread, event)

    def _on_thread_start(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        if thread.started:
            self._anomaly(
                "duplicate_start",
                WARN,
                "线程 %s 重复 start" % thread.tid,
                event,
            )
        thread.started = True
        thread.ended = False
        if thread.start_event is None:
            thread.start_event = event

    def _on_thread_end(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        if thread.waiting is not None and not thread.waiting.cancelled:
            self._anomaly(
                "wait_cancelled_on_exit",
                WARN,
                "线程 %s 退出时仍在等待，等待已撤销" % thread.tid,
                event,
            )
            self._cancel_wait(thread, "thread_end")
        held = [a for a in thread.levels if thread.levels[a]]
        if held:
            self._anomaly(
                "missing_release_on_exit",
                WARN,
                "线程 %s 退出时仍持有 %s，未观测到 release（不补造）"
                % (thread.tid, ", ".join(sorted(held))),
                event,
            )
        thread.ended = True
        thread.started = True

    def _on_request(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        address = event["lock"]
        lock = self._lock(address)
        if not thread.started:
            self._anomaly(
                "implicit_start",
                WARN,
                "线程 %s 未观测到 start 即请求锁 %s" % (thread.tid, address),
                event,
            )
            thread.started = True
        if thread.waiting is not None and not thread.waiting.cancelled:
            self._anomaly(
                "request_while_waiting",
                ERROR,
                "线程 %s 在等待期间又发起新请求（缺 acquired/timeout？）" % thread.tid,
                event,
            )
            self._cancel_wait(thread, "superseded")
        upgrading = event["mode"] == "exclusive" and any(
            lv.mode == "shared" for lv in thread.levels.get(address, [])
        )
        rec = WaitRec(
            kind="lock",
            lock=address,
            mode=event["mode"],
            request=event,
            upgrading=upgrading,
            deadline=event.get("deadline"),
        )
        thread.waiting = rec
        if not lock.declared_kind:
            kind = lock.effective_kind()
            if kind == "mutex":
                blocked = self._other_holders(thread, address)
                own = thread.levels.get(address, [])
                if blocked and not own:
                    # 被别人持有而请求：暂时无法判断 reentrant，保持 mutex 推断
                    pass

    def _on_acquired(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        address = event["lock"]
        lock = self._lock(address)
        wait = thread.waiting
        # 把 cond 重竞争拿到锁也归到对应等待记录上
        if wait is None or wait.lock != address:
            if thread.levels.get(address):
                self._anomaly(
                    "unmatched_acquired",
                    ERROR,
                    "线程 %s 对锁 %s 的 acquired 缺少对应 request" % (thread.tid, address),
                    event,
                )
            else:
                self._anomaly(
                    "unmatched_acquired",
                    ERROR,
                    "线程 %s 对锁 %s 的 acquired 缺少对应 request（缺事件？）"
                    % (thread.tid, address),
                    event,
                )
            mode = event["mode"]
            wait = WaitRec(kind="lock", lock=address, mode=mode, request=event)
            thread.waiting = wait

        mode = wait.mode
        own = thread.levels.get(address, [])
        if own:
            same_thread_holders = True
            top = own[-1]
            if mode == "exclusive" and top.mode == "exclusive":
                if not lock.declared_kind:
                    lock.inferred_reentrant = True
                if lock.effective_kind() == "mutex":
                    self._anomaly(
                        "self_deadlock",
                        ERROR,
                        "非可重入锁 %s 被线程 %s 重入请求" % (address, thread.tid),
                        event,
                        suggestion={
                            "type": "declare_lock_kind",
                            "lock": address,
                            "kind": "reentrant",
                        },
                    )
            elif mode == "exclusive" and top.mode == "shared":
                if not lock.declared_kind:
                    lock.inferred_shared = True
            elif mode == "shared":
                if not lock.declared_kind:
                    lock.inferred_shared = True
        else:
            if mode == "shared" and not lock.declared_kind:
                lock.inferred_shared = True

        # 检查是否与其它持有者冲突；冲突仍标记 acquired 则属于事件矛盾
        blockers = self._other_holders(thread, address, request_mode=mode)
        if blockers and self._conflicts(address, mode, blockers):
            self._anomaly(
                "acquired_while_held",
                ERROR,
                "锁 %s 仍被其它线程持有却记录 acquired（乱序/缺 release？）" % address,
                event,
            )

        if wait.kind == "cond" and wait.restore_levels:
            # condition 唤醒后重新拿到锁：恢复原持有栈，并把 level 的
            # 恢复来源记到顶层 acquired 事件，acquired 证据仍可回溯。
            restored = wait.restore_levels
            top_level = restored[-1] if restored else None
            new_top = Level(mode, event)
            if top_level is not None:
                # 顶层代表 cond_wait 时释放的那层：用新 acquired 取代，
                # 但记录恢复深度；其余层原样恢复。
                restored = restored[:-1] + [new_top]
            else:
                restored = [new_top]
            thread.levels[address] = restored
        else:
            thread.levels.setdefault(address, []).append(Level(mode, event))
        thread.waiting = None

    def _on_released(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        address = event["lock"]
        lock = self._lock(address)
        stack = thread.levels.get(address, [])
        if not stack:
            self._anomaly(
                "unmatched_release",
                ERROR,
                "线程 %s 释放了未持有的锁 %s（缺 acquired 或重复 release）"
                % (thread.tid, address),
                event,
            )
            return
        top = stack[-1]
        if event["mode"] != top.mode and not lock.declared_kind:
            self._anomaly(
                "release_mode_mismatch",
                WARN,
                "线程 %s 释放锁 %s 的模式 %s 与持有模式 %s 不一致"
                % (thread.tid, address, event["mode"], top.mode),
                event,
            )
        stack.pop()
        if not stack:
            del thread.levels[address]

    def _on_cond_wait(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        address = event["lock"]
        cond = event.get("cond") or address
        self._lock(address)
        stack = thread.levels.get(address, [])
        if not stack:
            self._anomaly(
                "cond_wait_without_lock",
                ERROR,
                "线程 %s cond_wait 时并未持有锁 %s" % (thread.tid, address),
                event,
            )
            restore: List[Level] = []
        else:
            restore = list(stack)
            del thread.levels[address]
        rec = WaitRec(
            kind="cond",
            lock=address,
            mode=restore[-1].mode if restore else "exclusive",
            request=event,
            cond=cond,
            cond_wait=event,
            restore_levels=restore,
            deadline=event.get("deadline"),
        )
        thread.waiting = rec

    def _on_notify(self, event: Dict[str, Any], all_waiters: bool) -> None:
        cond = event.get("cond")
        if not cond:
            self._anomaly(
                "notify_without_cond",
                WARN,
                "notify 事件 %s 缺少 cond" % event["id"],
                event,
            )
        woken: List[ThreadState] = []
        for state in self.threads.values():
            wait = state.waiting
            if wait is None or wait.cancelled or wait.kind != "cond":
                continue
            if cond is None or wait.cond == cond:
                woken.append(state)
                if not all_waiters:
                    break
        if not woken:
            self._anomaly(
                "notify_no_waiter",
                WARN,
                "notify(%s) 没有可唤醒的等待者（lost wakeup 或缺事件）" % cond,
                event,
            )
            return
        for state in woken:
            wait = state.waiting
            wait.kind = "lock"  # 唤醒后转为重新竞争该锁
            wait.request = event
            wait.cond = None
            wait.cond_wait = None
            wait.deadline = None

    def _on_timeout(self, thread: ThreadState, event: Dict[str, Any]) -> None:
        wait = thread.waiting
        if wait is None or wait.cancelled:
            self._anomaly(
                "timeout_without_wait",
                WARN,
                "线程 %s 的 timeout 事件没有活动等待" % thread.tid,
                event,
            )
            return
        if wait.kind == "cond":
            # condition 等待超时：重新竞争锁（恢复原持有语义）
            wait.kind = "lock"
            wait.request = event
            wait.cond = None
            wait.cond_wait = None
            wait.deadline = None
        else:
            self._cancel_wait(thread, "timeout")

    # -------------------------------------------------------------- conflicts

    def _other_holders(
        self,
        thread: ThreadState,
        address: str,
        request_mode: Optional[str] = None,
    ) -> List[ThreadState]:
        result = []
        for other in self.threads.values():
            if other is thread or other.ended:
                continue
            if other.levels.get(address):
                result.append(other)
        return result

    def _conflicts(
        self,
        address: str,
        request_mode: str,
        holders: List[ThreadState],
        upgrading: bool = False,
    ) -> bool:
        if not holders:
            return False
        if request_mode == "exclusive":
            return True
        # shared 与 shared 兼容；rwlock 升级请求即使当前全是共享也需排他
        if upgrading:
            return True
        return any(
            lv.mode == "exclusive"
            for holder in holders
            for lv in holder.levels.get(address, [])
        )
