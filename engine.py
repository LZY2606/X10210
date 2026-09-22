"""锁序解剖室核心引擎：事件归并、带置信度的状态重建、持有图/等待图、
死锁环检测、条件变量语义、最小事件片段缩减与流式增量缓存。

设计约定：
- 采集代次(epoch)区分进程重启；相同锁地址在不同 epoch 视为不同锁节点。
- 系统从不凭空补 release；一切缺失/冲突只产生 warning 与置信度标注。
- 所有边都携带产生它的事件引用(event_seq/event_id)，可回溯到原始事件。
"""
from __future__ import annotations

import copy
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

CANONICAL = {
    "request": "request",
    "acquire": "request",
    "request_acquire": "request",
    "try_acquire": "request",
    "acquired": "acquired",
    "acquire_success": "acquired",
    "acquire_ok": "acquired",
    "release": "release",
    "unlock": "release",
    "thread_start": "thread_start",
    "thread_begin": "thread_start",
    "start": "thread_start",
    "thread_end": "thread_end",
    "thread_exit": "thread_end",
    "thread_terminate": "thread_end",
    "exit": "thread_end",
    "cond_wait": "cond_wait",
    "wait": "cond_wait",
    "wait_start": "cond_wait",
    "cond_acquired": "cond_acquired",
    "wait_acquired": "cond_acquired",
    "cond_notify": "cond_notify",
    "notify": "cond_notify",
    "signal": "cond_notify",
    "cond_signal": "cond_notify",
    "cond_notify_all": "cond_notify_all",
    "notify_all": "cond_notify_all",
    "broadcast": "cond_notify_all",
    "timeout": "timeout",
    "timed_out": "timeout",
    "cancel": "timeout",
    "cancelled": "timeout",
}

MUTEX_MODES = {"X", "W", "write"}
READ_MODES = {"S", "R", "read"}


def canon_type(raw_type: str) -> Optional[str]:
    if not raw_type:
        return None
    return CANONICAL.get(str(raw_type).strip().lower())


def mode_of(ev: Dict[str, Any]) -> str:
    mode = str(ev.get("mode") or ev.get("lock_mode") or "X").upper()
    return "S" if mode in ("S", "R", "READ", "SHARED") else "X"


# ---------------------------------------------------------------------------
# 事件归并
# ---------------------------------------------------------------------------

def normalize_events(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """把导入的原始事件补齐 seq/id/epoch 并归一化字段。

    保持采集代次内的原始顺序（seq 单调），但乱序到达时仅在分析时标注，
    排序使用 (timestamp, seq)。
    """
    normalized: List[Dict[str, Any]] = []
    parse_errors: List[str] = []
    for i, raw in enumerate(raw_events):
        if not isinstance(raw, dict):
            parse_errors.append("第 %d 条不是对象，已跳过" % i)
            continue
        ctype = canon_type(str(raw.get("type", "")))
        if ctype is None:
            parse_errors.append(
                "第 %d 条未知事件类型 %r，已跳过" % (i, raw.get("type"))
            )
            continue
        ev = dict(raw)
        ev["type"] = ctype
        ev["seq"] = int(raw.get("seq", i))
        ev["id"] = raw.get("id") or ("ev%d" % i)
        ev["epoch"] = str(raw.get("epoch", "e0"))
        ev["thread"] = str(raw.get("thread", "?")) if raw.get("thread") is not None else "?"
        if raw.get("lock") is not None:
            ev["lock"] = str(raw["lock"])
        if raw.get("cond") is not None:
            ev["cond"] = str(raw["cond"])
        ev["mode"] = mode_of(ev)
        ts = raw.get("ts")
        if ts is None:
            ts = raw.get("timestamp")
        if ts is None:
            ts = i
        ev["timestamp"] = float(ts)
        ev["stack"] = raw.get("stack") or raw.get("backtrace") or []
        normalized.append(ev)
    normalized.sort(key=lambda e: (e["timestamp"], e["seq"]))
    prev_ts: Optional[float] = None
    prev_seq = -1
    for ev in normalized:
        if prev_ts is not None and ev["timestamp"] < prev_ts:
            ev["_out_of_order"] = True
        if ev["timestamp"] == prev_ts and ev["seq"] < prev_seq:
            ev["_out_of_order"] = True
        prev_ts = ev["timestamp"]
        prev_seq = ev["seq"]
    if parse_errors:
        # 归并错误通过哨兵事件保留下来（不参与重建，只展示）
        for msg in parse_errors:
            normalized.append(
                {
                    "type": "_parse_error",
                    "seq": 10**9 + len(normalized),
                    "id": "parse-%d" % len(normalized),
                    "epoch": "_",
                    "thread": "_",
                    "timestamp": float("inf"),
                    "stack": [],
                    "message": msg,
                }
            )
    return normalized


# ---------------------------------------------------------------------------
# 重建状态
# ---------------------------------------------------------------------------

@dataclass
class Waiter:
    thread: str
    lock: str
    epoch: str
    mode: str
    request_ev: Dict[str, Any]
    target: Optional[str] = None
    cond: Optional[str] = None
    cond_release_stack: List[Any] = field(default_factory=list)
    notified_by: Optional[Dict[str, Any]] = None
    reentrant: bool = False
    # 纯条件变量等待（此时并不竞争锁的等待边）
    cond_only: bool = False
    # cond_wait 时暂时脱离持有的实例链（可能含重入多层）
    detached: List["Instance"] = field(default_factory=list)


@dataclass
class Instance:
    """一次“加锁配对”：request -> acquired -> release，或 cond 重新竞争段。"""
    mode: str
    acquired_ev: Optional[Dict[str, Any]] = None
    request_ev: Optional[Dict[str, Any]] = None
    release_ev: Optional[Dict[str, Any]] = None
    # 条件变量等待段（在 acquired 之后、重新 acquired 之前）
    cond_ev: Optional[Dict[str, Any]] = None
    notify_ev: Optional[Dict[str, Any]] = None
    reacquire_ev: Optional[Dict[str, Any]] = None
    depth: int = 0


@dataclass
class Block:
    """缩减单位：某线程在某锁上的一段生命周期，可能含嵌套与 cond 段。"""
    block_id: int
    thread: str
    lock: str
    epoch: str
    instances: List[Instance] = field(default_factory=list)
    active: bool = False


@dataclass
class State:
    # (epoch, lock) -> {"mutex"/"rw"} -> thread -> [Instance,...]
    holds: Dict[Tuple[str, str], Dict[str, List[Instance]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(list))
    )
    # (epoch, thread, lock) -> 待处理请求
    pending: Dict[Tuple[str, str, str], Waiter] = field(default_factory=dict)
    # (epoch, thread, cond) -> 纯 cond 等待
    cond_waiters: Dict[Tuple[str, str, str], Waiter] = field(default_factory=dict)
    threads: Set[Tuple[str, str]] = field(default_factory=set)
    thread_started: Dict[Tuple[str, str], bool] = field(default_factory=dict)
    ended: Set[Tuple[str, str]] = field(default_factory=set)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    events_seen: int = 0
    events: List[Dict[str, Any]] = field(default_factory=list)
    blocks: List[Block] = field(default_factory=list)
    # (epoch, thread, lock) -> 当前活动 block
    active_blocks: Dict[Tuple[str, str, str], Block] = field(default_factory=dict)
    lock_types: Dict[Tuple[str, str], str] = field(default_factory=dict)
    infer_rw: Set[Tuple[str, str]] = field(default_factory=set)
    replayed: int = 0

    def warn(self, code: str, message: str, ev: Optional[Dict[str, Any]] = None,
             severity: str = "warn", confidence: str = "low") -> None:
        self.warnings.append(
            {
                "code": code,
                "severity": severity,
                "message": message,
                "confidence": confidence,
                "event_seq": ev.get("seq") if ev else None,
                "event_id": ev.get("id") if ev else None,
                "epoch": ev.get("epoch") if ev else None,
                "thread": ev.get("thread") if ev else None,
                "lock": ev.get("lock") if ev else None,
                "timestamp": ev.get("timestamp") if ev else None,
            }
        )

    def lock_type(self, epoch: str, lock: str) -> str:
        key = (epoch, lock)
        if key in self.lock_types:
            return self.lock_types[key]
        return "rw" if key in self.infer_rw else "mutex"

    def owners(self, epoch: str, lock: str) -> Dict[str, List[Instance]]:
        return self.holds[(epoch, lock)]

    def owner_threads(self, epoch: str, lock: str) -> List[str]:
        return [t for t, insts in self.owners(epoch, lock).items() if insts]

    def current_instance(self, thread: str, epoch: str, lock: str) -> Optional[Instance]:
        insts = self.holds[(epoch, lock)].get(thread)
        if insts:
            return insts[-1]
        return None


# ---------------------------------------------------------------------------
# 重建器
# ---------------------------------------------------------------------------

class Rebuilder:
    def __init__(self, state: Optional[State] = None,
                 lock_types: Optional[Dict[Tuple[str, str], str]] = None,
                 collect_instances: bool = True):
        self.s = state or State()
        self.collect_instances = collect_instances
        if lock_types:
            self.s.lock_types.update(lock_types)

    # --- block / instance 记账 ------------------------------------------------

    def _begin_block(self, ev: Dict[str, Any]) -> Optional[Block]:
        key = (ev["epoch"], ev["thread"], ev["lock"])
        block = self.s.active_blocks.get(key)
        if block is not None:
            return block
        block = Block(len(self.s.blocks), ev["thread"], ev["lock"], ev["epoch"])
        self.s.blocks.append(block)
        self.s.active_blocks[key] = block
        return block

    def _end_block(self, epoch: str, thread: str, lock: str) -> None:
        self.s.active_blocks.pop((epoch, thread, lock), None)

    def _ensure_instance(self, ev: Dict[str, Any], mode: str) -> Instance:
        key = (ev["epoch"], ev["lock"])
        insts = self.s.holds[key][ev["thread"]]
        depth = len(insts)
        inst = Instance(mode=mode, depth=depth)
        if self.collect_instances:
            inst.acquired_ev = ev
            block = self._begin_block(ev)
            if block is not None:
                block.instances.append(inst)
                block.active = True
        insts.append(inst)
        return inst

    # --- 事件处理 -------------------------------------------------------------

    def apply(self, ev: Dict[str, Any]) -> None:
        self.s.replayed += 1
        etype = ev["type"]
        if etype == "_parse_error":
            self.s.warn("parse_error", ev.get("message", "解析错误"), ev, "error", "low")
            return
        self.s.events.append(ev)
        self.s.events_seen += 1
        if ev.get("_out_of_order"):
            self.s.warn(
                "out_of_order",
                "事件乱序到达（时间戳早于已处理事件），按时间戳归位后重建",
                ev,
                "warn",
                "medium",
            )
        handler = {
            "thread_start": self._on_thread_start,
            "thread_end": self._on_thread_end,
            "request": self._on_request,
            "acquired": self._on_acquired,
            "release": self._on_release,
            "cond_wait": self._on_cond_wait,
            "cond_acquired": self._on_cond_acquired,
            "cond_notify": self._on_cond_notify,
            "cond_notify_all": self._on_cond_notify,
            "timeout": self._on_timeout,
        }.get(etype)
        if handler is None:
            self.s.warn("unknown_event", "未知事件类型 %s，已忽略" % etype, ev)
            return
        handler(ev)

    def _on_thread_start(self, ev: Dict[str, Any]) -> None:
        key = (ev["epoch"], ev["thread"])
        self.s.threads.add(key)
        if self.s.thread_started.get(key):
            self.s.warn("duplicate_thread_start", "线程重复 start 事件", ev, "warn", "medium")
        self.s.thread_started[key] = True
        self.s.ended.discard(key)

    def _on_thread_end(self, ev: Dict[str, Any]) -> None:
        key = (ev["epoch"], ev["thread"])
        if key not in self.s.threads:
            self.s.warn(
                "thread_end_without_start",
                "线程结束但从未出现 start 事件（缺失，不凭空补）",
                ev,
                "warn",
                "medium",
            )
            self.s.threads.add(key)
        if key in self.s.ended:
            self.s.warn("duplicate_thread_end", "线程重复 end 事件", ev, "warn", "medium")
        # 线程终止撤销所有等待
        for wkey in [k for k, w in self.s.pending.items()
                     if (w.epoch, w.thread) == key]:
            w = self.s.pending.pop(wkey)
            self.s.warn(
                "wait_cancelled_on_exit",
                "线程终止，撤销对锁 %s 的等待" % w.lock,
                ev,
                "info",
                "high",
            )
        for ckey in [k for k, w in self.s.cond_waiters.items()
                     if (w.epoch, w.thread) == key]:
            self.s.cond_waiters.pop(ckey)
        # 线程死亡时仍持有的锁：没有 release 事件，绝不补造
        still_held = [l for (e, l), owners in self.s.holds.items()
                      if (e, l) and ev["thread"] in owners and owners[ev["thread"]] and e == ev["epoch"]]
        for lock in still_held:
            insts = self.s.holds[(ev["epoch"], lock)][ev["thread"]]
            self.s.warn(
                "missing_release_on_exit",
                "线程终止时仍持有锁 %s（%d 层），缺少 release，保留低置信持有边"
                % (lock, len(insts)),
                ev,
                "warn",
                "low",
            )
            self._end_block(ev["epoch"], ev["thread"], lock)
        self.s.ended.add(key)

    # --- request / acquired ---------------------------------------------------

    def _on_request(self, ev: Dict[str, Any]) -> None:
        lock = ev.get("lock")
        if lock is None:
            self.s.warn("request_without_lock", "acquire 请求缺少 lock 字段", ev, "error", "low")
            return
        self._check_thread_liveness(ev)
        key = (ev["epoch"], ev["thread"], lock)
        if key in self.s.pending:
            old = self.s.pending[key]
            self.s.warn(
                "duplicate_request",
                "锁 %s 已有未完成请求（seq=%s），新请求替换旧等待"
                % (lock, old.request_ev.get("seq")),
                ev,
                "warn",
                "medium",
            )
        mode = ev["mode"]
        ltype = self.s.lock_type(ev["epoch"], lock)
        if ltype == "mutex" and mode == "S":
            self.s.warn("mode_on_mutex", "互斥锁 %s 出现共享模式，按排他处理" % lock,
                        ev, "warn", "medium")
            mode = "X"
        owners = self.s.owner_threads(ev["epoch"], lock)
        held_by_self = ev["thread"] in owners
        waiter = Waiter(
            thread=ev["thread"], lock=lock, epoch=ev["epoch"], mode=mode,
            request_ev=ev,
        )
        if ltype == "mutex":
            reentrant = bool(held_by_self)
        else:
            # 读写锁：同线程已持共享再请求共享 => 递归共享读；其它（含升级）真实等待
            cur = self.s.current_instance(ev["thread"], ev["epoch"], lock)
            reentrant = bool(held_by_self and cur and cur.mode == "S" and mode == "S")
            if held_by_self and mode == "X":
                self.s.infer_rw.add((ev["epoch"], lock))
        if reentrant:
            waiter.reentrant = True
            self.s.warn(
                "reentrant_request",
                "线程已持有 %s 锁 %s 又请求（按可重入处理，不构成等待边）"
                % ("读写" if ltype == "rw" else "互斥", lock),
                ev,
                "info",
                "high",
            )
        self.s.pending[key] = waiter

    def _check_thread_liveness(self, ev: Dict[str, Any]) -> None:
        key = (ev["epoch"], ev["thread"])
        if key in self.s.ended:
            self.s.warn("event_after_thread_end", "线程已结束后又出现事件", ev, "warn", "low")
        elif not self.s.thread_started.get(key):
            self.s.warn(
                "missing_thread_start",
                "线程缺少 start 事件（不补造，仅标注）",
                ev,
                "info",
                "medium",
            )
            self.s.threads.add(key)

    def _on_acquired(self, ev: Dict[str, Any]) -> None:
        lock = ev.get("lock")
        if lock is None:
            self.s.warn("acquired_without_lock", "成功事件缺少 lock 字段", ev, "error", "low")
            return
        self._check_thread_liveness(ev)
        key = (ev["epoch"], ev["thread"], lock)
        waiter = self.s.pending.pop(key, None)
        if waiter is not None and waiter.cond_only:
            # 不该发生：cond_only 在 cond_acquired 中处理
            self.s.pending[key] = waiter
        mode = waiter.mode if waiter else ev["mode"]
        owners = self.s.owners(ev["epoch"], lock)
        existing = owners.get(ev["thread"])
        if existing:
            ltype = self.s.lock_type(ev["epoch"], lock)
            cur = existing[-1]
            same_read = ltype == "rw" and cur.mode == "S" and mode == "S"
            if waiter is not None and waiter.reentrant or same_read:
                inst = self._ensure_instance(ev, cur.mode if same_read else mode)
                inst.request_ev = waiter.request_ev if waiter else None
                return
            self.s.warn(
                "acquired_while_held",
                "成功事件到达但线程已持有锁 %s（可能缺 release 或可重入未声明），"
                "按再一层持有处理" % lock,
                ev,
                "warn",
                "low",
            )
        if waiter is None:
            self.s.warn(
                "acquired_without_request",
                "锁 %s 的成功事件没有对应 acquire 请求（缺失，不补造请求边）" % lock,
                ev,
                "warn",
                "medium",
            )
        inst = self._ensure_instance(ev, mode)
        if waiter is not None:
            inst.request_ev = waiter.request_ev

    def _on_release(self, ev: Dict[str, Any]) -> None:
        lock = ev.get("lock")
        if lock is None:
            self.s.warn("release_without_lock", "release 事件缺少 lock 字段", ev, "error", "low")
            return
        key = (ev["epoch"], ev["lock"])
        insts = self.s.holds[key].get(ev["thread"])
        if not insts:
            self.s.warn(
                "release_without_hold",
                "线程释放锁 %s 但没有持有记录（缺 acquire 或多余 release），忽略该 release",
                ev,
                "warn",
                "low",
            )
            return
        inst = insts.pop()
        if self.collect_instances:
            inst.release_ev = ev
        if not insts:
            del self.s.holds[key][ev["thread"]]
            self._end_block(ev["epoch"], ev["thread"], lock)
        # 释放后恰好被唤醒、重新竞争中的 cond waiter：不撤销（它独立竞争）

    def _on_cond_wait(self, ev: Dict[str, Any]) -> None:
        lock = ev.get("lock")
        cond = ev.get("cond")
        if lock is None or cond is None:
            self.s.warn("cond_wait_fields", "cond_wait 缺少 lock 或 cond 字段", ev, "error", "low")
            return
        self._check_thread_liveness(ev)
        insts = self.s.holds[(ev["epoch"], lock)].get(ev["thread"])
        if not insts:
            self.s.warn(
                "cond_wait_without_hold",
                "cond_wait 时线程并未持有锁 %s（缺 acquire），只登记条件等待" % lock,
                ev,
                "warn",
                "low",
            )
        # 暂时释放整段持有（含重入层），唤醒后原样恢复
        detached = self.s.holds[(ev["epoch"], lock)].pop(ev["thread"], [])
        for d in detached:
            d.cond_ev = ev
        inst = detached[-1] if detached else None
        ckey = (ev["epoch"], ev["thread"], cond)
        waiter = Waiter(
            thread=ev["thread"], lock=lock, epoch=ev["epoch"],
            mode=inst.mode if inst else ev["mode"], request_ev=ev,
            cond=cond, cond_only=True, detached=detached,
        )
        waiter.cond_release_stack = list(ev.get("stack") or [])
        self.s.cond_waiters[ckey] = waiter

    def _on_cond_acquired(self, ev: Dict[str, Any]) -> None:
        lock = ev.get("lock")
        cond = ev.get("cond")
        if lock is None:
            self.s.warn("cond_acquired_fields", "cond 重新获取成功缺少 lock", ev, "error", "low")
            return
        self._check_thread_liveness(ev)
        if cond is not None:
            ckey = (ev["epoch"], ev["thread"], cond)
            cw = self.s.cond_waiters.pop(ckey, None)
        else:
            cw = None
            for k, w in list(self.s.cond_waiters.items()):
                if k[0] == ev["epoch"] and w.thread == ev["thread"] and w.lock == lock:
                    cw = self.s.cond_waiters.pop(k)
                    break
        if cw is None:
            self.s.warn(
                "cond_acquired_without_wait",
                "cond 重新获取成功但没有 wait 记录（缺 wait 事件）",
                ev,
                "warn",
                "medium",
            )
        owners = self.s.owners(ev["epoch"], lock)
        existing = owners.get(ev["thread"])
        if existing:
            self.s.warn(
                "cond_acquired_while_held",
                "唤醒重新获取 %s 时线程已持有该锁（事件重复？）",
                ev,
                "warn",
                "low",
            )
        if cw is not None and cw.detached:
            # 恢复 cond_wait 前的持有链
            restored = cw.detached
            self.s.holds[(ev["epoch"], lock)][ev["thread"]] = restored
            self._begin_block(ev)
            for inst in restored:
                inst.notify_ev = cw.notified_by
            restored[-1].reacquire_ev = ev
            return
        mode = cw.mode if cw else ev["mode"]
        inst = self._ensure_instance(ev, mode)
        if cw is not None:
            inst.cond_ev = cw.request_ev
            inst.notify_ev = cw.notified_by
            # reacquire_ev 就是当前事件本身
            inst.reacquire_ev = ev

    def _on_cond_notify(self, ev: Dict[str, Any]) -> None:
        cond = ev.get("cond")
        if cond is None:
            self.s.warn("notify_without_cond", "notify 缺少 cond 字段", ev, "error", "low")
            return
        all_waiters = [
            (k, w) for k, w in self.s.cond_waiters.items()
            if k[0] == ev["epoch"] and k[2] == cond
        ]
        if not all_waiters:
            self.s.warn(
                "lost_notify",
                "notify 条件变量 %s 时没有等待者（lost wakeup 或事件缺失）" % cond,
                ev,
                "info",
                "medium",
            )
            return
        all_waiters.sort(key=lambda kw: kw[1].request_ev["timestamp"])
        broadcast = ev["type"] == "cond_notify_all"
        chosen = all_waiters if broadcast else all_waiters[:1]
        for ckey, w in chosen:
            w.notified_by = ev
            w.cond_only = False
            self.s.cond_waiters.pop(ckey, None)
            # 唤醒后重新竞争锁：登记为普通待处理请求，等待 cond_acquired
            pkey = (w.epoch, w.thread, w.lock)
            if pkey in self.s.pending:
                self.s.warn("notify_pending_collision",
                            "唤醒的线程已有锁 %s 的待处理请求" % w.lock, ev, "warn", "medium")
            self.s.pending[pkey] = w

    def _on_timeout(self, ev: Dict[str, Any]) -> None:
        cond = ev.get("cond")
        lock = ev.get("lock")
        if cond is not None:
            # cond 超时：条件等待结束，重新竞争锁
            ckey = (ev["epoch"], ev["thread"], cond)
            w = self.s.cond_waiters.pop(ckey, None)
            if w is None:
                self.s.warn("timeout_without_cond_wait",
                            "cond 超时但没有等待记录", ev, "warn", "medium")
                return
            w.cond_only = False
            self.s.pending[(w.epoch, w.thread, w.lock)] = w
            self.s.warn(
                "cond_timeout_recompete",
                "条件等待 %s 超时，线程重新竞争锁 %s" % (cond, w.lock),
                ev, "info", "high",
            )
            return
        if lock is None:
            self.s.warn("timeout_fields", "超时事件缺少 lock/cond", ev, "error", "low")
            return
        pkey = (ev["epoch"], ev["thread"], lock)
        w = self.s.pending.pop(pkey, None)
        if w is None:
            self.s.warn("timeout_without_wait",
                        "锁 %s 超时取消但没有待处理请求" % lock, ev, "warn", "medium")
            return
        self.s.warn(
            "request_timeout",
            "获取锁 %s 的请求超时，撤销等待" % lock, ev, "info", "high",
        )


# ---------------------------------------------------------------------------
# 持有图 / 等待图
# ---------------------------------------------------------------------------

def build_graph(state: State, time: Optional[float] = None) -> Dict[str, Any]:
    """从重建状态生成节点、持有边、等待边、cond 等待与死锁环。"""
    nodes: Dict[str, Dict[str, Any]] = {}
    hold_edges: List[Dict[str, Any]] = []
    wait_edges: List[Dict[str, Any]] = []

    def add_thread(epoch: str, thread: str) -> str:
        nid = "t:%s:%s" % (epoch, thread)
        nodes.setdefault(nid, {"id": nid, "kind": "thread", "label": thread, "epoch": epoch})
        return nid

    def add_lock(epoch: str, lock: str) -> str:
        nid = "l:%s:%s" % (epoch, lock)
        nodes.setdefault(
            nid,
            {
                "id": nid, "kind": "lock", "label": lock, "epoch": epoch,
                "lock_type": state.lock_type(epoch, lock),
            },
        )
        return nid

    # 持有边：锁 -> 线程（“当前被谁持有”）
    for (epoch, lock), owners in state.holds.items():
        for thread, insts in owners.items():
            if not insts:
                continue
            add_lock(epoch, lock)
            add_thread(epoch, thread)
            top = insts[-1]
            confidence = "high"
            warnings_codes: List[str] = []
            if top.acquired_ev is None:
                confidence = "low"
            hold_edges.append(
                {
                    "id": "hold-%s-%s-%s" % (epoch, lock, thread),
                    "kind": "hold",
                    "source": "l:%s:%s" % (epoch, lock),
                    "target": "t:%s:%s" % (epoch, thread),
                    "epoch": epoch,
                    "lock": lock,
                    "thread": thread,
                    "mode": top.mode,
                    "depth": len(insts),
                    "confidence": confidence,
                    "evidence": {
                        "acquired_event": top.acquired_ev and top.acquired_ev.get("id"),
                        "acquired_seq": top.acquired_ev and top.acquired_ev.get("seq"),
                        "request_event": top.request_ev and top.request_ev.get("id"),
                        "release_event": top.release_ev and top.release_ev.get("id"),
                        "hold_stack": (top.acquired_ev or {}).get("stack", []),
                        "cond_wait_event": top.cond_ev and top.cond_ev.get("id"),
                        "notify_event": top.notify_ev and top.notify_ev.get("id"),
                        "reacquire_event": top.reacquire_ev and top.reacquire_ev.get("id"),
                    },
                    "warnings": warnings_codes,
                }
            )

    # 等待边：线程 -> 锁
    for waiter in state.pending.values():
        if waiter.reentrant:
            continue
        add_thread(waiter.epoch, waiter.thread)
        add_lock(waiter.epoch, waiter.lock)
        owners = state.owner_threads(waiter.epoch, waiter.lock)
        ltype = state.lock_type(waiter.epoch, waiter.lock)
        # 读写锁：若当前只有共享持有者，新的共享请求本可直接通过；
        # 既然没有 acquired 事件，说明采集期内确有竞争，统一建边并标置信度。
        edge = {
            "id": "wait-%s-%s-%s" % (waiter.epoch, waiter.lock, waiter.thread),
            "kind": "wait",
            "source": "t:%s:%s" % (waiter.epoch, waiter.thread),
            "target": "l:%s:%s" % (waiter.epoch, waiter.lock),
            "epoch": waiter.epoch,
            "lock": waiter.lock,
            "thread": waiter.thread,
            "mode": waiter.mode,
            "confidence": "high",
            "targets": owners,
            "is_upgrade": False,
            "cond": waiter.cond,
            "evidence": {
                "request_event": waiter.request_ev.get("id"),
                "request_seq": waiter.request_ev.get("seq"),
                "wait_stack": waiter.request_ev.get("stack", []),
                "notified_by": waiter.notified_by and waiter.notified_by.get("id"),
            },
        }
        cur = state.current_instance(waiter.thread, waiter.epoch, waiter.lock)
        if cur is not None:
            edge["is_upgrade"] = True
            edge["confidence"] = "medium"
            edge["evidence"]["held_stack"] = (cur.acquired_ev or {}).get("stack", [])
        if not owners:
            # 等待一个此刻无持有者的锁：可能乱序/缺事件
            edge["confidence"] = "low"
        wait_edges.append(edge)

    cond_waits = []
    for waiter in state.cond_waiters.values():
        add_thread(waiter.epoch, waiter.thread)
        cond_waits.append(
            {
                "thread": waiter.thread,
                "epoch": waiter.epoch,
                "cond": waiter.cond,
                "lock": waiter.lock,
                "mode": waiter.mode,
                "confidence": "high",
                "evidence": {
                    "wait_event": waiter.request_ev.get("id"),
                    "wait_seq": waiter.request_ev.get("seq"),
                    "release_stack": waiter.cond_release_stack,
                    "wait_stack": waiter.request_ev.get("stack", []),
                },
            }
        )

    cycles = find_cycles(nodes, hold_edges, wait_edges, state)
    return {
        "nodes": list(nodes.values()),
        "hold_edges": hold_edges,
        "wait_edges": wait_edges,
        "cond_waits": cond_waits,
        "cycles": cycles,
        "warnings": state.warnings,
        "events_seen": state.events_seen,
        "time": time,
    }


def find_cycles(nodes: Dict[str, Dict[str, Any]],
                hold_edges: List[Dict[str, Any]],
                wait_edges: List[Dict[str, Any]],
                state: State) -> List[Dict[str, Any]]:
    """在 线程--wait-->锁--hold-->线程 的有向图上枚举简单环。

    锁 -> 线程为持有边，线程 -> 锁为等待边，真正的死锁环必然交替出现。
    仅由“自己等自己”构成的 2 边自环不计入（递归读/伪升级），
    但真正的同线程读写升级自死锁（线程持有共享又等待同一把锁的排他）保留。
    """
    adj: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in hold_edges:
        adj[e["source"]].append(e)
    for e in wait_edges:
        adj[e["source"]].append(e)

    found: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    node_ids = sorted(nodes.keys())

    def dfs(start: str, current: str, path_nodes: List[str],
            path_edges: List[Dict[str, Any]], visited: Set[str]) -> None:
        for edge in adj.get(current, []):
            nxt = edge["target"]
            if nxt == start and len(path_nodes) >= 2:
                cycle_nodes = tuple(path_nodes)
                cand_edges = path_edges + [edge]
                kinds = [x["kind"] for x in cand_edges]
                if not _valid_cycle(kinds, cand_edges):
                    continue
                if cycle_nodes not in found:
                    found[cycle_nodes] = cand_edges
                continue
            if nxt in visited or nxt < start:
                continue
            visited.add(nxt)
            dfs(start, nxt, path_nodes + [nxt], path_edges + [edge], visited)
            visited.discard(nxt)

    for nid in node_ids:
        dfs(nid, nid, [nid], [], {nid})

    cycles: List[Dict[str, Any]] = []
    for cidx, (cnodes, cedges) in enumerate(found.items()):
        threads: List[str] = []
        locks: List[str] = []
        wait_stacks: List[Dict[str, Any]] = []
        hold_stacks: List[Dict[str, Any]] = []
        epoch = cedges[0]["epoch"]
        confidence = "high"
        for e in cedges:
            confidence = _min_conf(confidence, e.get("confidence", "high"))
            if e["kind"] == "wait":
                if e["thread"] not in threads:
                    threads.append(e["thread"])
                if e["lock"] not in locks:
                    locks.append(e["lock"])
                wait_stacks.append(
                    {
                        "thread": e["thread"],
                        "lock": e["lock"],
                        "mode": e["mode"],
                        "stack": e["evidence"].get("wait_stack", []),
                        "request_event": e["evidence"].get("request_event"),
                        "is_upgrade": e.get("is_upgrade", False),
                        "cond": e.get("cond"),
                    }
                )
            else:
                if e["thread"] not in threads:
                    threads.append(e["thread"])
                if e["lock"] not in locks:
                    locks.append(e["lock"])
                hold_stacks.append(
                    {
                        "thread": e["thread"],
                        "lock": e["lock"],
                        "mode": e["mode"],
                        "stack": e["evidence"].get("hold_stack", []),
                        "acquired_event": e["evidence"].get("acquired_event"),
                        "depth": e.get("depth", 1),
                    }
                )
        sig = _cycle_signature(cedges)
        cycles.append(
            {
                "id": "cycle-%d" % cidx,
                "epoch": epoch,
                "nodes": list(cnodes),
                "edge_ids": [e["id"] for e in cedges],
                "threads": threads,
                "locks": locks,
                "wait_stack": wait_stacks,
                "hold_stack": hold_stacks,
                "confidence": confidence,
                "signature": sig,
                "description": _describe_cycle(cedges),
            }
        )
    cycles.sort(key=lambda c: c["signature"])
    return cycles


def _min_conf(a: str, b: str) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    return a if order.get(a, 2) <= order.get(b, 2) else b


def _valid_cycle(kinds: List[str], edges: List[Dict[str, Any]]) -> bool:
    if len(kinds) < 4:
        # 只可能是 wait + hold 两节点自环
        if len(kinds) == 2 and kinds[0] == "wait" and kinds[1] == "hold":
            wait_edge = edges[0]
            return bool(wait_edge.get("is_upgrade")) and wait_edge["mode"] == "X"
        return False
    # 必须严格交替 wait/hold
    expected = ["wait", "hold"] * (len(kinds) // 2)
    if kinds != expected:
        return False
    return True


def _cycle_signature(edges: List[Dict[str, Any]]) -> str:
    parts = []
    for e in edges:
        if e["kind"] == "wait":
            parts.append("%s>%s" % (e["thread"], e["lock"]))
        else:
            parts.append("%s@%s" % (e["lock"], e["thread"]))
    return "|".join(parts)


def _describe_cycle(edges: List[Dict[str, Any]]) -> str:
    waits = [e for e in edges if e["kind"] == "wait"]
    return " ; ".join(
        "线程 %s 等待锁 %s（%s），锁 %s 由线程 %s 持有"
        % (
            w["thread"], w["lock"],
            "升级写" if w.get("is_upgrade") else ("共享请求" if w["mode"] == "S" else "排他请求"),
            w["lock"],
            ", ".join(w.get("targets") or ["?"]),
        )
        for w in waits
    )


# ---------------------------------------------------------------------------
# 流式分析器：只重算受影响时间段
# ---------------------------------------------------------------------------

class Analyzer:
    """按时间滑块查询；checkpoint 让追加事件时只向前推进，而时间回退到更早
    检查点之间时仅回放该段（而非整个轨迹）。"""

    CHECKPOINT_EVENTS = 32

    def __init__(self, events: Optional[List[Dict[str, Any]]] = None,
                 lock_types: Optional[Dict[Tuple[str, str], str]] = None):
        self.events: List[Dict[str, Any]] = []
        self.lock_types = dict(lock_types or {})
        self.checkpoints: List[Tuple[int, int, State]] = []
        # (index, state.replayed) 缓存，避免完全相同查询重复回放
        self._last_query: Optional[Tuple[int, State]] = None
        if events:
            self.append(events)

    def append(self, new_events: List[Dict[str, Any]]) -> None:
        """流式追加：已有事件状态原样保留，仅对新事件继续 apply。"""
        batch_start = len(self.events)
        normed = normalize_events(new_events)
        self.events.extend(normed)
        self.events.sort(key=lambda e: (e["timestamp"], e["seq"]))
        # 排序可能使新事件插到中间：检查既有事件之后是否出现更早事件
        if any(e["timestamp"] < self.events[batch_start - 1]["timestamp"]
               for e in normed) and batch_start > 0:
            self._invalidate_from(batch_start)
        # 继续推进最后一个检查点/缓存
        self._advance()

    def _invalidate_from(self, idx: int) -> None:
        """新到达事件落在已分析区间内（乱序到达）：回滚到 idx 之前的检查点。"""
        cp_idx = 0
        for i, (ev_idx, _rep, _st) in enumerate(self.checkpoints):
            if ev_idx <= idx:
                cp_idx = i
            else:
                break
        self.checkpoints = self.checkpoints[: cp_idx + 1]
        self._last_query = None
        self._advance()

    def _advance(self) -> None:
        if self.checkpoints:
            start_idx, _rep, state = self.checkpoints[-1]
        else:
            start_idx = 0
            state = None
        if state is None:
            state = State()
            state.lock_types.update(self.lock_types)
            rebuilder = Rebuilder(state)
            self.checkpoints.append((0, 0, copy.deepcopy(state)))
        else:
            rebuilder = Rebuilder(state)
        last_cp_idx = self.checkpoints[-1][0]
        for i in range(start_idx, len(self.events)):
            rebuilder.apply(self.events[i])
            if (i + 1 - last_cp_idx) % self.CHECKPOINT_EVENTS == 0:
                self.checkpoints.append((i + 1, state.replayed, copy.deepcopy(state)))
        self._last_query = (len(self.events), state)

    def graph_at(self, time: Optional[float] = None) -> Dict[str, Any]:
        return self.snapshot(time)["graph"]

    def snapshot(self, time: Optional[float] = None) -> Dict[str, Any]:
        # 选择时间点：包含 timestamp <= time 的全部事件
        upto = len(self.events)
        if time is not None:
            upto = 0
            for ev in self.events:
                if ev["type"] == "_parse_error":
                    continue
                if ev["timestamp"] <= time:
                    upto += 1
        # 找最近检查点
        base_idx = 0
        base_state: Optional[State] = None
        for ev_idx, _rep, st in self.checkpoints:
            if ev_idx <= upto:
                base_idx, base_state = ev_idx, st
        if base_state is None:
            base_state = State()
            base_state.lock_types.update(self.lock_types)
        state = copy.deepcopy(base_state)
        rebuilder = Rebuilder(state)
        for ev in self.events[base_idx:upto]:
            rebuilder.apply(ev)
        graph = build_graph(state, time)
        timestamps = [e["timestamp"] for e in self.events[:upto]
                      if e["type"] != "_parse_error"]
        graph["time_bounds"] = {
            "min": timestamps[0] if timestamps else 0.0,
            "max": timestamps[-1] if timestamps else 0.0,
            "requested": time,
            "events_included": upto,
            "total_events": len([e for e in self.events if e["type"] != "_parse_error"]),
        }
        return {"state": state, "graph": graph}

    def full_state(self) -> State:
        if self._last_query is not None and self._last_query[0] == len(self.events):
            return self._last_query[1]
        self._advance()
        return self._last_query[1]
