"""锁序解剖室 —— 核心重建引擎。

输入：带采集代次（generation）与时间戳的线程事件流（允许乱序到达）。
输出：任一时点的持有图、等待图、候选置信、环路证据、近失时间线，
     以及保留配对语义的最小化轨迹。

引擎本身是纯函数式的：给定一组事件与锁类型声明，重放出全部时点，
不修改入参，方便分支对比与流式追加。
"""
from __future__ import annotations

from collections import defaultdict
from functools import reduce
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

EVENT_TYPES = {
    "thread_start",
    "thread_end",
    "acquire_request",
    "acquire_granted",
    "acquire_timeout",
    "release",
    "cond_wait",
    "cond_notify",
    "cond_granted",
}

# 置信度：
HIGH = "high"        # 请求与授予配对完整，顺序合理
MEDIUM = "medium"    # 推断或经历过乱序/冲突
LOW = "low"          # 缺事件支撑（如授予没有对应请求）

SEV_ERROR = "error"
SEV_WARN = "warn"
SEV_INFO = "info"

_CONF_RANK = {HIGH: 3, MEDIUM: 2, LOW: 1}


def min_conf(a: str, b: str) -> str:
    return a if _CONF_RANK[a] <= _CONF_RANK[b] else b


def lock_kind(types: dict, lock: str) -> str:
    return types.get(lock, "mutex")


def _modes_compatible(lock: str, kind: str, holders: list, mode: str) -> bool:
    """holders: list[Hold]；判断新的 mode 请求当前是否与持有者兼容。"""
    if not holders:
        return True
    if kind == "rwlock":
        if mode == "R":
            return all(h.mode == "R" for h in holders)
        return False  # W/X 需要独占
    return False


def _own_modes_for(holders: list, thread: str) -> list:
    return [h for h in holders if h.thread == thread]


def sort_key(ev: dict) -> tuple:
    return (ev["ts"], ev.get("arrival_seq", 0))


def normalize_event(raw: dict, arrival_seq: int) -> dict:
    """把外部事件规整成引擎内部形态；不做任何“补事件”。"""
    ev = {
        "id": raw.get("id"),
        "gen": raw["gen"],
        "ts": raw["ts"],
        "type": raw["type"],
        "thread": raw.get("thread"),
        "lock": raw.get("lock"),
        "cond": raw.get("cond"),
        "mode": raw.get("mode", "X"),
        "stack": list(raw.get("stack") or []),
        "arrival_seq": raw.get("arrival_seq", arrival_seq),
        "out_of_order": bool(raw.get("out_of_order", False)),
        "suppressed": bool(raw.get("suppressed", False)),
    }
    if ev["mode"] not in ("R", "X"):
        ev["mode"] = "X"
    return ev


# ---------------------------------------------------------------------------
# 运行期数据结构
# ---------------------------------------------------------------------------


@dataclass
class Waiter:
    thread: str
    lock: str
    mode: str
    req_id: Any
    req_ts: float
    req_seq: int
    req_stack: list
    status: str = "waiting"          # waiting / done / canceled
    confidence: str = HIGH
    causes: list = field(default_factory=list)   # 记录的 holder id 序列
    # cond 等待：
    cond: Optional[str] = None
    saved_mode: Optional[str] = None
    cond_req_stack: Optional[list] = None
    notify_id: Any = None
    notify_ts: Optional[float] = None


@dataclass
class Hold:
    lock: str
    thread: str
    mode: str
    count: int
    grant_id: Any
    grant_ts: float
    grant_seq: int
    req_id: Any
    req_stack: list
    grant_stack: list
    confidence: str
    cond: Optional[str] = None


@dataclass
class Anomaly:
    kind: str
    severity: str
    message: str
    event_id: Any = None
    gen: Any = None
    thread: Optional[str] = None
    lock: Optional[str] = None


@dataclass
class ThreadState:
    alive: bool = True
    started: bool = False
    ended: bool = False


class Replayer:
    """按 (ts, arrival_seq) 重放事件，维护锁/线程/条件变量状态。"""

    def __init__(self, events: list[dict], lock_types: Optional[dict] = None,
                 gen: Any = None):
        self.gen = gen
        self.lock_types = dict(lock_types or {})
        norm = [normalize_event(e, i) for i, e in enumerate(events)]
        if gen is not None:
            norm = [e for e in norm if e["gen"] == gen]
        # 乱序标记：到达序与时间序不一致
        max_seq = -1
        by_arrival = sorted(norm, key=lambda e: e["arrival_seq"])
        for e in by_arrival:
            if e["arrival_seq"] < max_seq:
                e["out_of_order"] = True
            max_seq = max(max_seq, e["arrival_seq"])
        self.events = sorted(norm, key=sort_key)
        self._reset()

    def _reset(self):
        self.holders: dict[str, list[Hold]] = defaultdict(list)
        self.waiters: dict[str, list[Waiter]] = defaultdict(list)
        self.cond_waits: dict[str, list[Waiter]] = defaultdict(list)
        self.thread_states: dict[str, ThreadState] = defaultdict(ThreadState)
        self.anomalies: list[Anomaly] = []
        self.last_cause: dict[Any, str] = {}
        self.edge_history: dict[tuple, dict] = {}
        self.cycle_log: list[dict] = []
        self._snap_index: list[tuple] = []

    # -- 小工具 -------------------------------------------------------------

    def _anom(self, kind: str, severity: str, msg: str, ev: dict, lock=None):
        self.anomalies.append(Anomaly(
            kind=kind, severity=severity, message=msg, event_id=ev["id"],
            gen=ev["gen"], thread=ev.get("thread"),
            lock=lock if lock is not None else ev.get("lock"),
        ))

    def _holds(self, lock: str) -> list[Hold]:
        return self.holders.get(lock, [])

    def _own_hold(self, lock: str, thread: str, mode: str):
        kind = lock_kind(self.lock_types, lock)
        for h in self._holds(lock):
            if h.thread != thread:
                continue
            if kind == "rwlock":
                if mode == "R" and h.mode == "R":
                    return h
                if h.mode == "X":
                    return h
            else:
                return h
        return None

    def _incompat(self, lock: str, mode: str):
        kind = lock_kind(self.lock_types, lock)
        out = []
        for h in self._holds(lock):
            if kind == "rwlock" and mode == "R" and h.mode == "R":
                continue
            out.append(h)
        return out

    def _granted_count(self, thread: str, lock: str, mode: str) -> int:
        kind = lock_kind(self.lock_types, lock)
        total = 0
        for h in self._holds(lock):
            if h.thread != thread:
                continue
            if kind == "rwlock":
                if mode == "R" and h.mode == "R":
                    total += h.count
                elif h.mode == "X":
                    total += h.count
            else:
                total += h.count
        return total

    def _active_wait(self, thread: str, lock: str):
        for w in self.waiters.get(lock, []):
            if w.thread == thread and w.status == "waiting":
                return w
        return None

    def _all_active_waits(self, thread: str):
        return [w for ws in self.waiters.values() for w in ws
                if w.thread == thread and w.status == "waiting"]

    def _add_hold(self, lock, thread, mode, count, ev, req, confidence, cond=None):
        hold = Hold(
            lock=lock, thread=thread, mode=mode, count=count,
            grant_id=ev["id"], grant_ts=ev["ts"], grant_seq=ev["arrival_seq"],
            req_id=req.get("id") if req else None,
            req_stack=list(req.get("stack") or []) if req else list(ev.get("stack") or []),
            grant_stack=list(ev.get("stack") or []),
            confidence=confidence, cond=cond,
        )
        self.holders[lock].append(hold)
        return hold

    def _drop_waiter(self, waiter: Waiter, status: str):
        waiter.status = status

    def _check_dead(self, ev: dict) -> bool:
        ts = self.thread_states[ev["thread"]]
        if ts.ended:
            self._anom("event_after_thread_end", SEV_WARN,
                       f"线程 {ev['thread']} 已结束后仍出现 {ev['type']}", ev)
            return True
        return False

    # -- 各事件类型处理 -----------------------------------------------------

    def on_thread_start(self, ev):
        st = self.thread_states[ev["thread"]]
        if st.started:
            self._anom("duplicate_thread_start", SEV_WARN,
                       f"线程 {ev['thread']} 重复 thread_start", ev)
        st.alive = True
        st.started = True
        st.ended = False

    def on_thread_end(self, ev):
        st = self.thread_states[ev["thread"]]
        st.ended = True
        st.alive = False
        # 终止撤销所有等待（锁等待与 cond 等待）
        for lock, ws in list(self.waiters.items()):
            for w in ws:
                if w.thread == ev["thread"] and w.status == "waiting":
                    self._drop_waiter(w, "canceled")
                    self._anom("wait_canceled_on_exit", SEV_INFO,
                               f"线程 {ev['thread']} 退出，撤销对 {lock} 的等待", ev, lock)
        for cname, ws in list(self.cond_waits.items()):
            for w in ws:
                if w.thread == ev["thread"] and w.status == "waiting":
                    self._drop_waiter(w, "canceled")
                    self._anom("cond_canceled_on_exit", SEV_INFO,
                               f"线程 {ev['thread']} 退出，离开 cond {cname}", ev)

    def on_acquire_request(self, ev):
        if self._check_dead(ev):
            return
        lock, thread, mode = ev["lock"], ev["thread"], ev["mode"]
        kind = lock_kind(self.lock_types, lock)
        own = self._own_hold(lock, thread, mode)
        if own is not None:
            if kind == "reentrant":
                # 递归加锁：不产生等待边
                own.count += 1
                return
            if kind == "mutex":
                self._anom("recursive_nonreentrant", SEV_WARN,
                           f"线程 {thread} 对非重入锁 {lock} 递归请求", ev)
                # 继续按候选状态处理：自等待
        if self._active_wait(thread, lock):
            self._anom("duplicate_request", SEV_WARN,
                       f"线程 {thread} 已在等待 {lock}，重复请求", ev)
            return
        conf = LOW if ev["out_of_order"] else HIGH
        waiter = Waiter(
            thread=thread, lock=lock, mode=mode, req_id=ev["id"],
            req_ts=ev["ts"], req_seq=ev["arrival_seq"],
            req_stack=list(ev["stack"]), confidence=conf,
        )
        self.waiters[lock].append(waiter)
        self.last_cause[ev["id"]] = waiter.thread
        if own is not None and kind == "mutex":
            self._anom("self_deadlock_candidate", SEV_ERROR,
                       f"线程 {thread} 再次请求自己持有的非重入锁 {lock}", ev)

    def on_acquire_granted(self, ev):
        if self._check_dead(ev):
            return
        lock, thread, mode = ev["lock"], ev["thread"], ev["mode"]
        kind = lock_kind(self.lock_types, lock)
        waiter = self._active_wait(thread, lock)
        req = None
        confidence = HIGH
        if waiter is None:
            # cond_granted 之后的 reacquire 会复用一个特殊 waiter；
            # 走到这里说明缺少 acquire_request
            confidence = LOW
            self._anom("grant_without_request", SEV_WARN,
                       f"锁 {lock} 授予 {thread}，但没有对应 acquire_request", ev)
            waiter = Waiter(
                thread=thread, lock=lock, mode=mode, req_id=None,
                req_ts=ev["ts"], req_seq=ev["arrival_seq"],
                req_stack=list(ev["stack"]), confidence=LOW,
            )
            self.waiters[lock].append(waiter)
        else:
            req = {"id": waiter.req_id, "stack": waiter.req_stack,
                   "ts": waiter.req_ts}
            if waiter.confidence == LOW or ev["out_of_order"]:
                confidence = MEDIUM
        own = self._own_hold(lock, thread, mode)
        if own is not None:
            if kind == "reentrant" or (kind == "rwlock" and own.mode == "X"):
                own.count += 1
                self._drop_waiter(waiter, "done")
                return
            if kind == "rwlock" and mode == "R" and own.mode == "R":
                own.count += 1
                self._drop_waiter(waiter, "done")
                return
            self._anom("grant_conflict", SEV_ERROR,
                       f"锁 {lock} 授予已以冲突方式持锁的线程 {thread}", ev)
        if self._incompat(lock, mode) and not own:
            # 显式授予在冲突状态下仍照做（信任事件），但置信降级、留异常
            confidence = MEDIUM if confidence == HIGH else confidence
            self._anom("grant_while_held", SEV_WARN,
                       f"锁 {lock} 在仍被冲突持有时授予 {thread}", ev)
        self._add_hold(lock, thread, mode, 1, ev, req,
                       min_conf(confidence, waiter.confidence))
        self._drop_waiter(waiter, "done")

    def on_release(self, ev):
        if self._check_dead(ev):
            return
        lock, thread = ev["lock"], ev["thread"]
        kind = lock_kind(self.lock_types, lock)
        own = self._own_hold(lock, thread, "X")
        if own is None and kind == "rwlock":
            own = self._own_hold(lock, thread, "R")
        if own is None:
            self._anom("release_without_hold", SEV_ERROR,
                       f"线程 {thread} 释放未持有的锁 {lock}（不凭空补 release）", ev)
            return
        own.count -= 1
        if own.count <= 0:
            self.holders[lock] = [h for h in self.holders[lock] if h is not own]

    def on_acquire_timeout(self, ev):
        if self._check_dead(ev):
            return
        lock, thread = ev["lock"], ev["thread"]
        waiter = self._active_wait(thread, lock)
        if waiter is None:
            self._anom("timeout_without_wait", SEV_WARN,
                       f"线程 {thread} 对 {lock} 的超时事件没有对应等待", ev)
            return
        self._drop_waiter(waiter, "timeout")

    def on_cond_wait(self, ev):
        if self._check_dead(ev):
            return
        lock, thread, mode = ev["lock"], ev["thread"], ev["mode"]
        own = self._own_hold(lock, thread, mode)
        kind = lock_kind(self.lock_types, lock)
        if own is None:
            self._anom("cond_wait_without_lock", SEV_ERROR,
                       f"线程 {thread} 在未持有 {lock} 时进入 cond_wait", ev)
        # 若有未决 acquire_request 等待同一把锁，视为异常并撤销
        dup = self._active_wait(thread, lock)
        if dup is not None:
            self._anom("cond_wait_during_pending", SEV_WARN,
                       f"线程 {thread} 在等待 {lock} 期间进入 cond_wait", ev)
            self._drop_waiter(dup, "canceled")
        saved_mode = own.mode if own else mode
        saved_stack = list(own.grant_stack) if own else list(ev["stack"])
        waiter = Waiter(
            thread=thread, lock=lock, mode=mode, req_id=ev["id"],
            req_ts=ev["ts"], req_seq=ev["arrival_seq"],
            req_stack=list(ev["stack"]), confidence=HIGH,
            cond=ev["cond"], saved_mode=saved_mode,
            cond_req_stack=saved_stack,
        )
        # cond_wait 原子地暂时释放锁（按层级计数全部释放）
        if own is not None:
            self.holders[lock] = [h for h in self.holders[lock] if h is not own]
        self.cond_waits[ev["cond"]].append(waiter)

    def on_cond_notify(self, ev):
        cname = ev["cond"]
        q = [w for w in self.cond_waits.get(cname, []) if w.status == "waiting"]
        if not q:
            self._anom("notify_nobody", SEV_INFO,
                       f"cond {cname} notify 时没有等待线程", ev)
            return
        target = q[0]
        target.notify_id = ev["id"]
        target.notify_ts = ev["ts"]
        # 从 cond 队列移动到锁等待队列，重新竞争
        target.status = "waiting"
        self.cond_waits[cname].remove(target)
        self.waiters[target.lock].append(target)

    def on_cond_granted(self, ev):
        if self._check_dead(ev):
            return
        lock, thread = ev["lock"], ev["thread"]
        waiter = self._active_wait(thread, lock)
        confidence = HIGH
        if waiter is None or waiter.cond is None:
            # 没有 notify 痕迹的重新获得
            confidence = LOW
            self._anom("cond_grant_without_notify", SEV_WARN,
                       f"线程 {thread} 从 cond 重新获得 {lock}，缺少 wait/notify 链", ev)
            waiter = Waiter(
                thread=thread, lock=lock, mode=ev["mode"], req_id=ev["id"],
                req_ts=ev["ts"], req_seq=ev["arrival_seq"],
                req_stack=list(ev["stack"]), confidence=LOW,
                saved_mode=ev["mode"],
            )
            self.waiters[lock].append(waiter)
        mode = waiter.saved_mode or ev["mode"]
        own = self._own_hold(lock, thread, mode)
        if own is not None:
            if lock_kind(self.lock_types, lock) in ("reentrant",):
                own.count += 1
                self._drop_waiter(waiter, "done")
                return
            self._anom("cond_grant_conflict", SEV_WARN,
                       f"cond 重新获得 {lock} 时 {thread} 已持锁", ev)
            confidence = MEDIUM
        req = {"id": waiter.req_id, "stack": waiter.cond_req_stack or waiter.req_stack,
               "ts": waiter.req_ts}
        hold = self._add_hold(lock, thread, mode, 1, ev, req,
                              min_conf(confidence, waiter.confidence),
                              cond=waiter.cond)
        hold.grant_stack = list(ev["stack"])
        self._drop_waiter(waiter, "done")

    _DISPATCH = None

    def _step(self, ev):
        handler = {
            "thread_start": self.on_thread_start,
            "thread_end": self.on_thread_end,
            "acquire_request": self.on_acquire_request,
            "acquire_granted": self.on_acquire_granted,
            "acquire_timeout": self.on_acquire_timeout,
            "release": self.on_release,
            "cond_wait": self.on_cond_wait,
            "cond_notify": self.on_cond_notify,
            "cond_granted": self.on_cond_granted,
        }.get(ev["type"])
        if handler is None:
            self._anom("unknown_event", SEV_WARN, f"未知事件类型 {ev['type']}", ev)
            return
        handler(ev)

    # -- 时间线：环 / 近失 --------------------------------------------------

    def timeline(self) -> list[dict]:
        """在每个事件时点抽样，记录环与“曾成环后被打破”的近失片段。"""
        self._reset()
        ticks = []
        active_sig: dict[tuple, dict] = {}
        closed_near_misses = []
        prev_edges_sig = set()
        for ev in self.events:
            if ev["suppressed"]:
                continue
            self._step(ev)
            edges = self._wait_for_edges()
            cycles = find_cycles(edges)
            edge_sig = {(e["from_thread"], e["to_thread"], e["lock"]) for e in edges}
            appeared = edge_sig - prev_edges_sig
            disappeared = prev_edges_sig - edge_sig
            prev_edges_sig = edge_sig
            now_sigs = {tuple(c["nodes"]): c for c in cycles}
            ticks.append({
                "ts": ev["ts"],
                "event_id": ev["id"],
                "type": ev["type"],
                "thread": ev["thread"],
                "lock": ev["lock"],
                "cycle_count": len(cycles),
                "cycle_threads": [list(c["nodes"]) for c in cycles],
                "edge_appeared": sorted(appeared),
                "edge_disappeared": sorted(disappeared),
                "wait_edges": [
                    {"from": e["from_thread"], "to": e["to_thread"],
                     "lock": e["lock"], "confidence": e["confidence"]}
                    for e in edges
                ],
            })
            # 维护环的生命周期
            ended_now = []
            for sig, info in list(active_sig.items()):
                if sig not in now_sigs:
                    info["end_ts"] = ev["ts"]
                    info["end_event"] = ev["id"]
                    closed_near_misses.append(info)
                    ended_now.append(sig)
            for sig in ended_now:
                active_sig.pop(sig, None)
            for sig, cyc in now_sigs.items():
                if sig not in active_sig:
                    active_sig[sig] = {
                        "threads": list(sig),
                        "locks": sorted({e["lock"] for e in cyc["edges"]}),
                        "start_ts": cyc["edges"][0]["wait_ts"] if cyc["edges"] else ev["ts"],
                        "detected_ts": ev["ts"],
                        "detected_event": ev["id"],
                        "end_ts": None,
                        "end_event": None,
                        "status": "active",
                        "confidence": reduce(min_conf,
                                             [e["confidence"] for e in cyc["edges"]]),
                    }
        for info in active_sig.values():
            info["status"] = "open"
            closed_near_misses.append(info)
        for info in closed_near_misses:
            if info["end_ts"] is not None:
                info["status"] = "near_miss"
        return {
            "ticks": ticks,
            "cycle_spans": sorted(
                closed_near_misses,
                key=lambda s: (s["start_ts"], s["detected_ts"]),
            ),
        }

    def replay_all(self) -> dict:
        tl = self.timeline()
        last_ts = self.events[-1]["ts"] if self.events else 0.0
        snap = self.snapshot(last_ts)
        tl["latest"] = snap
        tl["time_range"] = {
            "min": self.events[0]["ts"] if self.events else 0.0,
            "max": last_ts,
        }
        return tl

    # -- 快照：持有图 / 等待图 / 候选置信 ----------------------------------

    def _wait_for_edges(self):
        """线程 -> 线程 的 wait-for 边（含证据与置信度）。"""
        edges = []
        for lock, ws in self.waiters.items():
            active = [w for w in ws if w.status == "waiting"]
            if not active:
                continue
            kind = lock_kind(self.lock_types, lock)
            holders = self._holds(lock)
            # FIFO 队列序
            active.sort(key=lambda w: (w.req_ts, w.req_seq))
            blocked_seen = set()
            for w in active:
                blockers = []
                if kind == "rwlock" and w.mode == "R":
                    blockers = [h for h in holders if h.mode != "R" and h.thread != w.thread]
                else:
                    blockers = [h for h in holders if h.thread != w.thread]
                # 自死锁：自己持有的冲突锁
                self_block = self._own_hold(lock, w.thread,
                                            "X" if w.mode != "R" else "R")
                if kind == "mutex" and self_block is not None:
                    blockers = blockers + [self_block]
                if not blockers:
                    # 没有持有者：可能乱序（release/grant 尚未到）。
                    # 连到队列中更早的同锁请求，标 LOW 候选边。
                    if w is not active[0]:
                        earlier = active[0]
                        if earlier.thread != w.thread:
                            edges.append(self._wf_edge(w, lock, None, earlier, LOW,
                                                       "missing_owner"))
                    continue
                for h in blockers:
                    edges.append(self._wf_edge(w, lock, h, None,
                                               min_conf(w.confidence, h.confidence),
                                               "owner"))
                    blocked_seen.add((w.thread, h.thread))
        return edges

    def _wf_edge(self, waiter, lock, hold=None, other_waiter=None,
                 confidence=HIGH, reason="owner"):
        if hold is not None:
            blocker_thread = hold.thread
            hold_ev = hold.grant_id
            hold_stack = list(hold.grant_stack)
            hold_ts = hold.grant_ts
        else:
            blocker_thread = other_waiter.thread
            hold_ev = other_waiter.req_id
            hold_stack = list(other_waiter.req_stack)
            hold_ts = other_waiter.req_ts
        return {
            "from_thread": waiter.thread,
            "to_thread": blocker_thread,
            "lock": lock,
            "mode": waiter.mode,
            "confidence": confidence,
            "reason": reason,
            "wait_event": waiter.req_id,
            "wait_ts": waiter.req_ts,
            "wait_stack": list(waiter.req_stack),
            "block_event": hold_ev,
            "block_ts": hold_ts,
            "block_stack": hold_stack,
            "cond": waiter.cond,
        }


    def snapshot(self, at_ts: float) -> dict:
        """重放到 at_ts（含 <= at_ts 的事件），返回图与环路证据。"""
        self._reset()
        applied = []
        for ev in self.events:
            if ev["suppressed"]:
                continue
            if ev["ts"] <= at_ts:
                self._step(ev)
                applied.append(ev)
            else:
                break
        return self._build_snapshot(at_ts, applied)

    def _build_snapshot(self, at_ts, applied) -> dict:
        hold_edges = []
        for lock, hs in self.holders.items():
            for h in hs:
                hold_edges.append({
                    "thread": h.thread,
                    "lock": lock,
                    "mode": h.mode,
                    "count": h.count,
                    "confidence": h.confidence,
                    "grant_event": h.grant_id,
                    "grant_ts": h.grant_ts,
                    "request_event": h.req_id,
                    "hold_stack": list(h.grant_stack),
                    "acquire_stack": list(h.req_stack),
                    "cond": h.cond,
                })
        wait_edges = self._wait_for_edges()
        cycles = find_cycles(wait_edges)
        # 线程级等待/持有栈汇总
        per_thread = defaultdict(lambda: {"holds": [], "waits": []})
        for e in hold_edges:
            per_thread[e["thread"]]["holds"].append(e)
        for e in wait_edges:
            per_thread[e["from_thread"]]["waits"].append(e)
        cycle_reports = []
        for cyc in cycles:
            edges = cyc["edges"]
            involved_locks = sorted({e["lock"] for e in edges})
            threads = cyc["nodes"]
            report = {
                "threads": threads,
                "locks": involved_locks,
                "confidence": reduce(min_conf, [e["confidence"] for e in edges])
                if edges else HIGH,
                "edges": edges,
                "hold_stacks": {
                    t: [
                        {
                            "lock": h["lock"], "mode": h["mode"],
                            "count": h["count"],
                            "hold_stack": h["hold_stack"],
                            "acquire_stack": h["acquire_stack"],
                            "grant_event": h["grant_event"],
                            "request_event": h["request_event"],
                        }
                        for h in per_thread[t]["holds"]
                    ]
                    for t in threads
                },
                "wait_stacks": {
                    t: [
                        {
                            "lock": w["lock"], "mode": w["mode"],
                            "wait_stack": w["wait_stack"],
                            "wait_event": w["wait_event"],
                            "blocked_by": w["to_thread"],
                            "block_event": w["block_event"],
                            "block_stack": w["block_stack"],
                            "confidence": w["confidence"],
                        }
                        for w in per_thread[t]["waits"]
                    ]
                    for t in threads
                },
            }
            cycle_reports.append(report)
        return {
            "at_ts": at_ts,
            "hold_graph": hold_edges,
            "wait_graph": wait_edges,
            "cycles": cycle_reports,
            "anomalies": [a.__dict__ for a in self.anomalies],
            "threads": {
                t: {"alive": s.alive, "ended": s.ended}
                for t, s in self.thread_states.items()
            },
            "pending_cond_waits": [
                {
                    "cond": c, "thread": w.thread, "lock": w.lock,
                    "event": w.req_id, "ts": w.req_ts,
                    "stack": list(w.req_stack),
                }
                for c, ws in self.cond_waits.items() for w in ws
                if w.status == "waiting"
            ],
        }


# ---------------------------------------------------------------------------
# 环路检测：枚举线程 wait-for 图中的简单环（去重）
# ---------------------------------------------------------------------------


def find_cycles(wait_edges: list[dict]) -> list[dict]:
    adj = defaultdict(list)
    edge_map = {}
    nodes = set()
    for e in wait_edges:
        u, v = e["from_thread"], e["to_thread"]
        nodes.add(u)
        nodes.add(v)
        if (u, v) not in edge_map:
            adj[u].append(v)
            edge_map[(u, v)] = e
    found = {}

    def add_cycle(path_nodes, path_edges):
        # 规范化：从最小节点开始旋转
        start = min(range(len(path_nodes)), key=lambda i: path_nodes[i])
        rot_nodes = path_nodes[start:] + path_nodes[:start]
        rot_edges = path_edges[start:] + path_edges[:start]
        key = tuple(rot_nodes)
        if key not in found:
            found[key] = {"nodes": list(rot_nodes), "edges": list(rot_edges)}

    for a in list(nodes):
        # 自环
        if (a, a) in edge_map:
            add_cycle([a], [edge_map[(a, a)]])
        stack = [(a, [a], [], {a})]
        while stack:
            node, path, epath, seen = stack.pop()
            for nxt in adj.get(node, []):
                if nxt == a and len(path) >= 2:
                    add_cycle(path, epath + [edge_map[(node, nxt)]])
                elif nxt not in seen and nxt >= a:
                    stack.append((nxt, path + [nxt],
                                  epath + [edge_map[(node, nxt)]],
                                  seen | {nxt}))
    return list(found.values())
