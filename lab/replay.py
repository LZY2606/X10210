"""带置信状态的锁序重放状态机。

输入是已按“代次 + 原始到达顺序（时间戳乱序打 late 标记）”排序的事件。
输出某一时刻的持有图 / 等待图快照、异常与环路证据。

设计原则：

* 缺事件时构建带 ``tentative`` 置信度的候选图，但绝不凭空补 release；
* 每条边都带 ``evidence`` 与时间戳，可回溯到产生它的事件；
* 可重入互斥、读写锁共享持有/升级、condition 暂时释放/重新竞争、
  超时取消与线程退出撤销等待都在此处建模。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def _stack(event: Any) -> List[str]:
    return list(event.stack or [])


@dataclass
class Waiter:
    thread: str
    mode: str  # read / write
    req_seq: int
    req_ts: Optional[float]
    req_stack: List[str]
    upgrading: bool = False


@dataclass
class Hold:
    mode: str  # read / write
    since_seq: int
    since_ts: Optional[float]
    count: int = 1  # 重入计数
    stack: List[str] = field(default_factory=list)


@dataclass
class LockState:
    kind: str  # mutex / rwlock / unknown
    holds: Dict[str, Hold] = field(default_factory=dict)  # thread -> Hold
    waiters: List[Waiter] = field(default_factory=list)

    @property
    def free(self) -> bool:
        return not self.holds

    def writer(self) -> Optional[str]:
        for tid, hold in self.holds.items():
            if hold.mode == "write":
                return tid
        return None

    def readers(self) -> List[str]:
        return [tid for tid, h in self.holds.items() if h.mode == "read"]


@dataclass
class Parked:
    """condition wait：锁已暂时释放，等待 notify 后重新竞争。"""

    thread: str
    lock: str
    cond: str
    mode: str
    snapshot: Dict[str, Hold]
    req_seq: int
    wait_seq: int
    wait_ts: Optional[float]
    wait_stack: List[str]


def _anomaly(code: str, message: str, event: Any, **extra: Any) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "code": code,
        "message": message,
        "seq": getattr(event, "seq", None),
        "type": getattr(event, "type", None),
        "thread": getattr(event, "thread", None),
        "lock": getattr(event, "lock", None),
        "gen": getattr(event, "gen", None),
        "ts": getattr(event, "ts", None),
    }
    item.update(extra)
    return item


class Replayer:
    """逐事件重放，``snapshot()`` 给出当前持有图/等待图。"""

    def __init__(self, lock_types: Optional[Dict[str, str]] = None) -> None:
        self.lock_types: Dict[str, str] = dict(lock_types or {})
        self.threads: Dict[str, Dict[str, Any]] = {}
        self.locks: Dict[str, LockState] = {}
        self.parked: Dict[str, Parked] = {}  # thread -> Parked
        self.events: List[Any] = []
        self.anomalies: List[Dict[str, Any]] = []
        # seq -> 前置 seq 列表（配对 / condition 语义，供最小化闭包使用）
        self.pairs: Dict[int, List[int]] = {}
        self._cur_gen: Optional[str] = None

    # ---- 基础工具 -------------------------------------------------------

    def _thread_state(self, tid: str) -> Dict[str, Any]:
        st = self.threads.get(tid)
        if st is None:
            st = {"alive": False, "started_seq": None, "ended_seq": None}
            self.threads[tid] = st
        return st

    def _lock(self, lock: str, event: Any) -> LockState:
        ls = self.locks.get(lock)
        if ls is None:
            kind = self.lock_types.get(lock, "unknown")
            ls = LockState(kind=kind)
            self.locks[lock] = ls
        return ls

    def _waiter(self, ls: LockState, tid: str) -> Optional[Waiter]:
        for waiter in ls.waiters:
            if waiter.thread == tid:
                return waiter
        return None

    def _relate(self, seq: int, prereq: Optional[int]) -> None:
        if prereq is None:
            return
        self.pairs.setdefault(seq, [])
        if prereq not in self.pairs[seq]:
            self.pairs[seq].append(prereq)

    def _mode_conflict(self, ls: LockState, mode: str, requester: str) -> bool:
        if ls.kind != "rwlock":
            return True
        if mode == "read":
            return any(t != requester for t in ls.holds)
        return any(t != requester for t in ls.holds)

    def _blocked_by(self, ls: LockState, waiter: Waiter) -> List[str]:
        """快照时判定某等待者被哪些持有者阻塞。"""
        tid = waiter.thread
        if waiter.upgrading:
            return [t for t in ls.holds if t != tid]
        if tid in ls.holds:
            return []  # 可重入，自满足
        if waiter.mode == "read" and ls.kind == "rwlock":
            return [
                t for t, h in ls.holds.items() if h.mode == "write" and t != tid
            ]
        return [t for t in ls.holds if t != tid]

    # ---- 事件入口 -------------------------------------------------------

    def feed(self, event: Any) -> None:
        if self._cur_gen is not None and event.gen != self._cur_gen:
            self._reset_runtime()
        self._cur_gen = event.gen
        self.events.append(event)
        handler = getattr(self, "_on_" + event.type, None)
        if handler is None:
            self.anomalies.append(
                _anomaly("unknown_event", "未知事件类型 %r" % event.type, event)
            )
            return
        handler(event)

    def _reset_runtime(self) -> None:
        self.locks.clear()
        self.parked.clear()
        self.threads.clear()

    # ---- 生命周期 -------------------------------------------------------

    def _on_restart(self, event: Any) -> None:
        self._reset_runtime()

    def _on_thread_start(self, event: Any) -> None:
        st = self._thread_state(event.thread)
        if st["alive"]:
            self.anomalies.append(
                _anomaly("thread_restart", "线程重复开始", event)
            )
        st["alive"] = True
        st["started_seq"] = event.seq

    def _on_thread_end(self, event: Any) -> None:
        tid = event.thread
        st = self._thread_state(tid)
        st["alive"] = False
        st["ended_seq"] = event.seq
        # 线程终止撤销所有等待（锁等待 + condition 等待）
        for lock, ls in list(self.locks.items()):
            before = len(ls.waiters)
            ls.waiters = [w for w in ls.waiters if w.thread != tid]
            if len(ls.waiters) != before:
                self.anomalies.append(
                    _anomaly(
                        "wait_revoked_exit",
                        "线程退出，锁等待被撤销",
                        event,
                        lock=lock,
                    )
                )
        if tid in self.parked:
            parked = self.parked.pop(tid)
            self._relate(event.seq, parked.wait_seq)
            self.anomalies.append(
                _anomaly(
                    "cond_wait_revoked_exit",
                    "线程在 condition 等待中退出，等待撤销",
                    event,
                    lock=parked.lock,
                    cond=parked.cond,
                )
            )
        leaked = [lock for lock, ls in self.locks.items() if tid in ls.holds]
        # 线程死了仍然保留持锁边（泄漏候选），但记录异常，绝不替它释放
        for lock in leaked:
            self.anomalies.append(
                _anomaly(
                    "thread_exit_holding",
                    "线程退出时仍持有锁（缺 release，保留为候选状态）",
                    event,
                    lock=lock,
                )
            )

    # ---- 锁请求 / 成功 / 释放 ------------------------------------------

    def _on_request(self, event: Any) -> None:
        tid = event.thread
        lock = event.lock
        st = self._thread_state(tid)
        if st["alive"] is False and st["ended_seq"] is not None:
            self.anomalies.append(
                _anomaly("request_after_exit", "线程已终止仍发起请求", event)
            )
        if not lock:
            self.anomalies.append(_anomaly("missing_lock", "请求缺少锁地址", event))
            return
        ls = self._lock(lock, event)
        if self._waiter(ls, tid) is not None or tid in self.parked:
            self.anomalies.append(
                _anomaly("duplicate_request", "线程重复请求同一把锁", event)
            )
            return
        mode = "write" if ls.kind != "rwlock" else event.mode
        waiter = Waiter(
            thread=tid,
            mode=mode,
            req_seq=event.seq,
            req_ts=event.ts,
            req_stack=_stack(event),
            upgrading=(
                ls.kind == "rwlock"
                and mode == "write"
                and tid in ls.holds
                and ls.holds[tid].mode == "read"
            ),
        )
        ls.waiters.append(waiter)

    def _on_acquired(self, event: Any) -> None:
        tid = event.thread
        lock = event.lock
        if not lock:
            self.anomalies.append(_anomaly("missing_lock", "成功事件缺少锁地址", event))
            return
        ls = self._lock(lock, event)
        waiter = self._waiter(ls, tid)
        mode = event.mode
        if waiter is None:
            if tid in ls.holds:
                # 可重入获取，只显式看到 acquired：增加计数，候选状态
                hold = ls.holds[tid]
                hold.count += 1
                self._relate(event.seq, hold.since_seq)
                self.anomalies.append(
                    _anomaly(
                        "acquired_without_request",
                        "可重入成功缺少 request 事件，按重入计数",
                        event,
                    )
                )
                return
            self.anomalies.append(
                _anomaly(
                    "acquired_without_request",
                    "成功事件找不到等待中的 request，建立候选持有",
                    event,
                )
            )
            ls.holds[tid] = Hold(mode, event.seq, event.ts)
            return
        self._relate(event.seq, waiter.req_seq)
        idx = ls.waiters.index(waiter)
        ls.waiters.pop(idx)
        if tid in ls.holds:
            hold = ls.holds[tid]
            hold.count += 1
            if ls.kind != "rwlock" or mode == "write":
                hold.mode = "write"
        else:
            ls.holds[tid] = Hold(
                "read" if ls.kind == "rwlock" and waiter.mode == "read" else "write",
                waiter.req_seq,
                waiter.req_ts,
                stack=list(waiter.req_stack),
            )

    def _on_release(self, event: Any) -> None:
        tid = event.thread
        lock = event.lock
        if not lock:
            self.anomalies.append(_anomaly("missing_lock", "释放事件缺少锁地址", event))
            return
        ls = self._lock(lock, event)
        hold = ls.holds.get(tid)
        if hold is None:
            self.anomalies.append(
                _anomaly(
                    "release_without_hold",
                    "释放未持有的锁（不凭空建立状态）",
                    event,
                )
            )
            return
        hold.count -= 1
        grant_seq = hold.since_seq
        if hold.count <= 0:
            del ls.holds[tid]
        self._relate(event.seq, grant_seq)

    # ---- condition ------------------------------------------------------

    def _on_cond_wait(self, event: Any) -> None:
        tid = event.thread
        lock = event.lock
        cond = event.cond or lock
        if not lock:
            self.anomalies.append(_anomaly("missing_lock", "cond_wait 缺少锁地址", event))
            return
        ls = self._lock(lock, event)
        hold = ls.holds.get(tid)
        waiter = self._waiter(ls, tid)
        if hold is None and waiter is None:
            self.anomalies.append(
                _anomaly(
                    "cond_wait_without_hold",
                    "cond_wait 时线程未持有锁（候选记录，等待 notify）",
                    event,
                )
            )
            snapshot: Dict[str, Hold] = {}
            prereq = None
        else:
            snapshot = dict(ls.holds)
            prereq = hold.since_seq if hold is not None else waiter.req_seq
        if hold is not None:
            del ls.holds[tid]
        if waiter is not None:
            ls.waiters.remove(waiter)
        self.parked[tid] = Parked(
            thread=tid,
            lock=lock,
            cond=cond or lock,
            mode=hold.mode if hold is not None else event.mode,
            snapshot=snapshot,
            req_seq=waiter.req_seq if waiter is not None else event.seq,
            wait_seq=event.seq,
            wait_ts=event.ts,
            wait_stack=_stack(event),
        )
        self._relate(event.seq, prereq)

    def _wake_parked(self, parked: Parked, event: Any, *, all_woken: bool) -> None:
        """notify 后进入重新竞争：恢复“候选持有”并排队。"""
        tid = parked.thread
        lock = parked.lock
        ls = self._lock(lock, event)
        ls.waiters.append(
            Waiter(
                thread=tid,
                mode=parked.mode,
                req_seq=parked.req_seq,
                req_ts=parked.wait_ts,
                req_stack=parked.wait_stack,
            )
        )
        self._relate(event.seq, parked.wait_seq)

    def _on_cond_notify(self, event: Any) -> None:
        cond = event.cond or event.lock
        candidates = [
            parked
            for parked in self.parked.values()
            if parked.cond == cond
            and (event.lock is None or parked.lock == event.lock)
        ]
        if not candidates:
            self.anomalies.append(
                _anomaly("notify_no_waiter", "notify 时没有 condition 等待者", event)
            )
            return
        parked = sorted(candidates, key=lambda p: (p.wait_seq, p.thread))[0]
        del self.parked[parked.thread]
        self._wake_parked(parked, event, all_woken=False)

    def _on_cond_notify_all(self, event: Any) -> None:
        cond = event.cond or event.lock
        candidates = [
            parked
            for parked in self.parked.values()
            if parked.cond == cond
            and (event.lock is None or parked.lock == event.lock)
        ]
        if not candidates:
            self.anomalies.append(
                _anomaly("notify_no_waiter", "notifyAll 时没有 condition 等待者", event)
            )
            return
        for parked in sorted(candidates, key=lambda p: p.wait_seq):
            del self.parked[parked.thread]
            self._wake_parked(parked, event, all_woken=True)

    # ---- 超时 / 取消 ----------------------------------------------------

    def _on_timeout(self, event: Any) -> None:
        tid = event.thread
        lock = event.lock
        removed = False
        target_locks = [lock] if lock else list(self.locks.keys())
        for lock_name in target_locks:
            ls = self.locks.get(lock_name)
            if ls is None:
                continue
            waiter = self._waiter(ls, tid)
            if waiter is not None:
                ls.waiters.remove(waiter)
                self._relate(event.seq, waiter.req_seq)
                removed = True
        if tid in self.parked and (lock is None or self.parked[tid].lock == lock):
            parked = self.parked.pop(tid)
            self._relate(event.seq, parked.wait_seq)
            removed = True
        if not removed:
            self.anomalies.append(
                _anomaly("timeout_without_wait", "超时事件找不到等待中的请求", event)
            )

    # ---- 快照 -----------------------------------------------------------

    def _last_event(self) -> Optional[Any]:
        return self.events[-1] if self.events else None

    def snapshot(self) -> Dict[str, Any]:
        hold_edges: List[Dict[str, Any]] = []
        wait_edges: List[Dict[str, Any]] = []
        nodes_threads = set()
        nodes_locks = set()

        for lock_name, ls in self.locks.items():
            nodes_locks.add(lock_name)
            for tid, hold in ls.holds.items():
                nodes_threads.add(tid)
                hold_edges.append(
                    {
                        "thread": tid,
                        "lock": lock_name,
                        "mode": hold.mode,
                        "kind": ls.kind,
                        "count": hold.count,
                        "gen": self._cur_gen,
                        "evidence": {
                            "request_seq": hold.since_seq,
                            "acquired_seq": hold.since_seq,
                            "ts": hold.since_ts,
                        },
                        "stack": list(hold.stack),
                    }
                )
            for waiter in ls.waiters:
                nodes_threads.add(waiter.thread)
                blockers = self._blocked_by(ls, waiter)
                for blocker in blockers:
                    wait_edges.append(
                        {
                            "thread": waiter.thread,
                            "blocker": blocker,
                            "lock": lock_name,
                            "mode": waiter.mode,
                            "kind": ls.kind,
                            "upgrading": waiter.upgrading,
                            "gen": self._cur_gen,
                            "evidence": {
                                "request_seq": waiter.req_seq,
                                "ts": waiter.req_ts,
                            },
                            "stack": list(waiter.req_stack),
                        }
                    )

        parked_edges = [
            {
                "thread": parked.thread,
                "lock": parked.lock,
                "cond": parked.cond,
                "mode": parked.mode,
                "gen": self._cur_gen,
                "evidence": {
                    "wait_seq": parked.wait_seq,
                    "request_seq": parked.req_seq,
                    "ts": parked.wait_ts,
                },
                "stack": list(parked.wait_stack),
            }
            for parked in self.parked.values()
        ]

        cycles = self._detect_cycles(wait_edges)

        lock_rows = []
        for lock_name, ls in self.locks.items():
            lock_rows.append(
                {
                    "lock": lock_name,
                    "kind": ls.kind,
                    "holders": [
                        {"thread": tid, "mode": hold.mode, "count": hold.count}
                        for tid, hold in ls.holds.items()
                    ],
                    "waiters": [
                        {
                            "thread": w.thread,
                            "mode": w.mode,
                            "request_seq": w.req_seq,
                            "upgrading": w.upgrading,
                        }
                        for w in ls.waiters
                    ],
                }
            )

        late_events = [e.seq for e in self.events if getattr(e, "late", False)]
        tentative_codes = {
            "acquired_without_request",
            "release_without_hold",
            "cond_wait_without_hold",
            "duplicate_request",
            "request_after_exit",
            "unknown_event",
        }
        tentative = [a for a in self.anomalies if a["code"] in tentative_codes]
        if tentative or late_events:
            confidence = "tentative"
            reasons = sorted({a["code"] for a in tentative})
            if late_events:
                reasons.append("out_of_order")
        else:
            confidence = "high"
            reasons = []

        last = self._last_event()
        return {
            "gen": self._cur_gen,
            "last_seq": last.seq if last else 0,
            "last_ts": last.ts if last else None,
            "confidence": confidence,
            "confidence_reasons": reasons,
            "threads": [
                {
                    "thread": tid,
                    "alive": st["alive"],
                    "started_seq": st["started_seq"],
                    "ended_seq": st["ended_seq"],
                }
                for tid, st in sorted(self.threads.items())
            ],
            "locks": lock_rows,
            "nodes": {
                "threads": sorted(nodes_threads),
                "locks": sorted(nodes_locks),
            },
            "hold_edges": hold_edges,
            "wait_edges": wait_edges,
            "parked_edges": parked_edges,
            "cycles": cycles,
            "anomalies": list(self.anomalies),
        }

    def _detect_cycles(self, wait_edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        # 线程级阻塞图：thread -> [(blocker, edge)]
        adj: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
        for edge in wait_edges:
            if edge["thread"] == edge["blocker"]:
                continue
            adj.setdefault(edge["thread"], []).append((edge["blocker"], edge))

        nodes = sorted(adj.keys() | {b for bs in adj.values() for b, _ in bs})
        found: List[List[str]] = []
        seen_signatures = set()

        def dfs(start: str, current: str, path: List[str], edges: List[Dict[str, Any]]) -> None:
            for nxt, edge in sorted(adj.get(current, []), key=lambda x: x[0]):
                if nxt == start and len(path) >= 2:
                    cycle = list(path)
                    signature = self._cycle_signature(cycle)
                    if signature not in seen_signatures:
                        seen_signatures.add(signature)
                        found.append(cycle)
                elif nxt not in path and nxt >= start:
                    path.append(nxt)
                    edges.append(edge)
                    dfs(start, nxt, path, edges)
                    path.pop()
                    edges.pop()

        for node in nodes:
            dfs(node, node, [node], [])

        # 每个线程选一把“构成环证据”的持有锁：优先它正等待的锁的前驱冲突，
        # 即环中它在下一步要获取的锁由别人持有；这里取该线程任一持有的环中锁。
        cycle_locks = {edge["lock"] for edge in wait_edges}
        holds_by_thread: Dict[str, Dict[str, Any]] = {}
        holds_by_tl: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for edge in self._hold_lookup_edges():
            holds_by_tl[(edge["thread"], edge["lock"])] = edge
            if edge["lock"] in cycle_locks:
                holds_by_thread.setdefault(edge["thread"], edge)

        result = []
        for cycle in found:
            steps = []
            locks = []
            for i, tid in enumerate(cycle):
                nxt = cycle[(i + 1) % len(cycle)]
                wait_edge = None
                for candidate in wait_edges:
                    if candidate["thread"] == tid and candidate["blocker"] == nxt:
                        wait_edge = candidate
                        break
                hold_edge = holds_by_thread.get(tid)
                if wait_edge and wait_edge["upgrading"]:
                    # 升级场景：线程持有的正是等待升级的同一把锁
                    hold_edge = holds_by_tl.get((tid, wait_edge["lock"]))
                if wait_edge:
                    locks.append(wait_edge["lock"])
                steps.append(
                    {
                        "thread": tid,
                        "waiting_for_lock": wait_edge["lock"] if wait_edge else None,
                        "blocker": nxt,
                        "wait_stack": list(wait_edge["stack"]) if wait_edge else [],
                        "wait_evidence": dict(wait_edge["evidence"]) if wait_edge else None,
                        "wait_mode": wait_edge["mode"] if wait_edge else None,
                        "upgrading": wait_edge["upgrading"] if wait_edge else False,
                        "holding_lock": hold_edge["lock"] if hold_edge else None,
                        "hold_stack": list(hold_edge["stack"]) if hold_edge else [],
                        "hold_evidence": dict(hold_edge["evidence"]) if hold_edge else None,
                        "hold_mode": hold_edge["mode"] if hold_edge else None,
                    }
                )
            signature = self._cycle_signature(cycle)
            result.append(
                {
                    "signature": signature,
                    "threads": list(cycle),
                    "locks": locks,
                    "length": len(cycle),
                    "steps": steps,
                }
            )
        return result

    def _hold_lookup_edges(self) -> List[Dict[str, Any]]:
        rows = []
        for lock_name, ls in self.locks.items():
            for tid, hold in ls.holds.items():
                rows.append(
                    {
                        "thread": tid,
                        "lock": lock_name,
                        "mode": hold.mode,
                        "stack": list(hold.stack),
                        "evidence": {"acquired_seq": hold.since_seq, "ts": hold.since_ts},
                    }
                )
        return rows

    @staticmethod
    def _cycle_signature(cycle: List[str]) -> str:
        # 旋转归一：从最小元素开始
        if not cycle:
            return ""
        pivot = min(range(len(cycle)), key=lambda i: cycle[i])
        rotated = cycle[pivot:] + cycle[:pivot]
        return "->".join(rotated) + "->" + rotated[0]
