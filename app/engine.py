"""锁序解剖室：事件 -> 持有图 / 等待图 / 死锁环 的增量重建引擎。

纯函数风格：所有输入事件统一为 dict（见 normalize_event），
按 (timestamp, arrival) 排序后逐条喂给 Engine。引擎不凭空捏造
release：异常只会以 confidence != confirmed 的形式体现在边上。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

VALID_TYPES = {
    "thread_start", "thread_end",
    "acquire_request", "acquire_ok", "acquire_fail",
    "release",
    "cond_wait", "cond_wake", "cond_notify",
}
VALID_MODES = {"r", "w"}
CONFS = ("confirmed", "inferred", "suspect")


def normalize_event(raw: dict, arrival: int) -> dict:
    """把外部 JSON 事件规整成内部统一形态，缺失字段给默认值并标注置信。"""
    etype = raw.get("type")
    if etype not in VALID_TYPES:
        raise ValueError(f"unknown event type: {etype!r}")
    ev: dict[str, Any] = {
        "id": raw.get("id") or f"e{arrival}",
        "arrival": arrival,
        "generation": str(raw.get("generation", "g0")),
        "timestamp": int(raw["timestamp"]),
        "type": etype,
        "thread": raw.get("thread", "-"),
        "lock": raw.get("lock"),
        "cond": raw.get("cond"),
        "mode": raw.get("mode", "w"),
        "timeout": int(raw["timeout"]) if raw.get("timeout") is not None else None,
        "result": raw.get("result"),
        "notified_threads": list(raw.get("notified_threads") or []),
        "hold_stack": raw.get("hold_stack") or [],
        "wait_stack": raw.get("wait_stack") or [],
        "raw_order": arrival,
    }
    if ev["mode"] not in VALID_MODES:
        raise ValueError(f"invalid mode: {ev['mode']!r}")
    if etype in {"acquire_request", "acquire_ok", "acquire_fail", "release",
                 "cond_wait", "cond_wake"} and not ev["lock"]:
        raise ValueError(f"event {etype} requires a lock")
    if etype in {"cond_wait", "cond_notify"} and not ev["cond"]:
        raise ValueError(f"event {etype} requires a cond")
    return ev


@dataclass
class Pending:
    event: dict
    mode: str
    deadline: Optional[int]
    wait_stack: list


@dataclass
class ThreadState:
    name: str
    alive: bool = True
    started: bool = False
    start_event: Optional[dict] = None
    end_event: Optional[dict] = None
    # 每个锁的重入栈：[(event, mode, abandoned)]
    holds: dict[str, list] = field(default_factory=dict)
    # 每个锁的条件等待暂存（begin 时摘锁，wake 时归还）
    cond_holds: dict[str, tuple] = field(default_factory=dict)
    pending: Optional[Pending] = None
    cond_waiting: Optional[dict] = None
    anomalies: list = field(default_factory=list)


@dataclass
class Edge:
    edge_id: str
    edge_type: str          # wait_for | held_by | cond_wait
    lock: Optional[str]
    cond: Optional[str]
    owner_thread: Optional[str]
    waiter_thread: Optional[str]
    notifier_thread: Optional[str]
    mode: Optional[str]
    source_event_ids: list
    hold_stack: list
    wait_stack: list
    confidence: str
    note: str = ""

    def to_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class LockInfo:
    address: str
    lock_type: str = "unknown"     # unknown | reentrant | rwlock
    type_confidence: str = "inferred"
    type_event: Optional[dict] = None


def _worse(a: str, b: str) -> str:
    return CONFS[max(CONFS.index(a), CONFS.index(b))]


class Engine:
    """逐事件重建。events 必须已经按 (timestamp, arrival) 排好序。"""

    def __init__(self, events: list[dict]):
        self.events = events
        self.threads: dict[str, ThreadState] = {}
        self.locks: dict[str, LockInfo] = {}
        self.edges: dict[str, Edge] = {}
        self.cycle_log: list[dict] = []
        self.stat_recompute = 0
        self._edge_seq = 0

    # ---- 基础工具 -------------------------------------------------------
    def ts(self) -> ThreadState:
        # 仅用于事件处理时取“当前线程”
        raise NotImplementedError

    def _thread(self, name: str) -> ThreadState:
        st = self.threads.get(name)
        if st is None:
            st = ThreadState(name=name)
            self.threads[name] = st
            st.anomalies.append({
                "confidence": "suspect",
                "note": "线程缺少 thread_start 事件，推断其存在",
            })
        return st

    def _lock(self, addr: str) -> LockInfo:
        info = self.locks.get(addr)
        if info is None:
            info = LockInfo(addr)
            self.locks[addr] = info
        return info

    def _eid(self, kind: str) -> str:
        self._edge_seq += 1
        return f"{kind}-{self._edge_seq}"

    def _note(self, st: ThreadState, confidence: str, note: str) -> None:
        st.anomalies.append({"confidence": confidence, "note": note})

    # ---- 等待边 ---------------------------------------------------------
    def _blockers(self, lock: str, mode: str, requester: str) -> list[str]:
        info = self._lock(lock)
        holders: list[str] = []
        for name, st in self.threads.items():
            stack = st.holds.get(lock)
            if stack and not any(item[2] for item in stack):
                holders.append(name)
        if info.lock_type == "rwlock":
            if mode == "w":
                return [h for h in holders if h != requester] or holders
            readers = holders
            if not readers:
                return []
            # 读-读共享；只要还有别的读者/写者才阻塞
            others = [h for h in readers if h != requester]
            return others
        return [h for h in holders if h != requester]

    def _infer_type(self, lock: str, ev: dict) -> None:
        info = self._lock(lock)
        declared = (ev.get("_declared_types") or {}).get(lock)
        if declared:
            info.lock_type = declared
            info.type_confidence = "confirmed"
            info.type_event = ev
            return
        if info.lock_type != "unknown":
            return
        if ev.get("mode") == "r":
            info.lock_type = "rwlock"
            info.type_confidence = "inferred"
            info.type_event = ev

    def _refresh_pending_edge(self, st: ThreadState) -> None:
        if not st.pending:
            return
        p = st.pending
        lock, mode = p.event["lock"], p.mode
        key = f"w:{st.name}:{lock}"
        blockers = self._blockers(lock, mode, st.name)
        if not blockers:
            self.edges.pop(key, None)
            return
        old = self.edges.get(key)
        conf = "suspect" if st.name not in blockers and blockers else "confirmed"
        # 只要持有者里有非活动锁、或类型是推断的，降为 inferred
        info = self._lock(lock)
        if info.lock_type == "unknown":
            conf = _worse(conf, "inferred")
        for owner in blockers:
            ost = self.threads[owner]
            if any(item[2] for item in ost.holds.get(lock, [])):
                conf = _worse(conf, "inferred")
        note = f"{st.name} 以{mode}模式等待 {lock}"
        if p.deadline is not None:
            note += f"（超时点 {p.deadline}）"
        self.edges[key] = Edge(
            edge_id=key, edge_type="wait_for", lock=lock, cond=None,
            owner_thread=None, waiter_thread=st.name, notifier_thread=None,
            mode=mode, source_event_ids=[p.event["id"]],
            hold_stack=[], wait_stack=list(p.wait_stack),
            confidence=conf, note=note + f"；当前持有者 {sorted(blockers)}",
        )

    def _refresh_lock_edges(self, lock: str) -> None:
        for name, st in self.threads.items():
            if st.pending and st.pending.event["lock"] == lock:
                self._refresh_pending_edge(st)

    def _drop_pending_edge(self, st: ThreadState) -> None:
        if st.pending:
            self.edges.pop(f"w:{st.name}:{st.pending.event['lock']}", None)

    # ---- 持有边 ---------------------------------------------------------
    def _refresh_hold_edges(self, lock: str) -> None:
        info = self._lock(lock)
        for name, st in self.threads.items():
            stack = st.holds.get(lock)
            key = f"h:{name}:{lock}"
            if not stack:
                self.edges.pop(key, None)
                continue
            ev = stack[-1][0]
            abandoned = any(item[2] for item in stack)
            modes = sorted({item[1] for item in stack})
            mode = modes[0] if len(modes) == 1 else "w"
            if info.lock_type == "rwlock" and mode == "r":
                mode = "r"
            conf = "confirmed"
            note = f"{name} 持有 {lock}（重入 {len(stack)} 次）"
            if abandoned:
                conf = "suspect"
                note += "；线程已终止但缺少 release，标记为疑似遗留"
            if info.lock_type == "unknown":
                conf = _worse(conf, "inferred")
            self.edges[key] = Edge(
                edge_id=key, edge_type="held_by", lock=lock, cond=None,
                owner_thread=name, waiter_thread=None, notifier_thread=None,
                mode=mode,
                source_event_ids=[item[0]["id"] for item in stack],
                hold_stack=list(ev.get("hold_stack") or []),
                wait_stack=[], confidence=conf, note=note,
            )

    def _abandon_thread_locks(self, st: ThreadState) -> None:
        for lock, stack in list(st.holds.items()):
            if stack and not any(item[2] for item in stack):
                st.holds[lock] = [(item[0], item[1], True) for item in stack]
                self._refresh_hold_edges(lock)
                self._refresh_lock_edges(lock)

    # ---- 事件处理 -------------------------------------------------------
    def _on_acquire_request(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock = ev["lock"]
        self._infer_type(lock, ev)
        if st.pending is not None:
            self._note(st, "suspect",
                       f"{st.name} 在已有等待时再次请求 {lock}，旧请求保留")
        mode = ev["mode"]
        deadline = ev["timestamp"] + ev["timeout"] if ev.get("timeout") else None
        st.pending = Pending(ev, mode, deadline, list(ev.get("wait_stack") or []))
        self._refresh_pending_edge(st)

    def _grant(self, st: ThreadState, ev: dict, mode: str) -> bool:
        lock = ev["lock"]
        info = self._lock(lock)
        stack = st.holds.setdefault(lock, [])
        if stack:
            held_modes = {item[1] for item in stack}
            if info.lock_type == "rwlock":
                if "r" in held_modes and mode == "w":
                    pass  # 读->写升级：栈记录升级
                elif "w" in held_modes and mode == "r":
                    self._note(st, "inferred",
                               f"{st.name} 已写持有 {lock}，读请求视为重入")
                elif mode == "r":
                    self._note(st, "inferred",
                               f"{st.name} 重复读 {lock}，记为共享重入")
            stack.append((ev, mode, False))
            self._refresh_hold_edges(lock)
            return True
        blockers = self._blockers(lock, mode, st.name)
        if blockers:
            self._note(st, "suspect",
                       f"acquire_ok 时 {lock} 仍被 {sorted(blockers)} 持有，"
                       "授予事件存疑但仍记录持有")
        stack.append((ev, mode, False))
        self._refresh_hold_edges(lock)
        return True

    def _on_acquire_ok(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock = ev["lock"]
        self._infer_type(lock, ev)
        if st.pending is not None and st.pending.event["lock"] != lock:
            self._note(st, "suspect",
                       f"acquire_ok 的 {lock} 与等待中的 "
                       f"{st.pending.event['lock']} 不一致")
        mode = st.pending.mode if st.pending else ev["mode"]
        ws = st.pending.wait_stack if st.pending else list(ev.get("wait_stack") or [])
        if st.pending is None:
            self._note(st, "inferred",
                       f"{st.name} 获取 {lock} 缺少 acquire_request，补记等待栈")
        self._drop_pending_edge(st)
        st.pending = None
        if ev.get("hold_stack"):
            ev = {**ev}
        self._grant(st, ev, mode)
        # 保存等待栈到授予事件，供环证据回溯
        ev["_wait_stack"] = ws
        self._refresh_lock_edges(lock)

    def _on_acquire_fail(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock = ev["lock"]
        if st.pending is None:
            self._note(st, "inferred",
                       f"{st.name} 对 {lock} 的 acquire_fail 找不到等待请求")
        else:
            self._drop_pending_edge(st)
            st.pending = None

    def _on_release(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock = ev["lock"]
        if lock in st.cond_holds:
            self._note(st, "suspect",
                       f"{st.name} 在 cond_wait 期间释放 {lock}，忽略该释放")
            return
        stack = st.holds.get(lock)
        if not stack:
            self._note(st, "suspect",
                       f"{st.name} 释放未持有的 {lock}（缺少 acquire），丢弃该 release")
            return
        stack.pop()
        if not stack:
            del st.holds[lock]
        self._refresh_hold_edges(lock)
        self._refresh_lock_edges(lock)

    # ---- condition ------------------------------------------------------
    def _on_cond_wait(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock, cond = ev["lock"], ev["cond"]
        stack = st.holds.get(lock)
        if not stack:
            self._note(st, "suspect",
                       f"cond_wait 时 {st.name} 未持有 {lock}，等待仍记录但无锁可释放")
            st.cond_holds[lock] = (ev, None, 0)
        else:
            st.cond_holds[lock] = (ev, stack, len(stack))
            del st.holds[lock]
            self._refresh_hold_edges(lock)
            self._refresh_lock_edges(lock)
        st.cond_waiting = ev
        self.edges[f"c:{st.name}:{cond}"] = Edge(
            edge_id=f"c:{st.name}:{cond}", edge_type="cond_wait",
            lock=lock, cond=cond, owner_thread=None, waiter_thread=st.name,
            notifier_thread=None, mode=None, source_event_ids=[ev["id"]],
            hold_stack=list(ev.get("hold_stack") or []),
            wait_stack=list(ev.get("wait_stack") or []),
            confidence="confirmed",
            note=f"{st.name} 在 {cond} 上等待（暂时释放 {lock}）",
        )

    def _on_cond_wake(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        lock = ev["lock"]
        begin = st.cond_waiting
        if st.cond_waiting is None:
            self._note(st, "suspect",
                       f"{st.name} 的 cond_wake 找不到匹配的 cond_wait")
        else:
            self.edges.pop(f"c:{st.name}:{begin['cond']}", None)
            st.cond_waiting = None
        saved = st.cond_holds.pop(lock, None)
        if saved is not None and saved[1] is not None:
            st.holds[lock] = list(saved[1])
            self._refresh_hold_edges(lock)
        # wake 后通常还要重新竞争锁；若此刻拿不到则建立等待边
        mode = "w"
        st.pending = Pending(ev, mode, None,
                             list(ev.get("wait_stack") or
                                  (begin.get("wait_stack") if begin else [])))
        blockers = self._blockers(lock, mode, st.name)
        self._drop_pending_edge(st)
        if blockers:
            self._refresh_pending_edge(st)
        else:
            self._grant(st, ev, mode)
            st.pending = None
            self._refresh_lock_edges(lock)

    def _on_cond_notify(self, ev: dict) -> None:
        notifier = self._thread(ev["thread"])
        cond = ev["cond"]
        targets = ev.get("notified_threads") or [
            n for n, st in self.threads.items()
            if st.cond_waiting and st.cond_waiting["cond"] == cond
        ]
        for name in targets:
            wst = self.threads.get(name)
            if not wst or not wst.cond_waiting:
                continue
            edge = self.edges.get(f"c:{name}:{cond}")
            if edge is not None:
                edge.notifier_thread = notifier.name
                edge.source_event_ids.append(ev["id"])
                edge.note += f"；由 {notifier.name} 通知"

    # ---- 线程生命周期 ----------------------------------------------------
    def _on_thread_start(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        if st.started:
            self._note(st, "inferred", f"{st.name} 重复 thread_start")
        st.alive, st.started = True, True
        st.start_event = ev

    def _on_thread_end(self, ev: dict) -> None:
        st = self._thread(ev["thread"])
        st.alive = False
        st.end_event = ev
        if st.pending is not None:
            self._drop_pending_edge(st)
            st.pending = None
        if st.cond_waiting is not None:
            self.edges.pop(f"c:{st.name}:{st.cond_waiting['cond']}", None)
            st.cond_waiting = None
        self._abandon_thread_locks(st)

    # ---- 重放 -----------------------------------------------------------
    def run(self) -> "Engine":
        handlers = {
            "thread_start": self._on_thread_start,
            "thread_end": self._on_thread_end,
            "acquire_request": self._on_acquire_request,
            "acquire_ok": self._on_acquire_ok,
            "acquire_fail": self._on_acquire_fail,
            "release": self._on_release,
            "cond_wait": self._on_cond_wait,
            "cond_wake": self._on_cond_wake,
            "cond_notify": self._on_cond_notify,
        }
        for ev in self.events:
            self._sweep_timeouts(ev["timestamp"])
            handlers[ev["type"]](ev)
        return self

    def _sweep_timeouts(self, now: int) -> None:
        for name, st in self.threads.items():
            if st.pending and st.pending.deadline is not None \
                    and st.pending.deadline < now:
                self._drop_pending_edge(st)
                st.pending = None
                self._note(st, "inferred",
                           f"{name} 的等待超过超时点 {now}，自动撤销等待")

    # ---- 导出视图 -------------------------------------------------------
    def view(self) -> dict:
        edges = [e.to_dict() for e in sorted(self.edges.values(),
                                             key=lambda x: x.edge_id)]
        cycles = detect_cycles(edges)
        return {
            "threads": {
                name: {
                    "alive": st.alive, "started": st.started,
                    "pending": st.pending.event["id"] if st.pending else None,
                    "cond_waiting": st.cond_waiting["cond"]
                    if st.cond_waiting else None,
                    "holds": {lock: len(stack)
                              for lock, stack in st.holds.items()},
                    "anomalies": st.anomalies,
                }
                for name, st in sorted(self.threads.items())
            },
            "locks": {
                addr: {"lock_type": info.lock_type,
                       "type_confidence": info.type_confidence}
                for addr, info in sorted(self.locks.items())
            },
            "edges": edges,
            "cycles": cycles,
        }


def detect_cycles(edges: list[dict], limit: int = 32) -> list[dict]:
    """在 线程->线程 的 wait_for 边构成的等待图中枚举简单环。

    每条边都带来源事件 id、持有者的 hold_stack 与等待者的 wait_stack。
    """
    wait = [e for e in edges if e["edge_type"] == "wait_for"]
    # waiter -> {owner: edge}
    adj: dict[str, dict[str, dict]] = {}
    nodes = set()
    for e in wait:
        waiter = e["waiter_thread"]
        # 等待边 note 中记录了当前持有者；更可靠的是从 held_by 边反推，
        # 这里直接使用 note 不便，因此改由 held_by 计算。
        nodes.add(waiter)

    held = [e for e in edges if e["edge_type"] == "held_by"]
    holders: dict[str, list[dict]] = {}
    hold_edges: dict[tuple, dict] = {}
    for e in held:
        holders.setdefault(e["lock"], []).append(e)
        hold_edges[(e["owner_thread"], e["lock"])] = e
        nodes.add(e["owner_thread"])

    # 重新建立 waiter -> owners
    adj = {n: {} for n in nodes}
    for e in wait:
        lock = e["lock"]
        for he in holders.get(lock, []):
            owner = he["owner_thread"]
            if owner == e["waiter_thread"]:
                continue
            adj.setdefault(e["waiter_thread"], {})[owner] = (e, he)
            adj.setdefault(owner, {})

    found: list[dict] = []
    seen: set[tuple] = set()

    def norm(path: list[str]) -> tuple:
        i = path.index(min(path))
        rot = tuple(path[i:] + path[:i])
        return rot

    def dfs(start: str, node: str, chain: list[str],
            used_edges: list, visiting: set) -> None:
        if len(found) >= limit:
            return
        for nxt, pair in sorted(adj.get(node, {}).items()):
            if nxt == start and len(chain) >= 2:
                key = norm(chain)
                if key in seen:
                    continue
                seen.add(key)
                found.append(_cycle_payload(chain, used_edges + [pair]))
                if len(found) >= limit:
                    return
            elif nxt not in visiting and nxt >= start:
                visiting.add(nxt)
                dfs(start, nxt, chain + [nxt], used_edges + [pair], visiting)
                visiting.discard(nxt)

    for node in sorted(adj):
        dfs(node, node, [node], [], {node})
    found.sort(key=lambda c: (len(c["threads"]), c["signature"]))
    return found


def _cycle_payload(chain: list[str], pairs: list) -> dict:
    wait_edges = [p[0] for p in pairs]
    hold_edges = [p[1] for p in pairs]
    confidence = "confirmed"
    for e in wait_edges + hold_edges:
        confidence = _worse(confidence, e["confidence"])
    steps = []
    for (waiter, owner), (we, he) in zip(
            zip(chain, chain[1:] + chain[:1]), pairs):
        steps.append({
            "waiter": waiter,
            "owner": owner,
            "lock": we["lock"],
            "mode": we["mode"],
            "wait_event": we["source_event_ids"][0],
            "hold_event": he["source_event_ids"][-1],
            "wait_stack": we["wait_stack"],
            "hold_stack": he["hold_stack"],
            "wait_confidence": we["confidence"],
            "hold_confidence": he["confidence"],
        })
    locks = [s["lock"] for s in steps]
    return {
        "signature": " -> ".join(chain + [chain[0]]),
        "threads": list(chain),
        "locks": locks,
        "confidence": confidence,
        "steps": steps,
    }

    # ---- 检查点快照（供增量重放）----------------------------------------
    def _snapshot(self) -> dict:
        import copy
        return copy.deepcopy({
            "threads": self.threads,
            "locks": self.locks,
            "edges": self.edges,
            "cycle_log": self.cycle_log,
            "_edge_seq": self._edge_seq,
        })

    @classmethod
    def _from_snapshot(cls, snap: dict, events_seen: list[dict]) -> "Engine":
        import copy
        eng = cls(list(events_seen))
        eng.threads = copy.deepcopy(snap["threads"])
        eng.locks = copy.deepcopy(snap["locks"])
        eng.edges = copy.deepcopy(snap["edges"])
        eng.cycle_log = copy.deepcopy(snap.get("cycle_log", []))
        eng._edge_seq = snap.get("_edge_seq", 0)
        return eng
