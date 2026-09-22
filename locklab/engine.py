"""锁序解剖室 —— 锁事件重建引擎。

按采集代次重放事件流，维护每个线程的持有栈 / 等待栈，
推导等待图与锁序图，检测死锁环与近失环，并为缺事件、
乱序到达等情况维护带置信状态的候选图（绝不凭空补 release）。
"""
from __future__ import annotations

import copy

MUTEX = "mutex"
RECURSIVE = "recursive"
RWLOCK = "rwlock"
LOCK_TYPES = (MUTEX, RECURSIVE, RWLOCK)

CHECKPOINT_EVERY = 32

EVENT_TYPES = (
    "thread_start", "thread_end",
    "acquire_request", "acquire", "release", "timeout",
    "cond_wait", "cond_notify",
)


def initial_state():
    return {
        "threads": {},
        "lock_types": {},
        "order_edges": {},
        "anomalies": [],
        "tainted_locks": [],
        "tainted_threads": [],
        "last_ts": None,
    }


def _thread():
    return {"alive": True, "held": {}, "hold_order": [], "pending": None, "cond_wait": None}


def _mode(ev):
    return ev.get("mode") or "write"


def _taint_lock(state, lock):
    if lock is not None and lock not in state["tainted_locks"]:
        state["tainted_locks"].append(lock)


def _taint_thread(state, tid):
    if tid is not None and tid not in state["tainted_threads"]:
        state["tainted_threads"].append(tid)


def _anomaly(state, ev, kind, detail, lock=None):
    state["anomalies"].append({
        "event": ev.get("id"), "seq": ev.get("seq"), "kind": kind,
        "detail": detail, "lock": lock, "thread": ev.get("thread"),
    })


def lock_type_of(state, declared, lock):
    if lock in declared:
        return declared[lock]
    return state["lock_types"].get(lock, MUTEX)


def apply_event(state, ev, declared):
    etype = ev.get("type")
    ts = ev.get("ts")
    if ts is not None:
        if state["last_ts"] is not None and ts < state["last_ts"]:
            _anomaly(state, ev, "out_of_order", "事件时间戳早于同代次已到达事件，按采集顺序保留")
            _taint_thread(state, ev.get("thread"))
        state["last_ts"] = max(ts, state["last_ts"])
    tid = ev.get("thread")
    if etype == "thread_start":
        th = state["threads"].setdefault(tid, _thread())
        th["alive"] = True
        return
    if etype == "cond_notify":
        _apply_cond_notify(state, ev)
        return
    if tid is None:
        _anomaly(state, ev, "missing_thread", "事件缺少线程标识")
        return
    th = state["threads"].setdefault(tid, _thread())

    if etype == "acquire_request":
        if th["pending"] is not None:
            _anomaly(state, ev, "request_replaced", "未完成的 acquire 请求被新请求覆盖",
                     lock=th["pending"]["lock"])
        th["pending"] = {"lock": ev.get("lock"), "mode": _mode(ev), "event": ev.get("id"),
                         "timeout_ms": ev.get("timeout_ms"), "saved_count": 1, "reacquire": False}
    elif etype == "acquire":
        _apply_acquire(state, ev, th, declared)
    elif etype == "release":
        _apply_release(state, ev, th)
    elif etype == "timeout":
        pend = th["pending"]
        if pend and pend["lock"] == ev.get("lock"):
            th["pending"] = None
        else:
            _anomaly(state, ev, "timeout_without_request", "超时事件没有匹配的等待请求",
                     lock=ev.get("lock"))
    elif etype == "cond_wait":
        _apply_cond_wait(state, ev, th)
    elif etype == "thread_end":
        if th["held"]:
            _anomaly(state, ev, "unclosed_hold",
                     "线程结束时仍持有锁: " + ", ".join(sorted(th["held"])))
            _taint_thread(state, tid)
            for lk in th["held"]:
                _taint_lock(state, lk)
        th["held"] = {}
        th["hold_order"] = []
        th["pending"] = None
        th["cond_wait"] = None
        th["alive"] = False


def _apply_acquire(state, ev, th, declared):
    tid = ev.get("thread")
    lock = ev.get("lock")
    mode = _mode(ev)
    saved_count = 1
    pend = th["pending"]
    if pend and pend["lock"] == lock:
        saved_count = pend.get("saved_count", 1)
        if pend.get("reacquire"):
            mode = pend["mode"]
        th["pending"] = None
    elif pend:
        _anomaly(state, ev, "acquire_mismatch", "acquire 成功与挂起请求的锁不一致", lock=lock)
        th["pending"] = None
    else:
        _anomaly(state, ev, "acquire_without_request", "缺少对应的 acquire 请求事件", lock=lock)
    ltype = lock_type_of(state, declared, lock)
    hold = th["held"].get(lock)
    if hold:
        if declared.get(lock) == MUTEX:
            _anomaly(state, ev, "reentrant_on_mutex", "已声明为互斥锁却发生重入获取", lock=lock)
        elif lock not in declared and ltype != RWLOCK:
            state["lock_types"][lock] = RECURSIVE
        hold["count"] += 1
        hold["events"].append(ev.get("id"))
        return
    for otid, oth in state["threads"].items():
        if otid == tid:
            continue
        oh = oth["held"].get(lock)
        if not oh:
            continue
        if ltype == RWLOCK and mode == "read" and oh["mode"] == "read":
            continue
        _anomaly(state, ev, "missing_release",
                 "锁 %s 在 %s 未释放的情况下被 %s 获取（疑似缺失 release，不补全）"
                 % (lock, otid, tid), lock=lock)
        _taint_lock(state, lock)
        break
    th["held"][lock] = {"mode": mode, "count": saved_count, "events": [ev.get("id")]}
    th["hold_order"].append(lock)
    for other in th["hold_order"]:
        if other == lock:
            continue
        key = other + "->" + lock
        edge = state["order_edges"].setdefault(
            key, {"from": other, "to": lock, "locks": [other, lock], "events": []})
        if ev.get("id") not in edge["events"]:
            edge["events"].append(ev.get("id"))


def _apply_release(state, ev, th):
    lock = ev.get("lock")
    hold = th["held"].get(lock)
    if not hold:
        _anomaly(state, ev, "release_without_hold", "释放一个未持有的锁", lock=lock)
        return
    hold["count"] -= 1
    if hold["count"] <= 0:
        del th["held"][lock]
        th["hold_order"].remove(lock)


def _apply_cond_wait(state, ev, th):
    lock = ev.get("lock")
    hold = th["held"].get(lock)
    if not hold:
        _anomaly(state, ev, "cond_wait_without_hold", "condition wait 时未持有关联锁", lock=lock)
        return
    th["cond_wait"] = {"cond": ev.get("cond"), "lock": lock, "mode": hold["mode"],
                       "count": hold["count"], "event": ev.get("id")}
    del th["held"][lock]
    th["hold_order"].remove(lock)


def _apply_cond_notify(state, ev):
    cond = ev.get("cond")
    wake_all = bool(ev.get("all"))
    waiters = [(t, tt["cond_wait"]) for t, tt in state["threads"].items()
               if tt["cond_wait"] and tt["cond_wait"]["cond"] == cond]
    waiters.sort(key=lambda item: (item[1]["event"] or 0))
    if not wake_all:
        waiters = waiters[:1]
    for t, cw in waiters:
        tt = state["threads"][t]
        tt["pending"] = {"lock": cw["lock"], "mode": cw["mode"], "event": ev.get("id"),
                         "timeout_ms": None, "saved_count": cw["count"], "reacquire": True}
        tt["cond_wait"] = None


def compute_wait_edges(state, declared):
    edges = []
    for tid, th in state["threads"].items():
        req = th["pending"]
        if not req:
            continue
        lock = req["lock"]
        ltype = lock_type_of(state, declared, lock)
        for otid, oth in state["threads"].items():
            if otid == tid:
                continue
            oh = oth["held"].get(lock)
            if not oh:
                continue
            if ltype == RWLOCK and req["mode"] == "read" and oh["mode"] == "read":
                continue
            low = (lock in state["tainted_locks"] or tid in state["tainted_threads"]
                   or otid in state["tainted_threads"])
            edges.append({"from": tid, "to": otid, "lock": lock, "mode": req["mode"],
                          "request_event": req["event"], "hold_events": list(oh["events"]),
                          "confidence": "low" if low else "high"})
    return edges


def find_cycles(nodes, adj):
    index = {n: i for i, n in enumerate(nodes)}
    cycles = []
    for start in nodes:
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for nxt in adj.get(node, ()):
                if nxt == start and len(path) > 1:
                    cycles.append(list(path))
                elif index[nxt] > index[start] and nxt not in path:
                    stack.append((nxt, path + [nxt]))
    return cycles


def build_graph(state, declared):
    threads = []
    for tid in sorted(state["threads"]):
        th = state["threads"][tid]
        threads.append({
            "id": tid, "alive": th["alive"],
            "held": [dict({"lock": lk}, **th["held"][lk]) for lk in th["hold_order"]],
            "pending": th["pending"], "cond_wait": th["cond_wait"],
        })
    wait_edges = compute_wait_edges(state, declared)
    adj = {}
    for e in wait_edges:
        adj.setdefault(e["from"], []).append(e["to"])
    nodes = sorted({e["from"] for e in wait_edges} | {e["to"] for e in wait_edges})
    edge_map = {(e["from"], e["to"]): e for e in wait_edges}
    cycles = []
    for cyc in find_cycles(nodes, adj):
        evidence = {}
        locks = []
        conf = "high"
        for i, tid in enumerate(cyc):
            e = edge_map[(tid, cyc[(i + 1) % len(cyc)])]
            th = state["threads"][tid]
            evidence[tid] = {
                "held": [dict({"lock": lk}, **th["held"][lk]) for lk in th["hold_order"]],
                "waiting": {"lock": e["lock"], "mode": e["mode"],
                            "request_event": e["request_event"]},
                "holder": cyc[(i + 1) % len(cyc)],
            }
            locks.append(e["lock"])
            if e["confidence"] == "low":
                conf = "low"
        cycles.append({"kind": "deadlock", "threads": cyc, "locks": locks,
                       "evidence": evidence, "confidence": conf})
    order_edges = []
    for e in state["order_edges"].values():
        low = any(lk in state["tainted_locks"] for lk in e["locks"])
        order_edges.append(dict(e, confidence="low" if low else "high"))
    oadj = {}
    for e in order_edges:
        oadj.setdefault(e["from"], []).append(e["to"])
    near = []
    for cyc in find_cycles(sorted(oadj), oadj):
        events = []
        for i, lk in enumerate(cyc):
            e = state["order_edges"][lk + "->" + cyc[(i + 1) % len(cyc)]]
            for eid in e["events"]:
                if eid not in events:
                    events.append(eid)
        near.append({"kind": "near_miss", "locks": cyc, "events": events})
    return {
        "threads": threads,
        "wait_edges": wait_edges,
        "order_edges": order_edges,
        "cycles": cycles,
        "near_misses": near,
        "anomalies": list(state["anomalies"]),
        "status": "candidate" if state["anomalies"] else "confirmed",
    }


class Runner:
    """单代次重放器，带检查点以支持流式追加时只重算受影响时间段。"""

    def __init__(self, declared=None):
        self.declared = dict(declared or {})
        self.events = []
        self.state = initial_state()
        self.checkpoints = {0: initial_state()}
        self.stats = {"recomputed_from_seq": None, "replayed": 0}

    def rebuild(self, events):
        declared = self.declared
        self.__init__(declared)
        self.events = list(events)
        self._replay_from(0)

    def append(self, new_events):
        start = len(self.events)
        self.events.extend(new_events)
        self.checkpoints = {i: s for i, s in self.checkpoints.items() if i <= start}
        self._replay_from(start)

    def _replay_from(self, idx):
        cp = 0
        for i in self.checkpoints:
            if i <= idx and i > cp:
                cp = i
        self.state = copy.deepcopy(self.checkpoints[cp])
        n = len(self.events)
        if n:
            self.stats["recomputed_from_seq"] = self.events[min(cp, n - 1)]["seq"]
        i = cp
        while i < n:
            apply_event(self.state, self.events[i], self.declared)
            i += 1
            if i % CHECKPOINT_EVERY == 0:
                self.checkpoints[i] = copy.deepcopy(self.state)
        self.stats["replayed"] = n - cp

    def graph_at(self, count=None):
        if count is None or count >= len(self.events):
            return build_graph(self.state, self.declared), dict(self.stats)
        cp = 0
        for i in self.checkpoints:
            if i <= count and i > cp:
                cp = i
        st = copy.deepcopy(self.checkpoints[cp])
        for i in range(cp, count):
            apply_event(st, self.events[i], self.declared)
        stats = {"recomputed_from_seq": self.events[cp]["seq"] if self.events else None,
                 "replayed": count - cp}
        return build_graph(st, self.declared), stats
