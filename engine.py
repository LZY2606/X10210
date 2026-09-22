"""锁序解剖室核心引擎：事件重放、持有图/等待图、环检测、置信状态、增量重算。

设计要点：
- 事件按采集代次 gen 分组，组内按 (ts, arrival) 排序重放，保留原始到达序号。
- 缺事件 / 乱序到达时不凭空补 release，而是给图打 candidate 置信标记并记录异常。
- 等待边、持有边都能回溯到产生它的事件 id。
- 快照缓存支持流式追加后只重算受影响时间段。
"""
import copy

SNAPSHOT_EVERY = 8


def new_state():
    return {
        "threads": {},      # thread -> {"holds": [...], "pending": {...}|None, "cond_wait": {...}|None}
        "anomalies": [],    # {"type", "event_id", "ts", "detail"}
        "out_of_order": [], # 乱序到达的时间区间 [(lo, hi)]
    }


def _thread(state, name):
    threads = state["threads"]
    if name not in threads:
        threads[name] = {"holds": [], "pending": None, "cond_wait": None, "alive": True}
    return threads[name]


def _anomaly(state, ev, atype, detail):
    state["anomalies"].append({
        "type": atype, "event_id": ev.get("id"), "ts": ev.get("ts"),
        "thread": ev.get("thread"), "lock": ev.get("lock"), "detail": detail,
    })


def _conflicts(mode_a, mode_b):
    """读写锁共享持有：read/read 不冲突，其余组合冲突。"""
    return not (mode_a == "read" and mode_b == "read")


def apply_event(state, ev, lock_types):
    kind = ev["kind"]
    th = _thread(state, ev["thread"]) if ev.get("thread") else None

    if kind == "thread_start":
        th["alive"] = True
        return

    if kind == "thread_end":
        # 线程终止撤销等待，但不凭空补 release：遗留持有标记为 unclosed。
        if th["pending"]:
            th["pending"] = None
        if th["cond_wait"]:
            th["cond_wait"] = None
        for hold in th["holds"]:
            hold["confidence"] = "unclosed"
            _anomaly(state, ev, "unclosed_hold",
                     "线程结束时仍持有锁 %s，未补 release" % hold["lock"])
        th["alive"] = False
        return

    if kind == "req":
        if th["pending"]:
            _anomaly(state, ev, "req_while_pending", "已有未完成的 acquire 请求，被覆盖")
        th["pending"] = {
            "lock": ev["lock"], "mode": ev.get("mode") or "write",
            "req_id": ev.get("id"), "ts": ev["ts"], "timeout": ev.get("timeout"),
        }
        return

    if kind == "timeout":
        if th["pending"]:
            th["pending"] = None
        else:
            _anomaly(state, ev, "timeout_without_req", "超时事件没有对应请求")
        return

    if kind == "acq":
        ltype = lock_types.get(ev["lock"], "mutex")
        confidence = "confirmed"
        mode = ev.get("mode") or "write"
        pend = th["pending"]
        if pend and pend["lock"] == ev["lock"]:
            mode = pend["mode"]
            th["pending"] = None
        elif th["cond_wait"] and th["cond_wait"]["lock"] == ev["lock"]:
            # condition wait 后重新竞争成功，恢复原持有计数
            suspended = th["cond_wait"].get("suspended")
            count = suspended["count"] if suspended else 1
            th["cond_wait"] = None
            th["holds"].append({"lock": ev["lock"], "mode": mode, "count": count,
                                "acq_id": ev.get("id"), "confidence": "confirmed"})
            return
        else:
            confidence = "inferred"  # 缺 req 事件的成功获取
            _anomaly(state, ev, "acq_without_req", "acquire 成功缺少对应请求事件")

        for hold in th["holds"]:
            if hold["lock"] == ev["lock"]:
                if ltype == "recursive":
                    hold["count"] += 1  # 可重入
                else:
                    hold["count"] += 1
                    hold["confidence"] = "conflict"
                    _anomaly(state, ev, "mutex_relock", "非递归锁被同线程重复获取")
                return

        # 与他线程持有冲突 -> 候选持有（可能是缺 release 事件）
        for other_name, other in state["threads"].items():
            if other_name == ev["thread"]:
                continue
            for hold in other["holds"]:
                if hold["lock"] == ev["lock"] and _conflicts(hold["mode"], mode):
                    confidence = "conflict"
                    _anomaly(state, ev, "acq_conflicts_hold",
                             "获取与线程 %s 的持有冲突，疑似缺 release" % other_name)
        th["holds"].append({"lock": ev["lock"], "mode": mode, "count": 1,
                            "acq_id": ev.get("id"), "confidence": confidence})
        return

    if kind == "rel":
        for i, hold in enumerate(th["holds"]):
            if hold["lock"] == ev["lock"]:
                if hold["count"] > 1:
                    hold["count"] -= 1
                else:
                    th["holds"].pop(i)
                return
        # 不凭空补 release，也不凭空消掉别人的持有：只记录异常
        _anomaly(state, ev, "release_without_hold", "释放了一个未持有的锁")
        return

    if kind == "wait":
        suspended = None
        for i, hold in enumerate(th["holds"]):
            if hold["lock"] == ev["lock"]:
                suspended = th["holds"].pop(i)  # condition wait 暂时释放
                break
        if suspended is None:
            _anomaly(state, ev, "wait_without_hold", "condition wait 时未持有对应锁")
        th["cond_wait"] = {"cond": ev.get("cond"), "lock": ev["lock"],
                           "wait_id": ev.get("id"), "suspended": suspended,
                           "notified": False}
        return

    if kind == "notify":
        targets = []
        for name, t in state["threads"].items():
            cw = t["cond_wait"]
            if cw and cw["cond"] == ev.get("cond") and not cw["notified"]:
                targets.append(name)
        if not targets:
            return  # 空 notify 在运行时属正常
        if ev.get("mode") != "all":
            targets = targets[:1]
        for name in targets:
            t = state["threads"][name]
            cw = t["cond_wait"]
            cw["notified"] = True
            # 被唤醒后重新竞争原锁：等待边证据指向 notify 事件
            suspended = cw.get("suspended") or {}
            t["pending"] = {"lock": cw["lock"], "mode": suspended.get("mode", "write"),
                            "req_id": ev.get("id"), "ts": ev["ts"], "timeout": None}
        return

    raise ValueError("unknown event kind: %s" % kind)


def holders_of(state, lock):
    """锁的当前持有者 -> [(thread, hold)]"""
    out = []
    for name, t in state["threads"].items():
        for hold in t["holds"]:
            if hold["lock"] == lock:
                out.append((name, hold))
    return out


def build_graph(state, at_ts=None):
    """由状态构造持有图与等待图。每条边都带产生它的事件 id。"""
    held_edges = []
    wait_edges = []
    for name, t in state["threads"].items():
        for hold in t["holds"]:
            held_edges.append({
                "thread": name, "lock": hold["lock"], "mode": hold["mode"],
                "count": hold["count"], "event_id": hold["acq_id"],
                "confidence": hold["confidence"],
            })
        pend = t["pending"]
        if pend:
            if pend.get("timeout") is not None and at_ts is not None \
                    and pend["ts"] + pend["timeout"] <= at_ts:
                pass  # 超时请求在 at_ts 已自动撤销
            else:
                _wait_edges_for(state, name, pend["lock"], pend["mode"],
                                pend["req_id"], "req", wait_edges)
        cw = t["cond_wait"]
        if cw and not cw["notified"]:
            # 尚未被唤醒的 condition 等待：重新竞争时需要原锁
            suspended = cw.get("suspended") or {}
            _wait_edges_for(state, name, cw["lock"], suspended.get("mode", "write"),
                            cw["wait_id"], "cond", wait_edges)
    return {"held": held_edges, "wait": wait_edges}


def _wait_edges_for(state, waiter, lock, mode, event_id, kind, out):
    targets = holders_of(state, lock)
    if not targets:
        out.append({"from": waiter, "to": None, "lock": lock, "kind": kind,
                    "event_id": event_id, "detail": "等待 notify 或锁被持有"})
        return
    for holder, hold in targets:
        if holder == waiter and kind == "cond":
            continue
        if holder != waiter and not _conflicts(hold["mode"], mode):
            continue  # 共享读不冲突
        out.append({"from": waiter, "to": holder, "lock": lock, "kind": kind,
                    "event_id": event_id,
                    "detail": "等待 %s 持有的 %s" % (holder, lock)})


def detect_cycles(graph):
    """在等待图上找所有基本环（含自环）。返回线程/锁/持有栈/等待栈证据。"""
    edges = [e for e in graph["wait"] if e.get("to")]
    adj = {}
    for e in edges:
        adj.setdefault(e["from"], []).append(e)

    cycles = []
    seen = set()

    def canonical(cyc_edges):
        nodes = [e["from"] for e in cyc_edges]
        rots = [tuple(nodes[i:] + nodes[:i]) for i in range(len(nodes))]
        return min(rots)

    def dfs(start, node, path, visited):
        for e in adj.get(node, []):
            nxt = e["to"]
            if nxt == start:
                cyc = path + [e]
                key = canonical(cyc)
                if key not in seen:
                    seen.add(key)
                    cycles.append(cyc)
            elif nxt not in visited:
                dfs(start, nxt, path + [e], visited | {nxt})

    for node in list(adj):
        dfs(node, node, [], {node})

    held_by_thread = {}
    for h in graph["held"]:
        held_by_thread.setdefault(h["thread"], []).append(h)

    results = []
    for cyc in cycles:
        threads = []
        for e in cyc:
            threads.append(e["from"])
        results.append({
            "threads": threads,
            "locks": [e["lock"] for e in cyc],
            "edges": cyc,
            "hold_stacks": {t: held_by_thread.get(t, []) for t in threads},
            "wait_stacks": {t: [e for e in cyc if e["from"] == t] for t in threads},
            "signature": sorted("%s->%s:%s" % (e["from"], e["to"], e["lock"]) for e in cyc),
        })
    return results


def graph_confidence(state, at_ts=None):
    if state["anomalies"]:
        return "candidate"
    if at_ts is not None:
        for lo, hi in state["out_of_order"]:
            if lo <= at_ts:
                return "candidate"
    elif state["out_of_order"]:
        return "candidate"
    return "confirmed"


class BranchEngine:
    """一个分支的重放引擎：维护事件序列、快照缓存与增量重算。"""

    def __init__(self, lock_types=None):
        self.events = []            # 已按 (ts, arrival) 排序
        self.lock_types = dict(lock_types or {})
        self.snapshots = {}         # index -> 重放前 index 个事件后的状态
        self.state = new_state()
        self.stats = {"last_replay_from": 0, "last_replay_count": 0}

    def _sorted(self, events):
        return sorted(events, key=lambda e: (e["ts"], e.get("arrival") or 0))

    def _detect_out_of_order(self):
        ooo = []
        arrivals = [e.get("arrival") or 0 for e in self.events]
        for i in range(len(self.events)):
            for j in range(i + 1, len(self.events)):
                if arrivals[i] and arrivals[j] and arrivals[i] > arrivals[j]:
                    ooo.append((self.events[i]["ts"], self.events[j]["ts"]))
        if ooo:
            lo = min(a for a, _ in ooo)
            hi = max(b for _, b in ooo)
            self.state["out_of_order"] = [(lo, hi)]

    def _replay_from(self, start):
        if start > 0 and start in self.snapshots:
            self.state = copy.deepcopy(self.snapshots[start])
        elif start > 0:
            earlier = [i for i in self.snapshots if i <= start]
            if earlier:
                k = max(earlier)
                self.state = copy.deepcopy(self.snapshots[k])
                start = k
            else:
                self.state = new_state()
                start = 0
        else:
            self.state = new_state()
        self.stats = {"last_replay_from": start,
                      "last_replay_count": len(self.events) - start}
        for i in range(start, len(self.events)):
            if i % SNAPSHOT_EVERY == 0:
                self.snapshots[i] = copy.deepcopy(self.state)
            apply_event(self.state, self.events[i], self.lock_types)
        self._detect_out_of_order()

    def set_events(self, events):
        self.events = self._sorted(events)
        self.snapshots = {}
        self._replay_from(0)

    def append(self, new_events):
        """流式追加：只重算受影响时间段（最早新事件位置之后）。"""
        old = self.events
        merged = self._sorted(old + list(new_events))
        affected = len(merged)
        old_ids = [id(e) for e in old]
        for i, e in enumerate(merged):
            if i >= len(old) or id(e) != old_ids[i]:
                affected = i
                break
        self.events = merged
        self.snapshots = {i: s for i, s in self.snapshots.items() if i < affected}
        self._replay_from(affected)
        return dict(self.stats, total=len(self.events))

    def graph_at(self, at_ts=None):
        if at_ts is None:
            return build_graph(self.state), graph_confidence(self.state)
        # 重放到指定时点：找最近快照再前进
        idx = 0
        for i, e in enumerate(self.events):
            if e["ts"] <= at_ts:
                idx = i + 1
            else:
                break
        base = 0
        state = new_state()
        earlier = [i for i in self.snapshots if i <= idx]
        if earlier:
            base = max(earlier)
            state = copy.deepcopy(self.snapshots[base])
        for i in range(base, idx):
            apply_event(state, self.events[i], self.lock_types)
        state["out_of_order"] = list(self.state["out_of_order"])
        return build_graph(state, at_ts), graph_confidence(state, at_ts)
