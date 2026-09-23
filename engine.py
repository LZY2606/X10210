"""锁序解剖室重建引擎。

按采集代次内的原始到达顺序回放事件，维护：
- 每个线程的持有栈（可重入计数、产生持有的 acquire 事件、泄漏/置信标记）
- 每个线程的等待栈（请求事件、模式、超时、等待种类）
- condition 变量的等待队列（wait 暂时释放锁，notify 后重新竞争）
- 带置信状态的候选图：缺事件/乱序时记录异常，绝不凭空补 release

增量重算：按 (db, branch, generation) 缓存快照，流式追加只重放新增区间。
"""
import copy
import hashlib

SNAPSHOT_INTERVAL = 25

EVENT_KINDS = {
    "thread_start", "thread_end",
    "acquire_request", "acquire_success", "release",
    "cond_wait", "cond_notify", "cond_notify_all",
    "acquire_timeout",
}


def new_state():
    return {
        "threads": {},   # name -> {alive, held:{lock:hold}, waiting, cond}
        "locks": {},     # addr -> {type, holders:{thread:count}}
        "conds": {},     # cond -> [{thread, lock, wait_event}]
        "anomalies": [],
        "processed": 0,
        "max_ts": None,
    }


def ensure_thread(state, thread):
    return state["threads"].setdefault(thread, {
        "alive": True, "held": {}, "waiting": None, "cond": None,
    })


def ensure_lock(state, lock):
    return state["locks"].setdefault(lock, {"type": None, "holders": {}})


def add_anomaly(state, kind, event_id, detail):
    state["anomalies"].append({"type": kind, "event": event_id, "detail": detail})


def resolve_lock_type(state, lock, declared, extra):
    """锁类型优先级：分支声明 > 已推断 > 事件自带 > 默认 mutex。"""
    lt = declared.get(lock)
    if not lt:
        lt = state["locks"].get(lock, {}).get("type")
    if not lt:
        lt = (extra or {}).get("lock_type")
    if not lt and (extra or {}).get("mode") in ("read", "write"):
        lt = "rwlock"
    if not lt:
        lt = "mutex"
    ensure_lock(state, lock)["type"] = lt
    return lt


def process_event(state, ev, declared):
    kind = ev.get("kind")
    thread = ev.get("thread")
    lock = ev.get("lock")
    extra = ev.get("extra") or {}
    ts = ev.get("ts")
    eid = ev.get("id")

    if ts is not None:
        # 超时请求自动撤销等待（撤销等待不等于补 release）
        for tname, th in state["threads"].items():
            w = th.get("waiting")
            if w and w.get("timeout") is not None and w.get("since_ts") is not None:
                if ts > w["since_ts"] + w["timeout"]:
                    add_anomaly(state, "timeout_expired", eid,
                                {"thread": tname, "lock": w["lock"]})
                    th["waiting"] = None
        if state["max_ts"] is not None and ts < state["max_ts"]:
            add_anomaly(state, "out_of_order", eid,
                        {"ts": ts, "max_ts": state["max_ts"]})
        state["max_ts"] = ts if state["max_ts"] is None else max(state["max_ts"], ts)

    if kind == "thread_start":
        ensure_thread(state, thread)["alive"] = True

    elif kind == "thread_end":
        th = ensure_thread(state, thread)
        th["alive"] = False
        if th.get("waiting"):
            add_anomaly(state, "wait_cancelled_by_exit", eid,
                        {"thread": thread, "lock": th["waiting"]["lock"]})
            th["waiting"] = None
        if th.get("cond") is not None:
            waiters = state["conds"].get(th["cond"], [])
            state["conds"][th["cond"]] = [w for w in waiters if w["thread"] != thread]
            th["cond"] = None
        # 线程终止不补 release：持有保留并标记泄漏
        for l, h in th["held"].items():
            h["leaked"] = True
            add_anomaly(state, "missing_release", eid, {"thread": thread, "lock": l})

    elif kind == "acquire_request":
        th = ensure_thread(state, thread)
        lt = resolve_lock_type(state, lock, declared, extra)
        mode = extra.get("mode", "write")
        held = th["held"].get(lock)
        if held and lt == "reentrant":
            pass  # 可重入：同线程再次获取不阻塞
        elif held and lt == "rwlock" and held["mode"] == "read" and mode == "write":
            th["waiting"] = {"lock": lock, "mode": "write", "kind": "upgrade",
                             "request_event": eid, "since_ts": ts,
                             "timeout": extra.get("timeout")}
        else:
            th["waiting"] = {"lock": lock, "mode": mode, "kind": "lock",
                             "request_event": eid, "since_ts": ts,
                             "timeout": extra.get("timeout")}

    elif kind == "acquire_success":
        th = ensure_thread(state, thread)
        lt = resolve_lock_type(state, lock, declared, extra)
        w = th.get("waiting")
        confidence = "certain"
        mode = extra.get("mode", "write")
        upgrade = False
        if w and w["lock"] == lock:
            mode = w.get("mode", mode)
            upgrade = w.get("kind") == "upgrade"
            th["waiting"] = None
        else:
            confidence = "suspect"
            add_anomaly(state, "missing_request", eid,
                        {"thread": thread, "lock": lock})
        th["cond"] = None
        h = th["held"].setdefault(lock, {
            "count": 0, "mode": mode, "acquire_events": [],
            "leaked": False, "confidence": confidence,
        })
        if h["count"] == 0:
            h["mode"] = mode
            h["confidence"] = confidence
        h["count"] += 1
        h["acquire_events"].append(eid)
        if upgrade:
            h["mode"] = "write"
        lk = ensure_lock(state, lock)
        lk["holders"][thread] = h["count"]
        if h["count"] > 1 and lk["type"] == "mutex":
            lk["type"] = "reentrant"
            add_anomaly(state, "reentrant_inferred", eid,
                        {"thread": thread, "lock": lock})

    elif kind == "release":
        th = ensure_thread(state, thread)
        h = th["held"].get(lock)
        if not h:
            add_anomaly(state, "release_without_hold", eid,
                        {"thread": thread, "lock": lock})
        else:
            h["count"] -= 1
            if h["count"] <= 0:
                del th["held"][lock]
                ensure_lock(state, lock)["holders"].pop(thread, None)

    elif kind == "cond_wait":
        th = ensure_thread(state, thread)
        cond = extra.get("cond", lock)
        h = th["held"].get(lock)
        if not h:
            add_anomaly(state, "cond_wait_without_hold", eid,
                        {"thread": thread, "lock": lock})
        else:
            # condition wait 暂时完全释放关联锁
            del th["held"][lock]
            ensure_lock(state, lock)["holders"].pop(thread, None)
        th["cond"] = cond
        state["conds"].setdefault(cond, []).append(
            {"thread": thread, "lock": lock, "wait_event": eid})

    elif kind in ("cond_notify", "cond_notify_all"):
        cond = extra.get("cond", lock)
        waiters = state["conds"].get(cond, [])
        if not waiters:
            add_anomaly(state, "notify_without_waiter", eid, {"cond": cond})
        n = len(waiters) if kind == "cond_notify_all" else min(1, len(waiters))
        for w0 in waiters[:n]:
            th2 = state["threads"].get(w0["thread"])
            if th2 is None:
                continue
            th2["cond"] = None
            # 被唤醒后重新竞争原锁
            th2["waiting"] = {"lock": w0["lock"], "mode": "write",
                              "kind": "cond_reacquire", "request_event": eid,
                              "since_ts": ts, "timeout": None}
        state["conds"][cond] = waiters[n:]

    elif kind == "acquire_timeout":
        th = ensure_thread(state, thread)
        w = th.get("waiting")
        if w and (lock is None or w["lock"] == lock):
            add_anomaly(state, "timeout_cancel", eid,
                        {"thread": thread, "lock": w["lock"]})
            th["waiting"] = None

    state["processed"] += 1


def replay(events, declared, state=None):
    st = state if state is not None else new_state()
    for ev in events:
        process_event(st, ev, declared)
    return st


def build_graphs(state):
    """由状态构建持有图与等待图；每条边都携带产生它的事件证据。"""
    held = []
    for t, th in state["threads"].items():
        for l, h in th["held"].items():
            held.append({
                "thread": t, "lock": l, "count": h["count"], "mode": h["mode"],
                "acquire_events": list(h["acquire_events"]),
                "leaked": h["leaked"], "confidence": h["confidence"],
                "lock_type": state["locks"].get(l, {}).get("type"),
            })
    edges = []
    for t, th in state["threads"].items():
        w = th.get("waiting")
        if not w:
            continue
        l = w["lock"]
        lk = state["locks"].get(l, {"holders": {}})
        for holder in lk.get("holders", {}):
            hh = state["threads"].get(holder, {}).get("held", {}).get(l)
            if hh is None:
                continue
            if holder == t and w["kind"] == "upgrade":
                continue  # 唯一持读者升级写锁应被准许，不算自锁
            if w["mode"] == "read" and hh["mode"] != "write":
                continue  # 读-读共享，无等待边
            conf = "certain"
            if hh["confidence"] != "certain" or not w.get("request_event"):
                conf = "suspect"
            edges.append({
                "from": t, "to": holder, "lock": l,
                "wait_kind": w["kind"], "mode": w["mode"],
                "request_event": w.get("request_event"),
                "holder_events": list(hh["acquire_events"]),
                "confidence": conf,
            })
    cond_blocked = [{"thread": t, "cond": th["cond"]}
                    for t, th in state["threads"].items() if th.get("cond")]
    return {"held": held, "edges": edges, "cond_blocked": cond_blocked}


def find_cycles(edges):
    """枚举等待图中的所有基本环（支持多环并存与自环）。"""
    adj = {}
    for e in edges:
        adj.setdefault(e["from"], []).append(e)
    cycles = []
    seen = set()

    def canonical(cyc):
        return min(tuple(cyc[i:] + cyc[:i]) for i in range(len(cyc)))

    def dfs(start, node, path, visited):
        for e in adj.get(node, []):
            nxt = e["to"]
            if nxt == start:
                key = canonical(path)
                if key not in seen:
                    seen.add(key)
                    cycles.append(list(path))
            elif nxt not in visited:
                dfs(start, nxt, path + [nxt], visited | {nxt})

    for n in list(adj):
        dfs(n, n, [n], {n})
    return cycles


def cycle_reports(state, graphs):
    """为每个环给出具体线程、锁、持有栈与等待栈的证据。"""
    edges = graphs["edges"]
    reports = []
    for cyc in find_cycles(edges):
        cyc_edges = []
        for i, a in enumerate(cyc):
            b = cyc[(i + 1) % len(cyc)]
            for e in edges:
                if e["from"] == a and e["to"] == b:
                    cyc_edges.append(e)
                    break
        per_thread = {}
        for t in cyc:
            th = state["threads"].get(t, {})
            held_stack = [
                {"lock": l, "count": h["count"],
                 "acquire_events": list(h["acquire_events"]), "leaked": h["leaked"]}
                for l, h in th.get("held", {}).items()
            ]
            per_thread[t] = {"held_stack": held_stack, "waiting": th.get("waiting")}
        sig = tuple(sorted((e["from"], e["lock"], e["to"]) for e in cyc_edges))
        reports.append({
            "threads": list(cyc),
            "locks": sorted({e["lock"] for e in cyc_edges}),
            "edges": cyc_edges,
            "per_thread": per_thread,
            "signature": [list(s) for s in sig],
        })
    return reports


def _fp_events(events):
    h = hashlib.sha1()
    for e in events:
        h.update(repr((
            e.get("id"), e.get("kind"), e.get("thread"), e.get("lock"),
            e.get("ts"), sorted((e.get("extra") or {}).items()),
        )).encode())
    return h.hexdigest()


class Engine:
    """带快照缓存的增量重算引擎。追加事件只重放受影响的新区间。""""

    def __init__(self):
        self.cache = {}
        self.last_replayed = 0

    def state_for(self, key, events, declared, upto=None):
        total = len(events)
        target = total if upto is None else max(0, min(upto, total))
        ent = self.cache.get(key)
        if ent is not None:
            prefix_ok = (ent["declared"] == declared
                         and ent["count"] <= total
                         and ent["prefix_fp"] == _fp_events(events[:ent["count"]]))
            if not prefix_ok:
                ent = None
        if ent is None:
            ent = {"count": 0, "prefix_fp": _fp_events([]),
                   "final_state": new_state(),
                   "snapshots": [(0, new_state())],
                   "declared": copy.deepcopy(declared)}
            self.cache[key] = ent

        replayed = 0
        if total > ent["count"]:
            st = ent["final_state"]
            for i in range(ent["count"], total):
                process_event(st, events[i], declared)
                replayed += 1
                if (i + 1) % SNAPSHOT_INTERVAL == 0:
                    ent["snapshots"].append((i + 1, copy.deepcopy(st)))
            ent["count"] = total
            ent["prefix_fp"] = _fp_events(events)
            ent["final_state"] = st
            ent["snapshots"].append((total, copy.deepcopy(st)))

        if target == ent["count"]:
            self.last_replayed = replayed
            return ent["final_state"]

        best = 0
        snap_state = ent["snapshots"][0][1]
        for s, snap in ent["snapshots"]:
            if s <= target and s >= best:
                best, snap_state = s, snap
        st = copy.deepcopy(snap_state)
        for i in range(best, target):
            process_event(st, events[i], declared)
            replayed += 1
        self.last_replayed = replayed
        return st
