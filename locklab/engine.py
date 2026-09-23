"""事件回放引擎：由原始线程事件重建持有图 / 等待图，并检测死锁环。

设计要点：
- 锁身份 = (采集代次 epoch, 地址)，进程重启后同地址属于不同锁。
- 事件按 (epoch, seq) 原始顺序回放；时间戳乱序只降低置信度，不重排。
- 缺事件（缺 request / 缺 release / 乱序）时产出带 candidate 置信状态的候选图，
  绝不凭空补 release。
"""
from __future__ import annotations

import copy

CONFIRMED = "confirmed"
CANDIDATE = "candidate"

LOCK_TYPES = ("mutex", "reentrant", "rwlock")


def lock_key(epoch, lock):
    return f"{epoch}:{lock}"


def _new_thread():
    return {
        "held": {},       # lock_key -> {"count": int, "kind": str, "stack": [event_id]}
        "pending": None,  # 等待中的 acquire 请求
        "cond": None,     # condition wait 状态
        "ended": False,
    }


class State:
    """可增量推进的重建状态。"""

    def __init__(self, declarations=None):
        # declarations: {(epoch, lock_address): "mutex"|"reentrant"|"rwlock"}
        self.declarations = dict(declarations or {})
        self.threads = {}
        self.anomalies = []          # [{kind, event, detail, thread, lock}]
        self.tainted_threads = set()  # 乱序等异常波及的线程
        self.tainted_pairs = set()    # (thread, lock_key) 异常对
        self.prev_ts = {}             # epoch -> 上一个事件 ts（乱序检测）

    # ------------------------------------------------------------------ util
    def clone(self):
        dup = State(self.declarations)
        dup.threads = copy.deepcopy(self.threads)
        dup.anomalies = list(self.anomalies)
        dup.tainted_threads = set(self.tainted_threads)
        dup.tainted_pairs = set(self.tainted_pairs)
        dup.prev_ts = dict(self.prev_ts)
        return dup

    def _thread(self, name):
        return self.threads.setdefault(name, _new_thread())

    def _lock_type(self, epoch, lock):
        return self.declarations.get((epoch, lock), "reentrant")

    def _taint(self, thread=None, lkey=None):
        if thread:
            self.tainted_threads.add(thread)
        if thread and lkey:
            self.tainted_pairs.add((thread, lkey))

    def _anomaly(self, kind, event, thread=None, lkey=None, detail=""):
        self.anomalies.append({
            "kind": kind,
            "event": event.get("id"),
            "thread": thread,
            "lock": lkey,
            "detail": detail,
        })
        self._taint(thread, lkey)

    def confidence(self, thread, lkey):
        if thread in self.tainted_threads or (thread, lkey) in self.tainted_pairs:
            return CANDIDATE
        return CONFIRMED

    # -------------------------------------------------------------- mutation
    def expire_timeouts(self, ts):
        """把 timeout_at <= ts 的等待撤销（超时请求撤销等待）。"""
        for name, st in self.threads.items():
            pend = st["pending"]
            if pend and pend.get("timeout_at") is not None and pend["timeout_at"] <= ts:
                self.anomalies.append({
                    "kind": "timeout_cancel", "event": None, "thread": name,
                    "lock": pend["lock_key"],
                    "detail": "wait %s timed out at %s" % (pend.get("wait_id"), pend["timeout_at"]),
                })
                st["pending"] = None

    def apply(self, e):
        etype = e["type"]
        thread = e.get("thread")
        epoch = e.get("epoch", 0)
        ts = e.get("ts", 0)

        # 乱序检测：同一代次内 ts 倒退 -> 候选图
        last = self.prev_ts.get(epoch)
        if last is not None and ts < last:
            self._anomaly("out_of_order", e, thread=thread,
                          detail="ts %s < previous %s in epoch %s" % (ts, last, epoch))
        self.prev_ts[epoch] = ts

        # 先按当前时间撤销已超时等待
        self.expire_timeouts(ts)

        if etype == "thread_start":
            self._thread(thread)
            return
        st = self._thread(thread)

        if etype == "thread_end":
            if st["pending"]:
                self._anomaly("end_cancels_wait", e, thread=thread,
                              lkey=st["pending"]["lock_key"])
                st["pending"] = None
            if st["cond"]:
                self._anomaly("end_cancels_cond_wait", e, thread=thread,
                              lkey=st["cond"]["lock_key"])
                st["cond"] = None
            if st["held"]:
                # 线程终止释放全部持有；不补写 release 事件，只记录异常
                for lkey in st["held"]:
                    self._anomaly("end_with_holds", e, thread=thread, lkey=lkey)
                st["held"] = {}
            st["ended"] = True
            return

        lock = e.get("lock")
        lkey = lock_key(epoch, lock) if lock is not None else None

        if etype == "acquire_request":
            if st["pending"]:
                self._anomaly("request_superseded", e, thread=thread,
                              lkey=st["pending"]["lock_key"])
            timeout_ms = e.get("timeout_ms")
            st["pending"] = {
                "lock_key": lkey,
                "kind": e.get("kind") or "mutex",
                "wait_id": e.get("wait_id"),
                "since_event": e.get("id"),
                "since_ts": ts,
                "timeout_at": (ts + timeout_ms) if timeout_ms is not None else None,
                "restore": None,
            }
            return

        if etype == "acquire":
            pend = st["pending"]
            if pend and pend["lock_key"] == lkey and (
                    e.get("wait_id") is None or pend.get("wait_id") in (None, e.get("wait_id"))):
                restore = pend.get("restore")
                st["pending"] = None
            else:
                if pend and pend["lock_key"] != lkey:
                    self._anomaly("acquire_mismatch", e, thread=thread, lkey=lkey)
                else:
                    self._anomaly("success_without_request", e, thread=thread, lkey=lkey)
                restore = None
            held = st["held"].setdefault(lkey, {"count": 0, "kind": e.get("kind") or "mutex",
                                                "stack": []})
            if restore:
                # 重新竞争成功：恢复 cond_wait 前的持有深度
                held["count"] += restore["count"]
                held["stack"].extend(restore["events"])
            else:
                held["count"] += 1
            held["stack"].append(e.get("id"))
            if e.get("kind"):
                held["kind"] = e["kind"]
            return

        if etype == "release":
            held = st["held"].get(lkey)
            if not held:
                # 缺事件：release 无对应持有 —— 记录异常，不凭空补任何状态
                self._anomaly("release_without_hold", e, thread=thread, lkey=lkey)
                return
            held["count"] -= 1
            if held["stack"]:
                held["stack"].pop()
            if held["count"] <= 0:
                del st["held"][lkey]
            return

        if etype == "cond_wait":
            held = st["held"].get(lkey)
            if not held:
                self._anomaly("cond_wait_without_hold", e, thread=thread, lkey=lkey)
                return
            # condition wait 暂时完全释放互斥锁
            st["cond"] = {
                "cond": e.get("cond"),
                "lock_key": lkey,
                "count": held["count"],
                "events": list(held["stack"]),
                "since_event": e.get("id"),
                "since_ts": ts,
            }
            del st["held"][lkey]
            return

        if etype == "cond_notify":
            cond = e.get("cond")
            broadcast = bool(e.get("all"))
            waiters = [(n, s) for n, s in self.threads.items()
                       if s["cond"] and s["cond"]["cond"] == cond]
            if not waiters:
                self._anomaly("notify_without_waiter", e, thread=thread)
                return
            for name, wst in waiters if broadcast else waiters[:1]:
                cw = wst["cond"]
                wst["cond"] = None
                # 被唤醒后重新竞争原互斥锁
                wst["pending"] = {
                    "lock_key": cw["lock_key"],
                    "kind": "mutex",
                    "wait_id": None,
                    "since_event": e.get("id"),
                    "since_ts": ts,
                    "timeout_at": None,
                    "restore": {"count": cw["count"], "events": cw["events"]},
                }
            return

        self._anomaly("unknown_event", e, thread=thread, detail=etype)

    # ------------------------------------------------------------- snapshots
    def edges(self):
        holds, waits = [], []
        for name, st in self.threads.items():
            for lkey, h in st["held"].items():
                holds.append({
                    "thread": name, "lock": lkey, "count": h["count"], "kind": h["kind"],
                    "events": list(h["stack"]), "open": True,
                    "confidence": self.confidence(name, lkey),
                })
            pend = st["pending"]
            if pend:
                waits.append({
                    "thread": name, "lock": pend["lock_key"], "kind": pend["kind"],
                    "wait_id": pend.get("wait_id"), "since_event": pend["since_event"],
                    "since_ts": pend["since_ts"], "type": "lock",
                    "confidence": self.confidence(name, pend["lock_key"]),
                })
            if st["cond"]:
                waits.append({
                    "thread": name, "lock": None, "cond": st["cond"]["cond"],
                    "released_lock": st["cond"]["lock_key"],
                    "since_event": st["cond"]["since_event"], "type": "cond",
                    "confidence": self.confidence(name, st["cond"]["lock_key"]),
                })
        return holds, waits

    @staticmethod
    def _conflicts(wait_kind, hold_kind):
        if wait_kind == "read":
            return hold_kind == "write"
        return True  # write / mutex 与任何持有冲突

    def wait_for_edges(self, holds, waits):
        holders = {}
        for h in holds:
            holders.setdefault(h["lock"], []).append(h)
        edges = []
        for w in waits:
            if w["type"] != "lock":
                continue
            lkey = w["lock"]
            epoch, addr = lkey.split(":", 1)
            ltype = self._lock_type(epoch, addr)
            for h in holders.get(lkey, []):
                if h["thread"] == w["thread"]:
                    # 自等待：仅当声明为不可重入 mutex 时构成自死锁
                    if ltype == "mutex":
                        edges.append({
                            "from": w["thread"], "to": w["thread"], "lock": lkey,
                            "wait_event": w["since_event"], "hold_events": h["events"],
                            "confidence": CANDIDATE,
                        })
                    continue
                if self._conflicts(w["kind"], h["kind"]):
                    conf = CANDIDATE if (w["confidence"] == CANDIDATE
                                         or h["confidence"] == CANDIDATE) else CONFIRMED
                    edges.append({
                        "from": w["thread"], "to": h["thread"], "lock": lkey,
                        "wait_event": w["since_event"], "hold_events": h["events"],
                        "confidence": conf,
                    })
        return edges

    def snapshot(self, t=None):
        if t is not None:
            self.expire_timeouts(t)
        holds, waits = self.edges()
        wait_for = self.wait_for_edges(holds, waits)
        cycles = find_cycles(wait_for, holds, waits)
        return {
            "holds": holds,
            "waits": waits,
            "wait_for": wait_for,
            "cycles": cycles,
            "anomalies": list(self.anomalies),
        }


def find_cycles(wait_for, holds, waits):
    """在等待图上枚举所有基本环（含自环），并附带完整证据。"""
    adj = {}
    edge_map = {}
    for e in wait_for:
        adj.setdefault(e["from"], set()).add(e["to"])
        edge_map.setdefault((e["from"], e["to"]), e)
    nodes = sorted(adj)
    index = {n: i for i, n in enumerate(nodes)}
    found = []

    for start in nodes:
        # 自环
        if start in adj.get(start, ()):
            found.append([start])
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for nxt in adj.get(node, ()):
                if nxt == start and len(path) > 1:
                    found.append(list(path))
                elif nxt in adj and index.get(nxt, -1) > index[start] and nxt not in path:
                    stack.append((nxt, path + [nxt]))

    hold_by_thread = {}
    for h in holds:
        hold_by_thread.setdefault(h["thread"], []).append(h)
    wait_by_thread = {w["thread"]: w for w in waits if w["type"] == "lock"}

    cycles = []
    for i, cyc in enumerate(found):
        threads = list(cyc)
        locks, detail = [], {}
        for j, th in enumerate(threads):
            nxt = threads[(j + 1) % len(threads)]
            edge = edge_map.get((th, nxt))
            lock = edge["lock"] if edge else None
            if lock:
                locks.append(lock)
            w = wait_by_thread.get(th)
            detail[th] = {
                "waiting_for": {
                    "lock": lock,
                    "kind": w["kind"] if w else None,
                    "wait_id": w.get("wait_id") if w else None,
                    "since_event": w["since_event"] if w else None,
                    "since_ts": w.get("since_ts") if w else None,
                },
                "holding": [
                    {"lock": h["lock"], "count": h["count"], "kind": h["kind"],
                     "events": h["events"], "confidence": h["confidence"]}
                    for h in hold_by_thread.get(th, [])
                ],
                "wait_edge_confidence": edge["confidence"] if edge else None,
            }
        cycles.append({
            "id": i,
            "threads": threads,
            "locks": locks,
            "detail": detail,
            "confidence": CANDIDATE if any(
                edge_map[(threads[j], threads[(j + 1) % len(threads)])]["confidence"] == CANDIDATE
                for j in range(len(threads))) else CONFIRMED,
        })
    return cycles


def replay(events, t=None, declarations=None):
    """完整回放：按 (epoch, seq) 原始顺序应用 ts <= t 的事件。"""
    st = State(declarations)
    for e in sorted(events, key=lambda x: (x.get("epoch", 0), x.get("seq", 0))):
        if t is not None and e.get("ts", 0) > t:
            continue
        st.apply(e)
    return st.snapshot(t)


class Incremental:
    """流式重建：按段设检查点，追加事件只重算受影响的尾段。"""

    def __init__(self, declarations=None, segment=16):
        self.events = []
        self.declarations = dict(declarations or {})
        self.segment = max(1, segment)
        self.checkpoints = []  # checkpoints[i] = 处理完 (i+1)*segment 个事件后的 State
        self.stats = {"segments_recomputed": 0, "events_replayed": 0}

    def append(self, events):
        self.events.extend(events)
        start = len(self.checkpoints) * self.segment
        state = self.checkpoints[-1].clone() if self.checkpoints else State(self.declarations)
        recomputed = 0
        i = start
        while i < len(self.events):
            state.apply(self.events[i])
            self.stats["events_replayed"] += 1
            i += 1
            if i % self.segment == 0:
                self.checkpoints.append(state.clone())
                recomputed += 1
        self.stats["segments_recomputed"] += recomputed
        return recomputed

    def snapshot(self, t=None):
        if t is None:
            start = len(self.checkpoints) * self.segment
            state = self.checkpoints[-1].clone() if self.checkpoints else State(self.declarations)
            for e in self.events[start:]:
                state.apply(e)
            return state.snapshot()
        # 时间点快照：从不超过 t 的最近检查点续放
        base, state = 0, State(self.declarations)
        for k, cp in enumerate(self.checkpoints):
            last_ts = self.events[(k + 1) * self.segment - 1].get("ts", 0)
            if last_ts <= t:
                base, state = (k + 1) * self.segment, cp.clone()
        for e in self.events[base:]:
            if e.get("ts", 0) <= t:
                state.apply(e)
        return state.snapshot(t)
