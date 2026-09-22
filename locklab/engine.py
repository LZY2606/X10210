"""锁序解剖室核心引擎。

从原始锁事件流重建带置信状态的持有/等待关系，支持：
- 可重入锁、读写锁共享持有与升级
- condition wait / signal / broadcast / resume（等待期间暂时释放）
- 超时请求、线程终止撤销等待
- 缺事件 / 乱序到达下的候选图（不凭空补 release）
- 同地址跨进程重启（gen）分离
- 持有图 / 等待图，边可回溯到产生它的事件
- 具体环（线程、锁、持有栈、等待栈、事件证据）
- 保留配对与 condition 语义的最小化
- 流式追加与乱序插入后仅重算受影响时间段
"""
from __future__ import annotations

from copy import deepcopy
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

MUTEX = "mutex"
RECURSIVE = "recursive"
RWLOCK = "rwlock"
VALID_TYPES = (MUTEX, RECURSIVE, RWLOCK)

ACQ_REQUEST = "acquire_request"
ACQ_GRANTED = "acquire_granted"
RELEASE = "release"
THREAD_START = "thread_start"
THREAD_END = "thread_end"
COND_WAIT = "cond_wait"
COND_NOTIFY = "cond_notify"       # signal 单条 / broadcast 两条 notify
COND_RESUME = "cond_resume"

# 置信度：high（事件完整且有序）/ uncertain（乱序或授予缺失）/ low（缺释放等推断）
HIGH, UNCERTAIN, LOW = "high", "uncertain", "low"
CONF_RANK = {HIGH: 0, UNCERTAIN: 1, LOW: 2}


def worse(a: str, b: str) -> str:
    return a if CONF_RANK[a] >= CONF_RANK[b] else b


class EngineError(ValueError):
    pass


# ---------------------------------------------------------------------------
# 事件规范化与排序
# ---------------------------------------------------------------------------

def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把导入的 JSON 事件规范化为引擎内部事件。"""
    ev = dict(raw)
    etype = ev.get("type")
    if etype not in (
        ACQ_REQUEST, ACQ_GRANTED, RELEASE, THREAD_START, THREAD_END,
        COND_WAIT, COND_NOTIFY, COND_RESUME,
    ):
        raise EngineError("未知事件类型: %r" % (etype,))
    ev["gen"] = int(ev.get("gen", 0))
    ev["seq"] = int(ev["seq"])
    ev["ts"] = int(ev.get("ts", ev["seq"]))
    ev["id"] = int(ev.get("id", ev["seq"]))
    if "thread" in ev and ev["thread"] is not None:
        ev["thread"] = str(ev["thread"])
    if "lock" in ev and ev["lock"] is not None:
        ev["lock"] = str(ev["lock"])
    if "cond" in ev and ev["cond"] is not None:
        ev["cond"] = str(ev["cond"])
    # 采集代次（到达顺序），默认与 seq 一致
    ev["arrival"] = int(ev.get("arrival", ev["seq"]))
    if etype in (ACQ_REQUEST, ACQ_GRANTED, RELEASE, COND_WAIT, COND_RESUME):
        if not ev.get("thread") or not ev.get("lock"):
            raise EngineError("%s 需要 thread 与 lock" % etype)
    if etype == COND_NOTIFY and not ev.get("cond"):
        raise EngineError("cond_notify 需要 cond")
    if etype in (THREAD_START, THREAD_END) and not ev.get("thread"):
        raise EngineError("%s 需要 thread" % etype)
    if etype in (ACQ_REQUEST, ACQ_GRANTED, COND_WAIT, COND_RESUME):
        ev["mode"] = ev.get("mode", "X")
        if ev["mode"] not in ("S", "X"):
            raise EngineError("mode 只能是 S/X: %r" % ev["mode"])
    return ev


def materialize(events: List[Dict[str, Any]],
                lock_types: Dict[str, str]) -> List[Dict[str, Any]]:
    """分支物化：规范化、应用类型声明、按 (gen, seq) 保留原始时序。"""
    out = [normalize_event(e) for e in events]
    for ev in out:
        key = "%s:%s" % (ev["gen"], ev.get("lock"))
        if key in lock_types:
            ev["lock_type"] = lock_types[key]
    out.sort(key=lambda e: (e["gen"], e["seq"], e["arrival"], e["id"]))
    return out

# ---------------------------------------------------------------------------
# 编译器：把事件流重建成锁/线程状态
# ---------------------------------------------------------------------------

class Compiler:
    def __init__(self, lock_types: Optional[Dict[str, str]] = None):
        self.lock_types: Dict[str, str] = dict(lock_types or {})
        self.events: List[Dict[str, Any]] = []
        self.anomalies: List[Dict[str, Any]] = []
        self.threads: Dict[str, Dict[str, Any]] = {}
        self.locks: Dict[Tuple[int, str], Dict[str, Any]] = {}
        self.conds: Dict[Tuple[int, str], bool] = {}
        self.frames: Dict[int, Dict[str, Any]] = {}
        self.frame_stack: Dict[str, List[int]] = {}   # "gen:thread" -> fids
        self.holders: Dict[Tuple[int, str], Dict[str, Any]] = {}
        self.pending: Dict[str, Dict[str, Any]] = {}  # "gen:thread" -> request ev
        self.cond_pending: Dict[str, Dict[str, Any]] = {}
        self.waits: List[Dict[str, Any]] = []
        self._fid = 0

    def event_by_id(self, event_id: Optional[int]) -> Optional[Dict[str, Any]]:
        if event_id is None:
            return None
        for ev in self.events:
            if ev["id"] == event_id:
                return ev
        return None

    def event_disordered(self, event_id: Optional[int]) -> bool:
        ev = self.event_by_id(event_id)
        return bool(ev and ev.get("_disordered"))

    # -- 基础工具 ---------------------------------------------------------

    def _tkey(self, gen: int, thread: str) -> str:
        return "%s:%s" % (gen, thread)

    def _ltype(self, ev: Dict[str, Any]) -> str:
        t = self.lock_types.get("%s:%s" % (ev["gen"], ev.get("lock")))
        return t if t in VALID_TYPES else MUTEX

    def _note(self, ev: Optional[Dict[str, Any]], kind: str, confidence: str,
              message: str, thread: Optional[str] = None,
              lock: Optional[str] = None, cond: Optional[str] = None) -> None:
        self.anomalies.append({
            "id": len(self.anomalies) + 1,
            "event_id": ev["id"] if ev else None,
            "seq": ev["seq"] if ev else None,
            "gen": ev["gen"] if ev else None,
            "kind": kind,
            "confidence": confidence,
            "message": message,
            "thread": thread or (ev.get("thread") if ev else None),
            "lock": lock or (ev.get("lock") if ev else None),
            "cond": cond or (ev.get("cond") if ev else None),
        })

    def _ensure_thread(self, ev: Dict[str, Any]) -> Dict[str, Any]:
        key = self._tkey(ev["gen"], ev["thread"])
        th = self.threads.get(key)
        if th is None:
            self._note(ev, "missing_thread_start", LOW,
                       "线程 %s 没有 thread_start 事件" % ev["thread"])
            th = {"gen": ev["gen"], "name": ev["thread"],
                  "start": None, "end": None, "implicit": True}
            self.threads[key] = th
            self.frame_stack[key] = []
        return th

    def _lock_obj(self, gen: int, lock: str, ltype: str) -> Dict[str, Any]:
        key = (gen, lock)
        obj = self.locks.get(key)
        if obj is None:
            obj = {"gen": gen, "name": lock, "type": ltype,
                   "S": {}, "X": None, "fids": set()}
            self.locks[key] = obj
        else:
            if obj["type"] != ltype:
                obj["type"] = ltype
        return obj

    def _alive(self, gen: int, thread: str) -> bool:
        th = self.threads.get(self._tkey(gen, thread))
        return th is None or th["end"] is None

    def _top_frame_for(self, gen: int, thread: str,
                       lock: Optional[str] = None) -> Optional[Dict[str, Any]]:
        fids = self.frame_stack.get(self._tkey(gen, thread), [])
        for fid in reversed(fids):
            fr = self.frames[fid]
            if lock is None or fr["lock"] == lock:
                return fr
        return None

    def _holds_lock(self, gen: int, thread: str, lock: str) -> Optional[Dict[str, Any]]:
        return self._top_frame_for(gen, thread, lock)

    def _blockers(self, ev: Dict[str, Any]) -> List[str]:
        """当前时刻该请求被哪些线程挡住。"""
        gen, lock, mode, thread = ev["gen"], ev["lock"], ev["mode"], ev["thread"]
        lk = self.holders.get((gen, lock))
        if not lk:
            return []
        if mode == "X":
            if lk["X"]:
                owner = lk["X"][0]
                return [owner] if owner != thread else ["__self__"]
            others = [t for t in lk["S"] if t != thread]
            return others
        # S 请求只被 X 持有者挡
        if lk["X"]:
            owner = lk["X"][0]
            return [owner] if owner != thread else ["__self__"]
        return []

    # -- 持有帧 -----------------------------------------------------------

    def _new_frame(self, ev: Dict[str, Any], mode: str,
                   confidence: str = HIGH) -> Dict[str, Any]:
        self._fid += 1
        fid = self._fid
        fr = {
            "fid": fid,
            "gen": ev["gen"],
            "thread": ev["thread"],
            "lock": ev["lock"],
            "type": self._ltype(ev),
            "mode": mode,
            "depth": 1,
            "grant_seq": ev["seq"],
            "grant_ts": ev["ts"],
            "grant_event": ev["id"],
            "release_seq": None,
            "release_ts": None,
            "release_event": None,
            "abandoned": False,
            "abandon_reason": None,
            "confidence": confidence,
            "suspensions": [],
        }
        self.frames[fid] = fr
        self.frame_stack.setdefault(self._tkey(ev["gen"], ev["thread"]), []).append(fid)
        return fr

    def _add_holder(self, fr: Dict[str, Any]) -> None:
        key = (fr["gen"], fr["lock"])
        h = self.holders.setdefault(key, {"S": {}, "X": None, "fids": set()})
        h["fids"].add(fr["fid"])
        if fr["mode"] == "X":
            h["X"] = (fr["thread"], fr["fid"])
        else:
            h["S"][fr["thread"]] = fr["fid"]

    def _close_frame(self, fr: Dict[str, Any], ev: Dict[str, Any]) -> None:
        fr["release_seq"] = ev["seq"]
        fr["release_ts"] = ev["ts"]
        fr["release_event"] = ev["id"]
        stack = self.frame_stack[self._tkey(fr["gen"], fr["thread"])]
        if fr["fid"] in stack:
            stack.remove(fr["fid"])

    def _remove_holder(self, fr: Dict[str, Any]) -> None:
        key = (fr["gen"], fr["lock"])
        h = self.holders.get(key)
        if not h:
            return
        h["fids"].discard(fr["fid"])
        if fr["mode"] == "X" and h["X"] and h["X"][1] == fr["fid"]:
            h["X"] = None
        if fr["thread"] in h["S"] and h["S"][fr["thread"]] == fr["fid"]:
            del h["S"][fr["thread"]]

    def _abandon_frame(self, fr: Dict[str, Any], reason: str,
                       seq: int, ts: int, confidence: str = LOW) -> None:
        if fr["release_seq"] is not None:
            return
        fr["abandoned"] = True
        fr["abandon_reason"] = reason
        fr["release_seq"] = seq
        fr["release_ts"] = ts
        fr["release_event"] = None
        fr["confidence"] = worse(fr["confidence"], confidence)

    # -- 事件处理 ---------------------------------------------------------

    def _on_thread_start(self, ev: Dict[str, Any]) -> None:
        key = self._tkey(ev["gen"], ev["thread"])
        if key in self.threads and self.threads[key]["start"] is not None:
            self._note(ev, "duplicate_thread_start", LOW,
                       "线程 %s 重复 start" % ev["thread"])
            return
        th = self.threads.get(key)
        if th is None:
            th = {"gen": ev["gen"], "name": ev["thread"],
                  "start": ev["seq"], "end": None, "implicit": False}
            self.threads[key] = th
            self.frame_stack[key] = []
        else:
            th["start"] = ev["seq"]
            th["implicit"] = False

    def _on_thread_end(self, ev: Dict[str, Any]) -> None:
        th = self._ensure_thread(ev)
        if th["end"] is not None:
            self._note(ev, "duplicate_thread_end", LOW,
                       "线程 %s 重复 end" % ev["thread"])
        th["end"] = ev["seq"]
        tk = self._tkey(ev["gen"], ev["thread"])
        # 终止撤销等待（不发 grant/release，仅标注等待被线程退出撤销）
        if tk in self.pending:
            req = self.pending.pop(tk)
            self.waits.append({
                "gen": ev["gen"], "thread": ev["thread"],
                "lock": req["lock"], "kind": "lock",
                "mode": req["mode"], "request_seq": req["seq"],
                "end_seq": ev["seq"], "end_ts": ev["ts"],
                "end_event": ev["id"], "outcome": "thread_exit",
                "confidence": HIGH, "request_event": req["id"],
                "grant_event": None, "cond": None,
            })
        if tk in self.cond_pending:
            cp = self.cond_pending.pop(tk)
            self.waits.append({
                "gen": ev["gen"], "thread": ev["thread"],
                "lock": cp["lock"], "kind": "cond",
                "mode": cp["mode"], "request_seq": cp["seq"],
                "end_seq": ev["seq"], "end_ts": ev["ts"],
                "end_event": ev["id"], "outcome": "thread_exit",
                "confidence": HIGH, "request_event": cp["id"],
                "grant_event": None, "cond": cp["cond"],
            })
            for fid in cp["suspended"]:
                fr0 = self.frames[fid]
                if fr0["suspensions"] and fr0["suspensions"][-1]["end_seq"] is None:
                    fr0["suspensions"][-1].update({
                        "end_seq": ev["seq"], "end_ts": ev["ts"],
                        "end_event": ev["id"]})
                self._abandon_frame(self.frames[fid], "cond_suspended_thread_exit",
                                    ev["seq"], ev["ts"])
        # 已持有但未释放：标记为 abandoned，保留帧不凭空 release
        for fid in list(self.frame_stack.get(tk, [])):
            fr = self.frames[fid]
            self._abandon_frame(fr, "thread_exit", ev["seq"], ev["ts"])
            self._remove_holder(fr)
            self._note(ev, "missing_release", LOW,
                       "线程 %s 退出时仍持有锁 %s（无 release 事件）"
                       % (ev["thread"], fr["lock"]),
                       lock=fr["lock"])

    def _on_acquire_request(self, ev: Dict[str, Any]) -> None:
        self._ensure_thread(ev)
        gen, thread, lock, mode = ev["gen"], ev["thread"], ev["lock"], ev["mode"]
        tk = self._tkey(gen, thread)
        if not self._alive(gen, thread):
            self._note(ev, "event_after_thread_end", LOW,
                       "线程 %s 结束后仍发起请求" % thread)
        if tk in self.pending or tk in self.cond_pending:
            self._note(ev, "overlapping_request", LOW,
                       "线程 %s 在已有等待期间重复请求 %s" % (thread, lock),
                       lock=lock)
            return
        ltype = self._ltype(ev)
        self._lock_obj(gen, lock, ltype)
        held = self._holds_lock(gen, thread, lock)
        if held is not None:
            # 可重入判定
            if ltype == RECURSIVE:
                held["depth"] += 1
                ev["_frame"] = held["fid"]
                return
            if ltype == RWLOCK and held["mode"] == "S" and mode == "S":
                held["depth"] += 1
                ev["_frame"] = held["fid"]
                return
            if ltype == RWLOCK and held["mode"] == "S" and mode == "X":
                # 升级：没有其它 X 持有者时才等待；自身 S 不挡自己
                blockers = self._blockers(ev)
                if "__self__" in blockers:
                    self._note(ev, "self_deadlock", LOW,
                               "线程 %s 重复获取非重入锁 %s（自锁死锁）"
                               % (thread, lock), lock=lock)
                    blockers = []
                ev["_upgrading_fid"] = held["fid"]
                if blockers:
                    self.pending[tk] = ev
                else:
                    self._grant_inline(ev, held, upgrade=True)
                return
            self._note(ev, "self_deadlock", LOW,
                       "线程 %s 重复获取非重入锁 %s（自锁死锁）"
                       % (thread, lock), lock=lock)
            return
        blockers = self._blockers(ev)
        if "__self__" in blockers:
            self._note(ev, "self_deadlock", LOW,
                       "线程 %s 自锁 %s" % (thread, lock), lock=lock)
            return
        if blockers:
            self.pending[tk] = ev
        # 无阻塞：仍等待显式 granted（采集模型中 request 不会自动成功）

    def _grant_inline(self, ev: Dict[str, Any], held: Dict[str, Any],
                      upgrade: bool = False) -> None:
        held["mode"] = "X"
        key = (ev["gen"], ev["lock"])
        h = self.holders[key]
        if ev["thread"] in h["S"] and h["S"][ev["thread"]] == held["fid"]:
            del h["S"][ev["thread"]]
        h["X"] = (ev["thread"], held["fid"])
        ev["_frame"] = held["fid"]
        ev["_upgraded"] = True

    def _on_acquire_granted(self, ev: Dict[str, Any]) -> None:
        self._ensure_thread(ev)
        gen, thread, lock, mode = ev["gen"], ev["thread"], ev["lock"], ev["mode"]
        tk = self._tkey(gen, thread)
        ltype = self._ltype(ev)
        lk = self._lock_obj(gen, lock, ltype)
        req = self.pending.pop(tk, None)
        if req is not None:
            blockers_at_request = self._blockers(req)
            blockers_now = self._blockers(ev)
            if blockers_at_request or blockers_now:
                # 授予时仍有持有者：记录“冲突授予”（可能漏 release 或事件不全）
                fr = self._new_frame(ev, mode, confidence=UNCERTAIN)
                self._add_holder(fr)
                self._note(ev, "conflicting_grant", UNCERTAIN,
                           "锁 %s 在仍被占用时授予 %s（可能缺 release）"
                           % (lock, thread), lock=lock)
                self._waits_append(req, ev, "granted_conflict", UNCERTAIN)
                return
            fr = self._new_frame(ev, mode)
            self._add_holder(fr)
            self._waits_append(req, ev, "granted", HIGH)
            return
        # 没有 pending request
        held = self._holds_lock(gen, thread, lock)
        up_fid = req_meta_upgrade = None
        # request 可能在乱序下后到：记录为无请求授予，稍后 reconcile
        # 升级 request 已内联处理的情况
        if held is not None and ltype == RECURSIVE:
            held["depth"] += 1
            return
        if held is not None and ltype == RWLOCK and held["mode"] == "S" and mode == "X":
            self._grant_inline(ev, held, upgrade=True)
            self._note(ev, "upgraded_grant_without_request", UNCERTAIN,
                       "锁 %s 的升级授予缺少匹配 request" % lock, lock=lock)
            return
        fr = self._new_frame(ev, mode, confidence=UNCERTAIN)
        self._add_holder(fr)
        self._note(ev, "grant_without_request", UNCERTAIN,
                   "锁 %s 授予线程 %s 但没有匹配的 acquire_request"
                   % (lock, thread), lock=lock)

    def _waits_append(self, req: Dict[str, Any], end_ev: Dict[str, Any],
                      outcome: str, confidence: str) -> None:
        self.waits.append({
            "gen": req["gen"], "thread": req["thread"], "lock": req["lock"],
            "kind": "lock", "mode": req["mode"],
            "request_seq": req["seq"], "end_seq": end_ev["seq"],
            "end_ts": end_ev["ts"], "end_event": end_ev["id"],
            "outcome": outcome, "confidence": confidence,
            "request_event": req["id"], "grant_event": end_ev["id"],
            "cond": None,
        })

    def _on_release(self, ev: Dict[str, Any]) -> None:
        self._ensure_thread(ev)
        gen, thread, lock = ev["gen"], ev["thread"], ev["lock"]
        fr = self._holds_lock(gen, thread, lock)
        if fr is None:
            self._note(ev, "release_without_hold", LOW,
                       "线程 %s 释放未持有的锁 %s（可能缺 request/granted）"
                       % (thread, lock), lock=lock)
            return
        ltype = self._ltype(ev)
        if ltype == RECURSIVE:
            fr["depth"] -= 1
            if fr["depth"] > 0:
                return
        elif ltype == RWLOCK:
            fr["depth"] -= 1
            if fr["depth"] > 0:
                return
        self._remove_holder(fr)
        self._close_frame(fr, ev)

    # -- condition --------------------------------------------------------

    def _on_cond_wait(self, ev: Dict[str, Any]) -> None:
        self._ensure_thread(ev)
        gen, thread, lock = ev["gen"], ev["thread"], ev["lock"]
        tk = self._tkey(gen, thread)
        fr = self._holds_lock(gen, thread, lock)
        if fr is None:
            self._note(ev, "cond_wait_without_hold", LOW,
                       "线程 %s 在未持锁 %s 时进入 cond_wait" % (thread, lock),
                       lock=lock, cond=ev.get("cond"))
            suspended = []
        else:
            suspended = []
            # 暂停当前持有的该锁帧（通常即顶层锁）；语义上 wait 原子释放 mutex
            for fid in list(self.frame_stack.get(tk, [])):
                cand = self.frames[fid]
                if cand["lock"] == lock and cand["release_seq"] is None:
                    self._remove_holder(cand)
                    cand["suspensions"].append({
                        "start_seq": ev["seq"], "start_ts": ev["ts"],
                        "start_event": ev["id"],
                        "end_seq": None, "end_ts": None, "end_event": None,
                    })
                    suspended.append(fid)
        self.cond_pending[tk] = {
            "gen": gen, "thread": thread, "lock": lock,
            "cond": ev.get("cond"), "mode": ev.get("mode", "X"),
            "seq": ev["seq"], "ts": ev["ts"], "id": ev["id"],
            "suspended": suspended, "notified": False,
        }

    def _on_cond_notify(self, ev: Dict[str, Any]) -> None:
        gen = ev["gen"]
        cond = ev["cond"]
        # signal 唤醒 1 个，broadcast 唤醒全部
        want = 1 if ev.get("kind", "signal") != "broadcast" else 10 ** 9
        woken = 0
        for tk, cp in list(self.cond_pending.items()):
            if cp["gen"] != gen or cp["cond"] != cond:
                continue
            if ev.get("thread") is not None and cp["thread"] == ev["thread"]:
                continue
            cp["notified"] = True
            woken += 1
            if woken >= want:
                break
        if woken == 0:
            self._note(ev, "notify_no_waiter", UNCERTAIN,
                       "cond %s 通知时没有等待者" % cond, cond=cond)

    def _restore_suspended(self, cp: Dict[str, Any], confidence: str,
                           end_seq: Optional[int] = None,
                           end_ts: Optional[int] = None,
                           end_event: Optional[int] = None) -> None:
        for fid in cp["suspended"]:
            fr = self.frames[fid]
            if fr.get("abandon_reason") == "cond_suspended_thread_exit":
                continue
            if fr["suspensions"] and fr["suspensions"][-1]["end_seq"] is None:
                fr["suspensions"][-1].update({
                    "end_seq": end_seq, "end_ts": end_ts, "end_event": end_event})
            if fr["release_seq"] is not None and not fr["abandoned"]:
                continue
            fr["abandoned"] = False
            fr["abandon_reason"] = None
            fr["release_seq"] = None
            fr["release_ts"] = None
            fr["release_event"] = None
            fr["confidence"] = worse(fr["confidence"], confidence)
            self._add_holder(fr)

    def _on_cond_resume(self, ev: Dict[str, Any]) -> None:
        self._ensure_thread(ev)
        gen, thread, lock = ev["gen"], ev["thread"], ev["lock"]
        tk = self._tkey(gen, thread)
        cp = self.cond_pending.pop(tk, None)
        if cp is None:
            self._note(ev, "resume_without_wait", LOW,
                       "线程 %s 的 cond_resume 没有匹配的 cond_wait" % thread,
                       lock=lock, cond=ev.get("cond"))
            return
        confidence = HIGH if cp["notified"] else UNCERTAIN
        blockers = self._blockers({**ev, "mode": cp["mode"]})
        if blockers and "__self__" not in blockers:
            # 唤醒后重新竞争仍阻塞：转为普通等待，直到获得锁（缺事件下保持候选）
        self._restore_suspended(cp, confidence, ev["seq"], ev["ts"], ev["id"])
        fake_req = {
                "gen": gen, "thread": thread, "lock": lock,
                "mode": cp["mode"], "seq": ev["seq"], "ts": ev["ts"],
                "id": ev["id"], "type": ACQ_REQUEST, "_from_cond": True,
            }
            self.pending[tk] = fake_req
            self.waits.append({
                "gen": gen, "thread": thread, "lock": lock, "kind": "cond",
                "mode": cp["mode"], "request_seq": cp["seq"],
                "end_seq": None, "end_ts": None, "end_event": None,
                "outcome": "reacquire_waiting", "confidence": confidence,
                "request_event": cp["id"], "grant_event": None,
                "cond": cp["cond"],
            })
            return
        self._restore_suspended(cp, confidence, ev["seq"], ev["ts"], ev["id"])
        if cp["suspended"]:
            fr = self.frames[cp["suspended"][0]]
            if cp["mode"] == "X":
                # 恢复为 X 持有（重新获得）
                key = (gen, lock)
                h = self.holders.setdefault(key, {"S": {}, "X": None, "fids": set()})
                if fr["mode"] == "S":
                    h["S"].pop(thread, None)
                fr["mode"] = "X"
                h["X"] = (thread, fr["fid"])
                h["fids"].add(fr["fid"])
        self.waits.append({
            "gen": gen, "thread": thread, "lock": lock, "kind": "cond",
            "mode": cp["mode"], "request_seq": cp["seq"],
            "end_seq": ev["seq"], "end_ts": ev["ts"], "end_event": ev["id"],
            "outcome": "resumed" if cp["notified"] else "resumed_unsignaled",
            "confidence": confidence, "request_event": cp["id"],
            "grant_event": ev["id"], "cond": cp["cond"],
        })

    def _on_timeout(self, ev: Dict[str, Any]) -> None:
        gen, thread = ev["gen"], ev["thread"]
        tk = self._tkey(gen, thread)
        if tk in self.pending:
            req = self.pending.pop(tk)
            self.waits.append({
                "gen": gen, "thread": thread, "lock": req["lock"],
                "kind": "lock", "mode": req["mode"],
                "request_seq": req["seq"], "end_seq": ev["seq"],
                "end_ts": ev["ts"], "end_event": ev["id"],
                "outcome": "timeout", "confidence": HIGH,
                "request_event": req["id"], "grant_event": None, "cond": None,
            })
            return
        if tk in self.cond_pending:
            cp = self.cond_pending.pop(tk)
            self._restore_suspended(cp, UNCERTAIN, ev["seq"], ev["ts"], ev["id"])
            self.waits.append({
                "gen": gen, "thread": thread, "lock": cp["lock"],
                "kind": "cond", "mode": cp["mode"],
                "request_seq": cp["seq"], "end_seq": ev["seq"],
                "end_ts": ev["ts"], "end_event": ev["id"],
                "outcome": "timeout", "confidence": HIGH,
                "request_event": cp["id"], "grant_event": None,
                "cond": cp["cond"],
            })
            return
        self._note(ev, "timeout_without_wait", UNCERTAIN,
                   "线程 %s 超时事件没有匹配的等待" % thread)

    # -- 驱动 -------------------------------------------------------------

    HANDLERS = None  # 在类定义后填充

    def feed(self, events: List[Dict[str, Any]],
             start_index: int = 0) -> None:
        if Compiler.HANDLERS is None:
            Compiler.HANDLERS = {
                THREAD_START: Compiler._on_thread_start,
                THREAD_END: Compiler._on_thread_end,
                ACQ_REQUEST: Compiler._on_acquire_request,
                ACQ_GRANTED: Compiler._on_acquire_granted,
                RELEASE: Compiler._on_release,
                COND_WAIT: Compiler._on_cond_wait,
                COND_NOTIFY: Compiler._on_cond_notify,
                COND_RESUME: Compiler._on_cond_resume,
            }
        for ev in events:
            self.events.append(ev)
            if ev["type"] == "timeout":
                self._on_timeout(ev)
                continue
            handler = Compiler.HANDLERS.get(ev["type"])
            if handler:
                handler(self, ev)

    def checkpoint(self) -> Dict[str, Any]:
        return {
            "lock_types": dict(self.lock_types),
            "events": deepcopy(self.events),
            "anomalies": deepcopy(self.anomalies),
            "threads": deepcopy(self.threads),
            "locks": {k: {"gen": v["gen"], "name": v["name"],
                          "type": v["type"]} for k, v in self.locks.items()},
            "conds": dict(self.conds),
            "frames": deepcopy(self.frames),
            "frame_stack": {k: list(v) for k, v in self.frame_stack.items()},
            "holders": {("%s|%s" % k[0], k[1]): {
                            "S": dict(v["S"]),
                            "X": v["X"],
                            "fids": set(v["fids"])}
                        for k, v in self.holders.items()},
            "pending": deepcopy(self.pending),
            "cond_pending": deepcopy(self.cond_pending),
            "waits": deepcopy(self.waits),
            "_fid": self._fid,
        }

    @classmethod
    def restore(cls, cp: Dict[str, Any]) -> "Compiler":
        c = cls(cp["lock_types"])
        c.events = cp["events"]
        c.anomalies = cp["anomalies"]
        c.threads = cp["threads"]
        c.conds = cp["conds"]
        c.frames = cp["frames"]
        for fr in c.frames.values():
            fr.setdefault("suspensions", [])
        c.frame_stack = cp["frame_stack"]
        c.pending = cp["pending"]
        c.cond_pending = cp["cond_pending"]
        c.waits = cp["waits"]
        c._fid = cp["_fid"]
        for (gen, name), info in cp["locks"].items():
            c.locks[(gen, name)] = {
                "gen": gen, "name": name, "type": info["type"],
                "S": {}, "X": None, "fids": set()}
        for packed, v in cp["holders"].items():
            gen_s, name = packed.split("|", 1)
            c.holders[(int(gen_s), name)] = {
                "S": v["S"], "X": v["X"], "fids": set(v["fids"])}
        return c

    def finalize(self) -> None:
        last_seq = max((e["seq"] for e in self.events), default=0)
        last_ts = max((e["ts"] for e in self.events), default=0)
        # 未决 request：仍处于等待状态（开放区间）；若阻塞者已消失则缺授予
        for tk, req in list(self.pending.items()):
            blockers = self._blockers(req)
            outcome = "unresolved"
            conf = HIGH
            if not blockers:
                outcome = "missing_grant"
                conf = UNCERTAIN
                self._note(req, "missing_grant", UNCERTAIN,
                           "线程 %s 对 %s 的请求再无授予事件"
                           % (req["thread"], req["lock"]),
                           thread=req["thread"], lock=req["lock"])
            self.waits.append({
                "gen": req["gen"], "thread": req["thread"],
                "lock": req["lock"], "kind": "lock", "mode": req["mode"],
                "request_seq": req["seq"], "end_seq": None, "end_ts": None,
                "end_event": None, "outcome": outcome, "confidence": conf,
                "request_event": req["id"], "grant_event": None, "cond": None,
            })
        for tk, cp in list(self.cond_pending.items()):
            self.waits.append({
                "gen": cp["gen"], "thread": cp["thread"], "lock": cp["lock"],
                "kind": "cond", "mode": cp["mode"],
                "request_seq": cp["seq"], "end_seq": None, "end_ts": None,
                "end_event": None, "outcome": "cond_unresolved",
                "confidence": HIGH, "request_event": cp["id"],
                "grant_event": None, "cond": cp["cond"],
            })
        # 仍持有的锁：不补 release；标注缺释放，帧保持开放（末态仍可见）
        for fid, fr in self.frames.items():
            fr.setdefault("suspensions", [])
            if fr["release_seq"] is None:
                th = self.threads.get(self._tkey(fr["gen"], fr["thread"]))
                if th and th["end"] is not None:
                    continue  # thread_end 已处理
                fr["confidence"] = worse(fr["confidence"], LOW)
                self._note({"id": fr["grant_event"], "seq": fr["grant_seq"],
                            "gen": fr["gen"]}, "missing_release", LOW,
                           "锁 %s 授予线程 %s 后缺少 release"
                           % (fr["lock"], fr["thread"]),
                           thread=fr["thread"], lock=fr["lock"])
        _ = (last_seq, last_ts)

# ---------------------------------------------------------------------------
# 建图、快照与环
# ---------------------------------------------------------------------------

def _lock_id(gen: int, name: str) -> str:
    return "L:%s:%s" % (gen, name)


def _thread_id(gen: int, name: str) -> str:
    return "T:%s:%s" % (gen, name)


def _cond_id(gen: int, name: str) -> str:
    return "C:%s:%s" % (gen, name)


def _frame_active_at(fr: Dict[str, Any], pos: int) -> bool:
    if fr["grant_seq"] > pos:
        return False
    if fr["release_seq"] is not None and fr["release_seq"] <= pos:
        return False
    for s in fr.get("suspensions", []):
        if s["start_seq"] <= pos and (s["end_seq"] is None or s["end_seq"] > pos):
            return False
    return True


def _mark_disorder(events: List[Dict[str, Any]]) -> None:
    """乱序到达：arrival 顺序与 (gen, seq) 不一致的事件标 uncertain。"""
    by_gen: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for ev in events:
        by_gen[ev["gen"]].append(ev)
    for evs in by_gen.values():
        max_seq = -1
        for ev in sorted(evs, key=lambda e: e["arrival"]):
            ev["_disordered"] = ev["seq"] < max_seq
            max_seq = max(max_seq, ev["seq"])


def build_snapshot(comp: "Compiler", pos: Optional[int],
                   gen: Optional[int] = None) -> Dict[str, Any]:
    """在时点 pos（按 gen 内 seq）构造持有图 + 等待图，边可回溯事件。"""
    frames = list(comp.frames.values())
    if gen is not None:
        frames = [f for f in frames if f["gen"] == gen]
    active_frames = [f for f in frames if _frame_active_at(f, pos if pos is not None else 10 ** 18)]
    # 每个锁当前的持有者
    holders: Dict[Tuple[int, str], List[Dict[str, Any]]] = defaultdict(list)
    for fr in active_frames:
        holders[(fr["gen"], fr["lock"])].append(fr)

    hold_edges = []
    for fr in active_frames:
        conf = fr["confidence"]
        for s in fr.get("suspensions", []):
            if s["start_event"] and comp.event_disordered(s["start_event"]):
                conf = worse(conf, UNCERTAIN)
        hold_edges.append({
            "source": _thread_id(fr["gen"], fr["thread"]),
            "target": _lock_id(fr["gen"], fr["lock"]),
            "kind": "hold", "mode": fr["mode"],
            "lock_type": fr["type"], "fid": fr["fid"],
            "depth": fr["depth"], "confidence": conf,
            "grant_event": fr["grant_event"],
            "grant_seq": fr["grant_seq"],
            "release_event": fr["release_event"],
            "abandoned": fr["abandoned"],
        })

    wait_edges = []
    cond_edges = []
    # 活跃等待：request 已发生且终止事件未发生（end None 或 > pos）
    for w in comp.waits:
        if gen is not None and w["gen"] != gen:
            continue
        if w["request_seq"] > (pos if pos is not None else 10 ** 18):
            continue
        if w["end_seq"] is not None and w["end_seq"] <= (pos if pos is not None else 10 ** 18):
            continue
        req_ev = comp.event_by_id(w["request_event"])
        conf = w["confidence"]
        if req_ev and req_ev.get("_disordered"):
            conf = worse(conf, UNCERTAIN)
        if w["kind"] == "cond":
            cond_edges.append({
                "source": _thread_id(w["gen"], w["thread"]),
                "target": _cond_id(w["gen"], w["cond"] or "?"),
                "kind": "cond_wait", "mode": w["mode"],
                "confidence": conf, "request_event": w["request_event"],
                "request_seq": w["request_seq"], "resume_event": None,
                "lock": w["lock"], "outcome": w["outcome"],
            })
            continue
        owners = holders.get((w["gen"], w["lock"]), [])
        if not owners:
            # 无人持有却等待：缺授予 / 状态悬空
            conf = worse(conf, UNCERTAIN if w["outcome"] == "unresolved" else LOW)
        for ofr in owners:
            if ofr["thread"] == w["thread"]:
                continue
            edge_conf = conf
            if ofr["confidence"] != HIGH:
                edge_conf = worse(edge_conf, ofr["confidence"])
            wait_edges.append({
                "source": _lock_id(w["gen"], w["lock"]),
                "target": _thread_id(w["gen"], w["thread"]),
                "blocker_thread": ofr["thread"],
                "blocker_fid": ofr["fid"],
                "kind": "wait", "mode": w["mode"],
                "confidence": edge_conf,
                "request_event": w["request_event"],
                "request_seq": w["request_seq"],
                "grant_event_blocker": ofr["grant_event"],
                "outcome": w["outcome"],
            })

    nodes = _collect_nodes(comp, gen)
    cycles = detect_cycles(comp, hold_edges, wait_edges, active_frames, gen)
    return {
        "pos": pos,
        "nodes": nodes,
        "hold_edges": hold_edges,
        "wait_edges": wait_edges,
        "cond_edges": cond_edges,
        "cycles": cycles,
        "active_frame_count": len(active_frames),
    }

def _collect_nodes(comp: "Compiler", gen: Optional[int]) -> List[Dict[str, Any]]:
    nodes = []
    for key, th in comp.threads.items():
        if gen is not None and th["gen"] != gen:
            continue
        nodes.append({"id": _thread_id(th["gen"], th["name"]),
                      "kind": "thread", "label": th["name"],
                      "start": th["start"], "end": th["end"],
                      "implicit": th.get("implicit", False)})
    for (g, name), lk in comp.locks.items():
        if gen is not None and g != gen:
            continue
        nodes.append({"id": _lock_id(g, name), "kind": "lock",
                      "label": name, "lock_type": lk["type"]})
    conds = set()
    for w in comp.waits:
        if w.get("cond"):
            conds.add((w["gen"], w["cond"]))
    for g, name in conds:
        if gen is not None and g != gen:
            continue
        nodes.append({"id": _cond_id(g, name), "kind": "cond", "label": name})
    return nodes


def _wait_for_graph(hold_edges: List[Dict[str, Any]],
                    wait_edges: List[Dict[str, Any]]
                    ) -> Tuple[Dict[str, List[Tuple[str, Dict[str, Any]]]],
                               Dict[Tuple[str, str], Dict[str, Any]]]:
    """线程级 wait-for 图：
    hold: thread -> lock；wait: lock -> blocked thread。
    组合得 thread --(holds L, waits-for)--> thread。
    """
    lock_owners: Dict[str, List[str]] = defaultdict(list)
    owner_edge: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in hold_edges:
        lock_owners[e["target"]].append(e["source"])
        owner_edge[(e["source"], e["target"])] = e
    adj: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
    for we in wait_edges:
        lock_node, blocked_thread = we["source"], we["target"]
        he = owner_edge.get((_thread_id_owner(we), lock_node)) if False else None
        owners = lock_owners.get(lock_node, [])
        for owner in owners:
            if owner == blocked_thread:
                continue
            adj[owner].append((blocked_thread, {
                "lock_node": lock_node,
                "hold": owner_edge.get((owner, lock_node)),
                "wait": we,
            }))
    return adj, owner_edge


def _thread_id_owner(we: Dict[str, Any]) -> str:  # 保留占位，实际不用
    return ""


def detect_cycles(comp: "Compiler", hold_edges: List[Dict[str, Any]],
                  wait_edges: List[Dict[str, Any]],
                  active_frames: List[Dict[str, Any]],
                  gen: Optional[int]) -> List[Dict[str, Any]]:
    adj, _ = _wait_for_graph(hold_edges, wait_edges)
    nodes = set(adj.keys())
    for _, outs in adj.items():
        for t, _ in outs:
            nodes.add(t)
    cycles_raw = enumerate_simple_cycles(adj, nodes, cap=50)
    cycles = []
    seen = set()
    for cyc in cycles_raw:
        # 规范化签名
        m = min(range(len(cyc)))
        rot = tuple(cyc[m:] + cyc[:m])
        if rot in seen:
            continue
        seen.add(rot)
        detail = _cycle_detail(comp, adj, cyc, gen)
        cycles.append(detail)
    cycles.sort(key=lambda c: (len(c["thread_cycle"]), c["signature"]))
    return cycles


def enumerate_simple_cycles(
        adj: Dict[str, List[Tuple[str, Dict[str, Any]]]],
        nodes: set, cap: int = 50) -> List[List[str]]:
    """枚举有向简单环（Johnson 风格的轻量 DFS）。"""
    result: List[List[str]] = []
    node_list = sorted(nodes)
    index = {n: i for i, n in enumerate(node_list)}

    def dfs(start: str, current: str, path: List[str], visited: set):
        if len(result) >= cap:
            return
        for nxt, _meta in adj.get(current, []):
            if nxt not in index or index[nxt] < index[start]:
                continue
            if nxt == start and len(path) >= 2:
                result.append(list(path))
                if len(result) >= cap:
                    return
            elif nxt not in visited:
                visited.add(nxt)
                path.append(nxt)
                dfs(start, nxt, path, visited)
                path.pop()
                visited.discard(nxt)

    for s in node_list:
        if len(result) >= cap:
            break
        dfs(s, s, [s], {s})
    return result

def _cycle_detail(comp: "Compiler",
                  adj: Dict[str, List[Tuple[str, Dict[str, Any]]]],
                  cyc: List[str], gen: Optional[int]) -> Dict[str, Any]:
    by_thread_frames: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for fr in comp.frames.values():
        if gen is not None and fr["gen"] != gen:
            continue
        if _frame_active_at(fr, _snapshot_pos(comp, gen)):
            by_thread_frames[_thread_id(fr["gen"], fr["thread"])].append(fr)
    threads_info = []
    lock_cycle = []
    evidence = set()
    confidence = HIGH
    path = list(cyc) + [cyc[0]]
    transitions = []
    for i in range(len(cyc)):
        owner, blocked = path[i], path[i + 1]
        meta = None
        for t, m in adj.get(owner, []):
            if t == blocked:
                meta = m
                break
        lock_name = None
        if meta:
            he, we = meta["hold"], meta["wait"]
            lock_node = meta["lock_node"]
            lock_name = lock_node.split(":", 2)[2]
            lock_cycle.append(lock_name)
            if he:
                evidence.add(he["grant_event"])
                confidence = worse(confidence, he["confidence"])
            if we:
                evidence.add(we["request_event"])
                confidence = worse(confidence, we["confidence"])
            transitions.append({
                "owner": _label(owner), "blocked": _label(blocked),
                "lock": lock_name,
                "hold_event": he["grant_event"] if he else None,
                "wait_event": we["request_event"] if we else None,
                "wait_mode": we["mode"] if we else None,
                "hold_mode": he["mode"] if he else None,
                "confidence": worse(he["confidence"] if he else HIGH,
                                    we["confidence"] if we else HIGH),
            })
    for tnode in cyc:
        frames = sorted(by_thread_frames.get(tnode, []),
                        key=lambda f: (f["grant_seq"], f["fid"]))
        hold_stack = [{
            "lock": f["lock"], "mode": f["mode"], "type": f["type"],
            "grant_event": f["grant_event"], "grant_seq": f["grant_seq"],
            "depth": f["depth"], "confidence": f["confidence"],
        } for f in frames]
        wait_stack = []
        for w in comp.waits:
            if gen is not None and w["gen"] != gen:
                continue
            if _thread_id(w["gen"], w["thread"]) != tnode:
                continue
            if w["end_seq"] is None and w["kind"] == "lock":
                wait_stack.append({
                    "lock": w["lock"], "mode": w["mode"],
                    "request_event": w["request_event"],
                    "request_seq": w["request_seq"],
                    "confidence": w["confidence"],
                })
        for f in frames:
            evidence.add(f["grant_event"])
        for ws in wait_stack:
            evidence.add(ws["request_event"])
        threads_info.append({
            "thread": _label(tnode), "hold_stack": hold_stack,
            "wait_stack": wait_stack,
        })
    sig_locks = lock_cycle
    signature = " -> ".join(
        "%s@%s->%s" % (t["owner"], lk, t["blocked"])
        for t, lk in zip(transitions, sig_locks))
    return {
        "signature": signature,
        "thread_cycle": [_label(t) for t in cyc],
        "lock_cycle": lock_cycle,
        "transitions": transitions,
        "threads": threads_info,
        "evidence_event_ids": sorted(e for e in evidence if e is not None),
        "confidence": confidence,
    }


def _label(node: str) -> str:
    return node.split(":", 2)[2]


def _snapshot_pos(comp: "Compiler", gen: Optional[int]) -> int:
    seqs = [e["seq"] for e in comp.events if gen is None or e["gen"] == gen]
    return max(seqs, default=0)

# ---------------------------------------------------------------------------
# 编译入口 / 时间线
# ---------------------------------------------------------------------------

def compile_trace(events: List[Dict[str, Any]],
                  lock_types: Optional[Dict[str, str]] = None
                  ) -> Tuple["Compiler", List[Dict[str, Any]]]:
    evs = materialize(events, lock_types or {})
    _mark_disorder(evs)
    c = Compiler(lock_types or {})
    c.feed(evs)
    c.finalize()
    return c, evs


def timeline(comp: "Compiler", gen: Optional[int] = None
             ) -> Dict[str, Any]:
    events = [e for e in comp.events if gen is None or e["gen"] == gen]
    frames = [f for f in comp.frames.values()
              if gen is None or f["gen"] == gen]
    seqs = sorted({e["seq"] for e in events})
    lanes: Dict[str, List[Dict[str, Any]]] = {}
    thread_list = []
    for key, th in comp.threads.items():
        if gen is not None and th["gen"] != gen:
            continue
        node = _thread_id(th["gen"], th["name"])
        thread_list.append({"id": node, "name": th["name"],
                            "start": th["start"], "end": th["end"],
                            "implicit": th.get("implicit", False)})
        lane = []
        for fr in sorted([f for f in frames if f["gen"] == th["gen"]
                          and f["thread"] == th["name"]],
                         key=lambda f: (f["grant_seq"], f["fid"])):
            lane.append({
                "kind": "hold", "lock": fr["lock"], "mode": fr["mode"],
                "lock_type": fr["type"], "start_seq": fr["grant_seq"],
                "end_seq": fr["release_seq"], "fid": fr["fid"],
                "confidence": fr["confidence"],
                "abandoned": fr["abandoned"],
                "abandon_reason": fr["abandon_reason"],
                "grant_event": fr["grant_event"],
                "release_event": fr["release_event"],
                "suspensions": fr.get("suspensions", []),
            })
        for w in comp.waits:
            if w["gen"] != th["gen"] or w["thread"] != th["name"]:
                continue
            lane.append({
                "kind": w["kind"], "lock": w["lock"], "cond": w.get("cond"),
                "mode": w["mode"], "start_seq": w["request_seq"],
                "end_seq": w["end_seq"], "outcome": w["outcome"],
                "confidence": w["confidence"],
                "request_event": w["request_event"],
                "end_event": w["end_event"],
            })
        lane.sort(key=lambda x: (x["start_seq"], 0 if x["kind"] == "hold" else 1))
        lanes[node] = lane
    return {
        "gens": sorted({e["gen"] for e in comp.events}),
        "seqs": seqs,
        "min_seq": min(seqs, default=0),
        "max_seq": max(seqs, default=0),
        "threads": sorted(thread_list, key=lambda t: (t["start"] is None,
                                                      t["start"] or 0, t["name"])),
        "lanes": lanes,
        "events": [_event_public(e) for e in events],
    }


def _event_public(ev: Dict[str, Any]) -> Dict[str, Any]:
    keep = {k: v for k, v in ev.items() if not k.startswith("_")}
    return keep


def state_payload(comp: "Compiler", events: List[Dict[str, Any]],
                  branch_id: str, gen: Optional[int] = None
                  ) -> Dict[str, Any]:
    tl = timeline(comp, gen)
    anomalies = [a for a in comp.anomalies if gen is None or a.get("gen") == gen]
    return {
        "branch_id": branch_id,
        "timeline": tl,
        "anomalies": anomalies,
        "locks": [{"gen": lk["gen"], "name": lk["name"], "type": lk["type"]}
                  for (g, name), lk in comp.locks.items()
                  if gen is None or g == gen],
        "stats": {
            "events": len(events),
            "frames": len(comp.frames),
            "waits": len(comp.waits),
            "anomalies": len(anomalies),
            "threads": len(comp.threads),
        },
    }

# ---------------------------------------------------------------------------
# 流式增量：追加后仅重算受影响时间段
# ---------------------------------------------------------------------------

class IncrementalBuilder:
    """维护编译器与检查点。顺序追加只处理新事件；乱序插入回滚到
    插入点之前最近的检查点，再重放受影响区间。"""

    CHECKPOINT_EVERY = 128

    def __init__(self, lock_types: Optional[Dict[str, str]] = None):
        self.lock_types = dict(lock_types or {})
        self.compiler = Compiler(lock_types)
        self.processed: List[Dict[str, Any]] = []
        self._checkpoints: List[Tuple[int, Dict[str, Any]]] = []

    def _save_checkpoint(self) -> None:
        self._checkpoints.append((len(self.processed), self.compiler.checkpoint()))
        if len(self._checkpoints) > 8:
            self._checkpoints.pop(0)

    def _restore_to(self, index: int) -> None:
        cp = None
        for idx, snapshot in reversed(self._checkpoints):
            if idx <= index:
                cp = (idx, snapshot)
                break
        if cp is None:
            self.compiler = Compiler(self.lock_types)
            self.processed = []
            return
        idx, snapshot = cp
        self.compiler = Compiler.restore(snapshot)
        self.processed = self.compiler.events
        self._replay_from(idx, index)

    def _replay_from(self, start: int, end: int) -> None:
        # 回滚后的 processed 已是检查点事件；直接补喂到 end
        self.compiler.feed(self.processed[start:end])

    def append(self, new_events: List[Dict[str, Any]]
               ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if not new_events:
            return self.processed, {"affected_from": None, "affected_to": None,
                                    "in_order": True, "reprocessed": 0}
        norm = materialize(new_events, self.lock_types)
        combined = self.processed + norm
        combined.sort(key=lambda e: (e["gen"], e["seq"], e["arrival"], e["id"]))
        _mark_disorder(combined)
        first_new = min(e["seq"] for e in norm)
        affected_gens = {e["gen"] for e in norm}
        # 找受影响起点：同 gen 中 seq >= 最早新事件的首个已处理位置
        in_order = True
        reprocess_from = len(self.processed)
        for i, e in enumerate(self.processed):
            if e["gen"] in affected_gens and e["seq"] >= first_new:
                in_order = False
                reprocess_from = i
                break
        if in_order:
            tail = combined[len(self.processed):]
            self.compiler.feed(tail)
            self.processed = combined
            if len(self.processed) % self.CHECKPOINT_EVERY < len(tail):
                self._save_checkpoint()
            affected = {"affected_from": first_new,
                        "affected_to": max(e["seq"] for e in norm),
                        "in_order": True, "reprocessed": len(tail)}
        else:
            self._restore_to(reprocess_from)
            tail = combined[reprocess_from:]
            self.compiler.feed(tail)
            self.processed = combined
            self._save_checkpoint()
            affected_to = max(e["seq"] for e in tail)
            affected = {"affected_from":
                        self.processed[reprocess_from]["seq"]
                        if reprocess_from < len(self.processed) else first_new,
                        "affected_to": affected_to,
                        "in_order": False,
                        "reprocessed": len(tail)}
        return self.processed, affected

    def finalize(self) -> "Compiler":
        self.compiler.finalize()
        return self.compiler

# ---------------------------------------------------------------------------
# 最小化：保留配对与 condition 语义，仍复现同一环
# ---------------------------------------------------------------------------

def _pair_closure(events: List[Dict[str, Any]],
                  seed_ids: set) -> List[Dict[str, Any]]:
    """从证据事件出发，闭包加入每个线程的 request/granted/release/
    cond_wait/notify/resume/timeout 配对及线程起止，保持 condition 语义。"""
    by_id = {e["id"]: e for e in events}
    keep = set(seed_ids)
    changed = True
    while changed:
        changed = False
        for ev in events:
            if ev["id"] in keep:
                continue
            t = ev["type"]
            related = False
            if t in (THREAD_START, THREAD_END):
                gen, th = ev["gen"], ev["thread"]
                if any(by_id[i]["gen"] == gen and by_id[i].get("thread") == th
                       for i in keep if i in by_id):
                    related = True
            elif t == COND_NOTIFY:
                # notify 与同 cond 的 wait 配对
                if any(by_id[i].get("cond") == ev["cond"]
                       and by_id[i]["gen"] == ev["gen"]
                       for i in keep if i in by_id):
                    related = True
            else:
                # 同线程同锁事件保持配对链
                if any(by_id[i].get("thread") == ev.get("thread")
                       and by_id[i].get("lock") == ev.get("lock")
                       and by_id[i]["gen"] == ev["gen"]
                       for i in keep if i in by_id):
                    related = True
                if ev.get("cond") and any(
                        by_id[i].get("cond") == ev["cond"]
                        and by_id[i]["gen"] == ev["gen"]
                        for i in keep if i in by_id):
                    related = True
            if related:
                keep.add(ev["id"])
                changed = True
    return [e for e in events if e["id"] in keep]


def minimize_for_cycle(events: List[Dict[str, Any]],
                       lock_types: Dict[str, str],
                       signature: str,
                       gen: Optional[int] = None
                       ) -> Dict[str, Any]:
    materialized = materialize(events, lock_types)
    _mark_disorder(materialized)
    comp, _ = compile_trace(events, lock_types)
    snap = build_snapshot(comp, None, gen)
    target = next((c for c in snap["cycles"] if c["signature"] == signature), None)
    if target is None:
        raise EngineError("未找到指定环: %s" % signature)
    seed = set(target["evidence_event_ids"])
    candidate = _pair_closure(materialized, seed)
    candidate_ids = {e["id"] for e in candidate}

    def reproduces(ids: set) -> bool:
        subset = [e for e in materialized if e["id"] in ids]
        try:
            c2, _ = compile_trace(
                [{k: v for k, v in e.items() if not k.startswith("_")}
                 for e in subset], lock_types)
        except EngineError:
            return False
        s2 = build_snapshot(c2, None, gen)
        return any(c["signature"] == signature for c in s2["cycles"])

    # 贪心逐个移除非种子事件（保持闭包合法性）
    removable = [e for e in candidate if e["id"] not in seed]
    removable.sort(key=lambda e: (e["type"] in (THREAD_START, THREAD_END),
                                  -e["id"]))
    removed = []
    for ev in removable:
        trial = {i for i in candidate_ids if i != ev["id"]}
        trial_events = [e for e in materialized if e["id"] in trial]
        trial_closed = _pair_closure(trial_events, seed)
        trial_ids = {e["id"] for e in trial_closed}
        if reproduces(trial_ids):
            candidate_ids = trial_ids
            removed.append(ev["id"])
        # 单个事件不可删不影响后续事件继续尝试
    final_events = [e for e in materialized if e["id"] in candidate_ids]
    final_public = [_event_public(e) for e in final_events]
    c3, _ = compile_trace(final_public, lock_types)
    s3 = build_snapshot(c3, None, gen)
    cycle3 = next((c for c in s3["cycles"] if c["signature"] == signature), None)
    return {
        "signature": signature,
        "original_count": len(materialized),
        "minimal_count": len(final_public),
        "removed_ids": removed,
        "events": final_public,
        "cycle": cycle3,
        "reproduces": cycle3 is not None,
        "seq_range": [min((e["seq"] for e in final_public), default=None),
                      max((e["seq"] for e in final_public), default=None)],
    }
