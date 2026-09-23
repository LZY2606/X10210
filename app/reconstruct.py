"""锁序重放器。

维护带置信状态的候选持有图/等待图：
- 绝不凭空补 release；缺事件时锁在代次结束仍标 ``missing_release``。
- 时间戳乱序（晚到）事件照常参与重放，相关边打 ``reorder`` 标记。
- condition wait 期间释放锁，``cond_wait_end`` 之后重新竞争（需 acquire_ok 才真正持有）。
- 超时 / 取消 / 线程终止撤销等待。
- 相同锁地址按 (gen, addr) 区分，跨代次不可能构成环。
"""

from __future__ import annotations

import copy
from typing import Any, Optional

from .events import Event, ordered_for_replay
from . import cycles as cyc


def _span(kind: str, thread: str, gen: int, start_event: int, start_ts: float,
          stack: list[str], **extra) -> dict[str, Any]:
    s = {
        "kind": kind,
        "thread": thread,
        "gen": gen,
        "start_event": start_event,
        "start_ts": start_ts,
        "start_stack": stack,
        "end_event": None,
        "end_ts": None,
    }
    s.update(extra)
    return s


class Replayer:
    def __init__(self, events: list[Event], lock_types: Optional[dict[str, str]] = None,
                 with_frames: bool = True):
        self.events = events
        self.ordered = ordered_for_replay(events)
        self.by_id = {e.id: e for e in events}
        self.lock_types = dict(lock_types or {})  # addr -> type（不分代，地址即静态类型）
        self.with_frames = with_frames

        self.threads: dict[str, dict] = {}
        self.locks: dict[str, dict] = {}
        self.cvs: dict[str, dict] = {}
        self.global_wait: dict[str, str] = {}  # thread -> wait_span_id
        self.cond_waiting: dict[str, str] = {}  # thread -> cv_key（等待 CV，未竞争锁）
        self.spans: list[dict[str, Any]] = []
        self.issues: list[dict[str, Any]] = []
        self.incidents: list[dict[str, Any]] = []
        self.active_incidents: dict[str, dict] = {}
        self.frames: list[dict[str, Any]] = []
        self.gen_max_arrival: dict[int, int] = {}
        self.gen_started: set[int] = set()
        self.gen_finalized: set[int] = set()
        self.span_seq = 0

    # ---- 基础工具 ----
    def _add_span(self, span: dict[str, Any]) -> str:
        sid = f"s{self.span_seq}"
        self.span_seq += 1
        span = dict(span)
        span["id"] = sid
        self.spans.append(span)
        return sid

    def get_span(self, sid: str) -> dict[str, Any]:
        return self.spans[int(sid[1:])]

    def _close_span(self, sid: Optional[str], event_id: int, ts: float) -> None:
        if sid is None:
            return
        sp = self.get_span(sid)
        sp["end_event"] = event_id
        sp["end_ts"] = ts

    def _issue(self, kind: str, ev: Event, message: str, **extra) -> None:
        item = {"kind": kind, "event_id": ev.id, "ts": ev.ts, "gen": ev.gen,
                "thread": ev.thread, "lock": ev.lock, "message": message, "confidence": "low"}
        item.update(extra)
        self.issues.append(item)

    def _lock_type(self, lock_key: str, addr: Optional[str]) -> str:
        if addr and addr in self.lock_types:
            return self.lock_types[addr]
        return self.locks.get(lock_key, {}).get("type", "mutex")

    def _ensure_lock(self, ev: Event) -> dict[str, Any]:
        key = ev.lock_key
        lock = self.locks.get(key)
        if lock is None:
            lock = {
                "key": key, "addr": ev.lock, "gen": ev.gen,
                "type": self.lock_types.get(ev.lock or "", "mutex"),
                "owners": {}, "readers": {}, "waiters": [],
                "history": [], "type_inferred": ev.lock not in self.lock_types,
            }
            self.locks[key] = lock
        return lock

    def _ensure_thread(self, ev: Event, at_start: bool = False) -> dict[str, Any]:
        tid = ev.thread or "?"
        th = self.threads.get(tid)
        if th is None:
            th = {"id": tid, "status": "running" if at_start else "unknown",
                  "started_ev": ev.id if at_start else None, "ended_ev": None,
                  "first_seen": ev.id}
            self.threads[tid] = th
        return th

    def _owner_entry(self, lock: dict[str, Any], tid: str) -> Optional[dict[str, Any]]:
        if tid in lock["owners"]:
            return lock["owners"][tid]
        reader = lock["readers"].get(tid)
        if reader:
            return {"mode": "R", "count": reader["count"], "entry": reader["entry"],
                    "events": reader["events"], "span_ids": reader["span_ids"],
                    "stacks": reader["stacks"], "read_snapshot": reader.get("read_snapshot")}
        return None

    # ---- 主入口 ----
    def run(self) -> dict[str, Any]:
        for ev in self.ordered:
            self._step(ev)
        self._finalize()
        return self.result()

    def _step(self, ev: Event) -> None:
        if ev.gen not in self.gen_started and ev.kind not in ("gen_start", "gen_end"):
            self._begin_gen(ev.gen, ev)
        reorder = self._note_order(ev)
        handler = {
            "gen_start": self._h_gen_start,
            "gen_end": self._h_gen_end,
            "thread_start": self._h_thread_start,
            "thread_end": self._h_thread_end,
            "acquire_req": self._h_acquire_req,
            "acquire_ok": self._h_acquire_ok,
            "acquire_timeout": self._h_acquire_cancel,
            "acquire_cancel": self._h_acquire_cancel,
            "release": self._h_release,
            "cond_wait": self._h_cond_wait,
            "cond_wait_end": self._h_cond_wait_end,
            "cond_notify": self._h_cond_notify,
            "cond_notify_all": self._h_cond_notify,
        }.get(ev.kind)
        if handler is None:
            self._issue("unknown_event", ev, f"未知事件类型 {ev.kind}")
            self._snapshot(ev, reorder)
            return
        handler(ev, reorder)
        self._snapshot(ev, reorder)
        self._detect_incidents(ev)

    def _note_order(self, ev: Event) -> list[str]:
        prev_max = self.gen_max_arrival.get(ev.gen)
        flags: list[str] = []
        if prev_max is not None and ev.arrival_seq < prev_max:
            flags.append("reorder")
        self.gen_max_arrival[ev.gen] = max(ev.arrival_seq, prev_max or 0)
        return flags

    # ---- 代次 / 线程生命周期 ----
    def _begin_gen(self, gen: int, ev: Event) -> None:
        self.gen_started.add(gen)

    def _h_gen_start(self, ev: Event, flags: list[str]) -> None:
        if ev.gen in self.gen_started:
            self._issue("dup_gen_start", ev, f"代次 {ev.gen} 重复开始")
        self._begin_gen(ev.gen, ev)

    def _h_gen_end(self, ev: Event, flags: list[str]) -> None:
        self._finalize_gen(ev.gen, ev)

    def _h_thread_start(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        th = self.threads.get(tid)
        if th and th.get("status") != "ended":
            self._issue("dup_thread_start", ev, f"线程 {tid} 重复开始", confidence="low")
        th = self._ensure_thread(ev, at_start=True)
        th["status"] = "running"
        th["started_ev"] = ev.id

    def _h_thread_end(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        th = self._ensure_thread(ev)
        th["status"] = "ended"
        th["ended_ev"] = ev.id
        wait_sid = self.global_wait.pop(tid, None)
        if wait_sid is not None:
            self._close_span(wait_sid, ev.id, ev.ts)
            sp = self.get_span(wait_sid)
            lk = sp.get("lock_key")
            if lk in self.locks:
                self.locks[lk]["waiters"] = [
                    w for w in self.locks[lk]["waiters"] if w["thread"] != tid
                ]
        self.cond_waiting.pop(tid, None)
        for key, lock in self.locks.items():
            if lock["gen"] != ev.gen:
                continue
            entry = self._owner_entry(lock, tid)
            if entry is not None:
                self._issue(
                    "thread_exit_holding", ev,
                    f"线程 {tid} 终止时仍持有锁 {lock['addr']}，不自动补 release",
                    lock_key=key, confidence="low",
                )

    # ---- 获取锁 ----
    def _blocks(self, lock: dict[str, Any], tid: str, mode: str) -> list[tuple[str, dict]]:
        ltype = lock["type"]
        out: list[tuple[str, dict]] = []
        for holder_tid, holder in lock["owners"].items():
            if holder_tid == tid:
                if ltype == "mutex":
                    out.append((holder_tid, holder))
                # recursive / rwlock 自己 W 持有 -> 重入/升级，不构成别人阻塞
                continue
            out.append((holder_tid, holder))
        if mode == "W":
            for reader_tid, reader in lock["readers"].items():
                if reader_tid == tid:
                    continue
                out.append((reader_tid, reader))
        return out

    def _h_acquire_req(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        self._ensure_thread(ev)
        lock = self._ensure_lock(ev)
        mode = ev.mode or ("R" if lock["type"] == "rwlock" else "W")
        existing = next((w for w in lock["waiters"] if w["thread"] == tid), None)
        if existing is not None:
            self._issue("dup_acquire_req", ev, f"线程 {tid} 在等待锁 {ev.lock} 时再次请求",
                        lock_key=lock["key"], confidence="low")
        cond = self.cond_waiting.pop(tid, None)
        waiter = {
            "thread": tid,
            "mode": mode,
            "req_id": ev.id,
            "req_ts": ev.ts,
            "since": ev.ts,
            "stack": ev.stack,
            "timeout_ms": ev.timeout_ms,
            "from_cond": cond,
            "edge_confidence": {"flags": list(flags)},
        }
        lock["waiters"].append(waiter)
        sid = self._add_span(_span(
            "wait", tid, ev.gen, ev.id, ev.ts, ev.stack,
            lock_key=lock["key"], lock_addr=ev.lock, mode=mode,
            from_cond=cond, flags=list(flags),
        ))
        self.global_wait[tid] = sid

    def _h_acquire_ok(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        self._ensure_thread(ev)
        lock = self._ensure_lock(ev)
        waiter = next((w for w in lock["waiters"] if w["thread"] == tid), None)
        mode = ev.mode or (waiter["mode"] if waiter else ("R" if lock["type"] == "rwlock" else "W"))
        conflict = self._blocks(lock, tid, mode)

        wait_sid = self.global_wait.pop(tid, None)
        from_cond = None
        if waiter is not None:
            from_cond = waiter.get("from_cond")
            lock["waiters"] = [w for w in lock["waiters"] if w["thread"] != tid]
            if wait_sid is None:
                wait_sid = self.global_wait.pop(tid, None)
        if wait_sid is not None:
            self._close_span(wait_sid, ev.id, ev.ts)

        if conflict:
            self._issue(
                "acquired_while_held", ev,
                f"线程 {tid} 获取锁 {ev.lock} 时仍被其它线程持有，"
                "采集可能缺 release/ack 事件；按已持有处理并降低置信",
                lock_key=lock["key"], confidence="low",
            )

        sid = self._add_span(_span(
            "hold", tid, ev.gen, ev.id, ev.ts, ev.stack,
            lock_key=lock["key"], lock_addr=ev.lock, mode=mode,
            from_cond=from_cond,
        ))
        self._grant(lock, tid, mode, ev, sid, flags, upgrade=waiter is not None and
                    waiter.get("mode") == "W" and tid in lock["readers"] and
                    lock["type"] == "rwlock")

    def _grant(self, lock: dict[str, Any], tid: str, mode: str, ev: Event,
               sid: str, flags: list[str], upgrade: bool = False) -> None:
        if lock["type"] == "rwlock" and mode == "R" and not upgrade:
            reader = lock["readers"].get(tid)
            if reader:
                reader["count"] += 1
                reader["events"].append(ev.id)
                reader["span_ids"].append(sid)
                reader["stacks"].append(ev.stack)
            else:
                lock["readers"][tid] = {
                    "mode": "R", "count": 1, "entry": ev.id,
                    "events": [ev.id], "span_ids": [sid], "stacks": [ev.stack],
                    "gen": ev.gen,
                }
            return
        owner = lock["owners"].get(tid)
        if owner:
            owner["count"] += 1
            owner["events"].append(ev.id)
            owner["span_ids"].append(sid)
            owner["stacks"].append(ev.stack)
            if upgrade:
                owner["read_snapshot"] = lock["readers"][tid]["count"]
                owner["upgraded"] = True
            if flags:
                owner.setdefault("flags", [])
                owner["flags"] = sorted(set(owner.get("flags", []) + flags))
        else:
            lock["owners"][tid] = {
                "mode": mode, "count": 1, "entry": ev.id,
                "events": [ev.id], "span_ids": [sid], "stacks": [ev.stack],
                "gen": ev.gen, "upgraded": upgrade,
                "read_snapshot": lock["readers"][tid]["count"] if tid in lock["readers"] else 0,
                "flags": list(flags),
            }
        if upgrade or mode == "W":
            lock["readers"].pop(tid, None)
        if lock["type"] == "mutex" and (lock["owners"] or lock["readers"]):
            inferred = None
            if len(lock["owners"][tid]["events"]) > 1 or upgrade:
                inferred = "recursive"
            if inferred and lock.get("type_inferred"):
                lock["type"] = inferred

    def _h_acquire_cancel(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        lock = self._ensure_lock(ev)
        waiter = next((w for w in lock["waiters"] if w["thread"] == tid), None)
        if waiter is None:
            self._issue("cancel_without_wait", ev,
                        f"线程 {tid} 取消等待锁 {ev.lock}，但未找到等待请求",
                        confidence="low")
        lock["waiters"] = [w for w in lock["waiters"] if w["thread"] != tid]
        wait_sid = self.global_wait.pop(tid, None)
        if wait_sid is not None:
            self.get_span(wait_sid)["cancel_kind"] = ev.kind
            self._close_span(wait_sid, ev.id, ev.ts)
        timeout_chain = waiter is not None and self._in_wait_chain(lock, waiter)
        if timeout_chain:
            self.incidents.append({
                "id": f"nm-{ev.id}", "kind": "near_miss_timeout",
                "ts": ev.ts, "gen": ev.gen,
                "title": f"线程 {tid} 超时退出一条竞争链（近失）",
                "waiter": tid, "lock_key": lock["key"], "req_event": waiter["req_id"],
                "cancel_event": ev.id, "resolved_event": ev.id,
                "confidence": "high" if not flags else "low",
            })

    def _in_wait_chain(self, from_lock: dict[str, Any], waiter: dict) -> bool:
        # 该等待者等待的锁被某个线程持有，而该线程自己也在等待别的锁 => 链长 >= 2
        blockers = [h for h, _ in self._blocks(from_lock, waiter["thread"], waiter["mode"])]
        for holder_tid in blockers:
            if holder_tid in self.global_wait or holder_tid in self.cond_waiting:
                return True
        return False

    # ---- 释放 ----
    def _h_release(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        key = ev.lock_key
        lock = self.locks.get(key)
        entry = self._owner_entry(lock or {}, tid) if lock else None
        if lock is None or entry is None:
            self._issue("release_without_hold", ev,
                        f"线程 {tid} 释放未持有的锁 {ev.lock}（可能缺 acquire 或跨代次）",
                        confidence="low")
            return

        was_owner = tid in lock["owners"]
        if was_owner:
            owner = lock["owners"][tid]
            owner["count"] -= 1
            if owner["count"] > 0:
                return
            # 最终释放；若由 R 升级而来，按快照补齐 R 层（在状态里不再恢复持有）
            del lock["owners"][tid]
        else:
            reader = lock["readers"][tid]
            reader["count"] -= 1
            if reader["count"] > 0:
                return
            del lock["readers"][tid]

        for sid in entry["span_ids"]:
            self._close_span(sid, ev.id, ev.ts)

    # ---- 条件变量 ----
    def _h_cond_wait(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        self._ensure_thread(ev)
        lock = self._ensure_lock(ev) if ev.lock else None
        if lock is None or tid not in lock["owners"]:
            self._issue("cond_wait_without_lock", ev,
                        f"线程 {tid} 在未持有锁 {ev.lock} 时进入条件等待",
                        confidence="low")
        cv = self.cvs.setdefault(ev.cv_key, {"key": ev.cv_key, "addr": ev.cv,
                                             "gen": ev.gen, "waiters": []})
        if ev.lock:
            owner = lock["owners"].get(tid) if lock else None
            released_spans: list[str] = []
            if owner:
                released_spans = list(owner.get("span_ids", []))
                mode = owner.get("mode", "W")
                saved = {
                    "mode": mode, "span_ids": released_spans,
                    "read_snapshot": owner.get("read_snapshot", 0),
                    "upgraded": owner.get("upgraded", False),
                }
                del lock["owners"][tid]
            else:
                saved = {"mode": "W", "span_ids": [], "read_snapshot": 0, "upgraded": False}
            for sid in released_spans:
                self._close_span(sid, ev.id, ev.ts)
        else:
            saved = {"mode": "W", "span_ids": [], "read_snapshot": 0, "upgraded": False}

        self.cond_waiting[tid] = ev.cv_key
        sid = self._add_span(_span(
            "cond", tid, ev.gen, ev.id, ev.ts, ev.stack,
            cv_key=ev.cv_key, cv_addr=ev.cv, lock_key=ev.lock_key if ev.lock else None,
            lock_addr=ev.lock, saved=saved,
        ))
        cv["waiters"].append({
            "thread": tid, "ev_id": ev.id, "ts": ev.ts, "stack": ev.stack,
            "lock_key": ev.lock_key if ev.lock else None, "span_id": sid,
        })

    def _h_cond_wait_end(self, ev: Event, flags: list[str]) -> None:
        tid = ev.thread or "?"
        cv_key = self.cond_waiting.get(tid)
        cv = None
        rec = None
        if cv_key is not None:
            cv = self.cvs.get(cv_key)
        if cv is not None:
            rec = next((w for w in cv["waiters"] if w["thread"] == tid), None)
        if rec is None:
            self._issue("cond_end_without_wait", ev,
                        f"线程 {tid} 的 cond_wait_end 找不到对应等待", confidence="low")
            return
        cv["waiters"] = [w for w in cv["waiters"] if w["thread"] != tid]
        self.cond_waiting.pop(tid, None)
        self._close_span(rec["span_id"], ev.id, ev.ts)
        # 唤醒后重新竞争锁：必须有后续 acquire_ok 才真正持有（不凭空授予）
        if ev.lock:
            lock = self._ensure_lock(ev)
            mode = self.get_span(rec["span_id"]).get("saved", {}).get("mode", "W")
            waiter = {
                "thread": tid, "mode": mode, "req_id": ev.id, "req_ts": ev.ts,
                "since": ev.ts, "stack": ev.stack, "timeout_ms": None,
                "from_cond": cv["addr"], "edge_confidence": {"flags": list(flags)},
            }
            lock["waiters"].append(waiter)
            sid = self._add_span(_span(
                "wait", tid, ev.gen, ev.id, ev.ts, ev.stack,
                lock_key=lock["key"], "lock_addr": ev.lock, mode=mode,
                from_cond=cv["addr"], flags=list(flags),
            ))
            self.global_wait[tid] = sid

    def _h_cond_notify(self, ev: Event, flags: list[str]) -> None:
        cv = self.cvs.get(ev.cv_key)
        if cv is None or not cv["waiters"]:
            self._issue("notify_without_waiter", ev,
                        f"条件变量 {ev.cv} 通知时无等待者", confidence="low")
            return
        n = len(cv["waiters"]) if ev.kind == "cond_notify_all" else 1
        moved = cv["waiters"][:n]
        cv["waiters"] = cv["waiters"][n:]
        for rec in moved:
            tid = rec["thread"]
            self.cond_waiting.pop(tid, None)
            self._close_span(rec["span_id"], ev.id, ev.ts)
            lock_key = rec.get("lock_key")
            if lock_key and lock_key in self.locks:
                lock = self.locks[lock_key]
                saved = self.get_span(rec["span_id"]).get("saved", {})
                mode = saved.get("mode", "W")
                waiter = {
                    "thread": tid, "mode": mode, "req_id": ev.id, "req_ts": ev.ts,
                    "since": ev.ts, "stack": rec.get("stack", []), "timeout_ms": None,
                    "from_cond": cv["addr"], "notified_by": ev.id,
                    "edge_confidence": {"flags": list(flags)},
                }
                lock["waiters"].append(waiter)
                sid = self._add_span(_span(
                    "wait", tid, ev.gen, ev.id, ev.ts, rec.get("stack", []),
                    lock_key=lock_key, lock_addr=lock["addr"], mode=mode,
                    from_cond=cv["addr"], notified_by=ev.id, flags=list(flags),
                ))
                self.global_wait[tid] = sid

    # ---- 代次收尾 ----
    def _finalize_gen(self, gen: int, ev: Optional[Event] = None) -> None:
        if gen in self.gen_finalized:
            return
        self.gen_finalized.add(gen)
        ts = ev.ts if ev else max((e.ts for e in self.ordered if e.gen == gen), default=0.0)
        ev_id = ev.id if ev else None
        for key, lock in self.locks.items():
            if lock["gen"] != gen:
                continue
            if lock["owners"] or lock["readers"]:
                addrs = ",".join(sorted(set(list(lock["owners"]) + list(lock["readers"]))))
                self.issues.append({
                    "kind": "missing_release", "event_id": ev_id, "ts": ts, "gen": gen,
                    "thread": addrs, "lock": lock["addr"],
                    "message": f"代次结束时锁 {lock['addr']} 仍被 {addrs} 持有，"
                               "缺 release 事件，不凭空补放",
                    "confidence": "low", "lock_key": key,
                })
            for w in lock["waiters"]:
                sid = self.global_wait.get(w["thread"])
                if sid is not None:
                    self.get_span(sid)["abandoned"] = True
                    self._close_span(sid, ev_id, ts)
                self.issues.append({
                    "kind": "abandoned_wait", "event_id": ev_id, "ts": ts, "gen": gen,
                    "thread": w["thread"], "lock": lock["addr"],
                    "message": f"代次结束时线程 {w['thread']} 仍在等待锁 {lock['addr']}",
                    "confidence": "low", "lock_key": key,
                })
            lock["waiters"] = []
        for tid, cv_key in list(self.cond_waiting.items()):
            if not cv_key.startswith(f"{gen}:"):
                continue
            cv = self.cvs.get(cv_key)
            if cv:
                rec = next((w for w in cv["waiters"] if w["thread"] == tid), None)
                if rec:
                    self.get_span(rec["span_id"])["abandoned"] = True
                    self._close_span(rec["span_id"], ev_id, ts)
                cv["waiters"] = [w for w in cv["waiters"] if w["thread"] != tid]
            self.cond_waiting.pop(tid, None)
        self.global_wait = {t: s for t, s in self.global_wait.items()
                            if self.get_span(s)["gen"] != gen}

    def _finalize(self) -> None:
        gens = sorted({e.gen for e in self.ordered})
        for gen in gens:
            self._finalize_gen(gen)
        for inc in self.active_incidents.values():
            inc["status"] = "open"
            self.incidents.append(inc)
        self.active_incidents.clear()
        self.issues.sort(key=lambda i: (i["ts"], i["event_id"] or 0))

    # ---- 快照与事件检测 ----
    def _snapshot(self, ev: Event, flags: list[str]) -> None:
        if not self.with_frames:
            return
        edges = cyc.build_wait_edges(self.locks)
        cycs = cyc.find_cycles(edges)
        frame = {
            "index": len(self.frames),
            "event_id": ev.id,
            "kind": ev.kind,
            "ts": ev.ts,
            "gen": ev.gen,
            "thread": ev.thread,
            "flags": list(flags),
            "state": self._state_snapshot(),
            "wait_edges": edges,
            "cycles": cycs,
        }
        self.frames.append(frame)

    def _state_snapshot(self) -> dict[str, Any]:
        locks = {}
        for key, lock in self.locks.items():
            locks[key] = {
                "key": key, "addr": lock["addr"], "gen": lock["gen"],
                "type": lock["type"], "type_inferred": lock.get("type_inferred", False),
                "owners": copy.deepcopy(lock["owners"]),
                "readers": copy.deepcopy(lock["readers"]),
                "waiters": copy.deepcopy(lock["waiters"]),
            }
        return {
            "threads": copy.deepcopy(self.threads),
            "locks": locks,
            "cvs": {k: {"key": v["key"], "addr": v["addr"], "gen": v["gen"],
                        "waiters": [dict(w) for w in v["waiters"]]}
                    for k, v in self.cvs.items()},
            "cond_waiting": dict(self.cond_waiting),
            "issues": copy.deepcopy(self.issues),
        }

    def _detect_incidents(self, ev: Event) -> None:
        frame = self.frames[-1] if self.frames else None
        current = {}
        if frame is not None:
            for c in frame["cycles"]:
                current[c["signature"]] = c

        # 消失的环 => 死锁解除；若解除动作是 release 则为近失，线程终止则另标
        for sig, inc in list(self.active_incidents.items()):
            if sig not in current:
                inc["status"] = "resolved"
                inc["resolved_event"] = ev.id
                inc["resolved_ts"] = ev.ts
                if ev.kind == "release":
                    inc["kind"] = "near_miss_release"
                    inc["title"] = "等待环被一次 release 打破（近失）"
                elif ev.kind == "thread_end":
                    inc["kind"] = "deadlock_thread_exit"
                    inc["title"] = "等待环因线程终止而消失"
                else:
                    inc["kind"] = "deadlock_resolved"
                    inc["title"] = "等待环解除"
                self.incidents.append(inc)
                del self.active_incidents[sig]

        # 新出现的环 => 记录参与线程、锁、持有栈与等待栈
        for sig, c in current.items():
            if sig not in self.active_incidents:
                self.active_incidents[sig] = {
                    "id": f"dl-{ev.id}-{abs(hash(sig)) % 100000}",
                    "kind": "deadlock",
                    "status": "active",
                    "ts": ev.ts, "gen": ev.gen,
                    "frame_index": frame["index"] if frame else None,
                    "title": f"检测到等待环：{sig}",
                    "signature": sig,
                    "threads": c["threads"],
                    "lock_keys": c["lock_keys"],
                    "cycle": c,
                    "first_event": ev.id,
                }

    def result(self) -> dict[str, Any]:
        # 当前仍存活的活动事件也对外提供（finalize 前的增量调用场景）
        active = list(self.active_incidents.values())
        return {
            "frames": self.frames,
            "spans": self.spans,
            "issues": self.issues,
            "incidents": self.incidents + active,
            "threads": self.threads,
            "locks": self.locks,
            "cvs": self.cvs,
        }


def reconstruct(events: list[Event], lock_types: Optional[dict[str, str]] = None,
                with_frames: bool = True) -> dict[str, Any]:
    return Replayer(events, lock_types=lock_types, with_frames=with_frames).run()
