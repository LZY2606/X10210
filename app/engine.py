from __future__ import annotations

from bisect import bisect_right
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

VALID_KINDS = {
    "thread_start", "thread_end",
    "acquire_request", "acquire_success", "release",
    "acquire_timeout", "acquire_cancel",
    "condition_wait_start", "condition_wait_end",
    "condition_reacquired", "condition_notify",
    "condition_timeout", "condition_cancel",
}
VALID_MODES = {"", "read", "write", "shared", "exclusive", "upgrade"}
VALID_TYPES = {"recursive", "mutex", "rwlock"}
CONFIDENCE = ("high", "likely", "conflicted", "incomplete")


def shared_mode(mode: str) -> bool:
    return mode in {"read", "shared"}


def exclusive_mode(mode: str) -> bool:
    return mode in {"write", "exclusive", "upgrade"}


def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    kind = str(raw.get("kind", "")).strip()
    if kind not in VALID_KINDS:
        raise ValueError(f"unsupported event kind: {kind}")
    mode = str(raw.get("mode", "") or "").strip()
    if mode not in VALID_MODES:
        raise ValueError(f"unsupported lock mode: {mode}")
    thread_id = str(raw.get("thread_id", raw.get("thread", "")))
    lock_id = str(raw.get("lock_id", raw.get("lock", "")) or "")
    condition_id = str(raw.get("condition_id", raw.get("condition", "")) or "")
    request_id = str(raw.get("request_id", "") or "")
    if kind.startswith("acquire") and (not thread_id or not lock_id):
        raise ValueError("acquire events require thread_id and lock_id")
    if kind == "release" and (not thread_id or not lock_id):
        raise ValueError("release events require thread_id and lock_id")
    if kind.startswith("condition_wait") and (not thread_id or not lock_id):
        raise ValueError("condition wait events require thread_id and lock_id")
    return {
        "id": int(raw["id"]) if raw.get("id") is not None else None,
        "kind": kind,
        "ts": int(raw.get("ts", raw.get("timestamp", 0)) or 0),
        "seq": int(raw.get("seq", raw.get("arrival_seq", 0)) or 0),
        "gen": int(raw.get("gen", raw.get("generation", 0)) or 0),
        "thread_id": thread_id,
        "lock_id": lock_id,
        "condition_id": condition_id,
        "request_id": request_id,
        "mode": mode,
        "stack": list(raw.get("stack") or []),
        "payload": dict(raw.get("payload") or {}),
    }


@dataclass
class Event:
    id: int
    kind: str
    ts: int
    seq: int
    gen: int
    thread_id: str
    lock_id: str = ""
    condition_id: str = ""
    request_id: str = ""
    mode: str = ""
    stack: List[str] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "Event":
        return cls(**normalize_event(row))

    def public(self) -> Dict[str, Any]:
        return {
            "id": self.id, "kind": self.kind, "ts": self.ts, "seq": self.seq,
            "gen": self.gen, "thread_id": self.thread_id,
            "lock_id": self.lock_id, "condition_id": self.condition_id,
            "request_id": self.request_id, "mode": self.mode,
            "stack": self.stack, "payload": self.payload,
        }


@dataclass
class Issue:
    level: str
    code: str
    message: str
    event_ids: List[int] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level, "code": self.code, "message": self.message,
            "event_ids": self.event_ids,
        }


@dataclass
class Acquisition:
    gen: int
    lock_id: str
    thread_id: str
    mode: str
    count: int
    request_id: str
    success_event_id: int
    stack: List[str]
    suspended: int = 0
    wait_request_id: str = ""
    notified: bool = False
    wait_event_id: int = 0
    notify_event_id: int = 0

    def as_dict(self) -> Dict[str, Any]:
        return {
            "gen": self.gen, "lock_id": self.lock_id, "mode": self.mode,
            "count": self.count, "request_id": self.request_id,
            "success_event_id": self.success_event_id,
            "stack": list(self.stack), "suspended": self.suspended,
            "notified": self.notified,
            "wait_request_id": self.wait_request_id,
            "wait_event_id": self.wait_event_id,
            "notify_event_id": self.notify_event_id,
        }


@dataclass
class Request:
    event_id: int
    gen: int
    thread_id: str
    lock_id: str
    mode: str
    request_id: str
    stack: List[str]
    active: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class CondWait:
    event_id: int
    gen: int
    thread_id: str
    lock_id: str
    condition_id: str
    mode: str
    request_id: str
    stack: List[str]
    notified: bool = False
    notify_event_id: int = 0
    active: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return self.__dict__.copy()


@dataclass
class State:
    declarations: Dict[Tuple[int, str], str]
    events: List[Event]
    order: str = "timestamp"
    threads: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    holds: Dict[Tuple[int, str], Dict[str, Acquisition]] = field(default_factory=dict)
    pending: Dict[Tuple[int, str], List[Request]] = field(default_factory=dict)
    requests_by_id: Dict[str, Request] = field(default_factory=dict)
    condwaits: Dict[str, CondWait] = field(default_factory=dict)
    inferred: Dict[Tuple[int, str], str] = field(default_factory=dict)
    issues: List[Issue] = field(default_factory=list)
    seen_starts: set = field(default_factory=set)
    seen_success: set = field(default_factory=set)
    last_ts: Optional[int] = None
    out_of_order: bool = False
    confidence: str = "high"

    def clone(self) -> "State":
        return deepcopy(self)

    def key(self, event: Event) -> Tuple[int, str]:
        return (event.gen, event.lock_id)

    def lock_type(self, gen: int, lock_id: str) -> str:
        key = (gen, lock_id)
        if key in self.declarations:
            return self.declarations[key]
        return self.inferred.get(key, "recursive")

    def note(self, level: str, code: str, message: str, event_ids: Iterable[int] = ()):
        self.issues.append(Issue(level, code, message, list(event_ids)))
        ranks = {"info": 0, "warning": 1, "error": 2}
        if level == "error":
            self.confidence = "conflicted"
        elif level == "warning" and self.confidence == "high":
            self.confidence = "likely"

    def active_holders(self, key: Tuple[int, str]) -> List[Acquisition]:
        return [a for a in self.holds.get(key, {}).values() if a.count > a.suspended]

    def own_active(self, key: Tuple[int, str], thread_id: str) -> Optional[Acquisition]:
        acquisition = self.holds.get(key, {}).get(thread_id)
        if acquisition and acquisition.count > acquisition.suspended:
            return acquisition
        return None

    def can_grant(self, key: Tuple[int, str], thread_id: str, mode: str, lock_type: str) -> bool:
        holders = self.active_holders(key)
        if not holders:
            return True
        if all(h.thread_id if False else False for h in holders):
            return False
        same = [h for h in holders if getattr(h, "thread_id", "") == thread_id]
        others = [h for h in holders if getattr(h, "thread_id", "") != thread_id]
        if same and not others:
            if lock_type == "mutex":
                return False
            return True
        if others:
            if lock_type == "rwlock" and shared_mode(mode):
                return all(shared_mode(h.mode) for h in others) and (
                    not same or all(shared_mode(h.mode) for h in same)
                )
            return False
        return True

    def add_acquisition(self, event: Event, request: Optional[Request], rid: str) -> Acquisition:
        key = self.key(event)
        holders = self.holds.setdefault(key, {})
        stack = list(event.stack or (request.stack if request else []))
        mode = event.mode or (request.mode if request else "")
        rid = rid or (request.request_id if request else "")
        acquisition = holders.get(event.thread_id)
        if acquisition:
            acquisition.count += 1
            if exclusive_mode(mode):
                acquisition.mode = "write"
            elif not acquisition.mode and mode:
                acquisition.mode = mode
            acquisition.stack = stack or acquisition.stack
            acquisition.request_id = rid or acquisition.request_id
            acquisition.success_event_id = event.id
            return acquisition
        acquisition = Acquisition(
            gen=event.gen, lock_id=event.lock_id, thread_id=event.thread_id, mode=mode, count=1,
            request_id=rid, success_event_id=event.id, stack=stack,
        )
        holders[event.thread_id] = acquisition
        return acquisition

    def grant_request(self, event: Event, request: Request):
        key = self.key(event)
        lock_type = self.lock_type(event.gen, event.lock_id)
        if event.mode:
            mode = event.mode
        else:
            mode = request.mode
        if lock_type != "mutex":
            own = self.own_active(key, event.thread_id)
            if own and exclusive_mode(mode):
                own.mode = "write"
                own.success_event_id = event.id
                if event.stack:
                    own.stack = list(event.stack)
            elif own and lock_type == "recursive":
                own.count += 1
                own.success_event_id = event.id
                if event.stack:
                    own.stack = list(event.stack)
            elif own and lock_type == "rwlock" and shared_mode(mode):
                own.count += 1
                own.success_event_id = event.id
                if event.stack:
                    own.stack = list(event.stack)
            else:
                self.add_acquisition(event, request, request.request_id)
        else:
            self.add_acquisition(event, request, request.request_id)
        self.remove_pending(request)

    def remove_pending(self, request: Request):
        key = (request.gen, request.lock_id)
        queue = self.pending.get(key, [])
        if request in queue:
            queue.remove(request)
        request.active = False
        if request.request_id and self.requests_by_id.get(request.request_id) is request:
            del self.requests_by_id[request.request_id]

    def find_pending(self, event: Event) -> Optional[Request]:
        key = self.key(event)
        if event.request_id and event.request_id in self.requests_by_id:
            return self.requests_by_id[event.request_id]
        for request in self.pending.get(key, []):
            if request.thread_id == event.thread_id and request.active:
                return request
        return None

    def block_request(self, event: Event, request: Request, reason: str, confidence: str):
        key = self.key(event)
        holders = self.active_holders(key)
        for holder in holders:
            if holder.thread_id == event.thread_id:
                continue
            request.stack = list(event.stack or request.stack)
            self.pending.setdefault(key, []).append(request)
            if request.request_id:
                self.requests_by_id[request.request_id] = request
            self._wait_edges(
                event, request, holder,
                reason, confidence,
                blocked_event_id=event.id,
                wait_event_id=event.id,
                stack=request.stack,
            )

    def _wait_edges(self, event, request, holder, reason, confidence,
                    blocked_event_id, wait_event_id, stack, condition_id=""):
        return

    def process(self, event: Event):
        if self.last_ts is not None and event.ts < self.last_ts:
            self.out_of_order = True
            self.note(
                "warning", "timestamp_inversion",
                f"event {event.id} ({event.kind}) has an earlier timestamp than a prior event",
                [event.id],
            )
        self.last_ts = event.ts
        handler = getattr(self, f"handle_{event.kind}", None)
        if handler:
            handler(event)
        else:
            self.note("warning", "unhandled_event", f"no handler for {event.kind}", [event.id])

    def handle_thread_start(self, event: Event):
        if event.thread_id in self.threads and self.threads[event.thread_id]["ended"]:
            self.note("warning", "thread_restarted", f"thread {event.thread_id} restarted", [event.id])
        self.threads[event.thread_id] = {
            "thread_id": event.thread_id, "start_event_id": event.id,
            "end_event_id": None, "ended": False,
        }
        self.seen_starts.add(event.thread_id)

    def handle_thread_end(self, event: Event):
        thread = self.threads.setdefault(event.thread_id, {
            "thread_id": event.thread_id, "start_event_id": None,
            "end_event_id": None, "ended": False,
        })
        thread["ended"] = True
        thread["end_event_id"] = event.id
        for key, holders in list(self.holds.items()):
            if event.thread_id in holders:
                acquisition = holders[event.thread_id]
                if acquisition.count > acquisition.suspended:
                    self.note(
                        "error", "missing_release_at_exit",
                        f"thread {event.thread_id} ended while holding lock {event.lock_id or key[1]}",
                        [acquisition.success_event_id, event.id],
                    )
        for key, queue in list(self.pending.items()):
            for request in list(queue):
                if request.thread_id == event.thread_id:
                    self.remove_pending(request)
        for rid, wait in list(self.condwaits.items()):
            if wait.thread_id == event.thread_id and wait.active:
                self.finish_condition(wait, event, cancelled=True)

    def handle_acquire_request(self, event: Event):
        key = self.key(event)
        declared = key in self.declarations
        lock_type = self.lock_type(event.gen, event.lock_id)
        rid = event.request_id or f"e{event.id}"
        request = Request(
            event_id=event.id, gen=event.gen, thread_id=event.thread_id,
            lock_id=event.lock_id, mode=event.mode, request_id=rid,
            stack=list(event.stack),
        )
        if event.request_id and event.request_id in self.requests_by_id:
            self.note("error", "duplicate_request_id", f"duplicate request id {rid}", [event.id])
        holders = self.active_holders(key)
        own = self.own_active(key, event.thread_id)
        other = [h for h in holders if h.thread_id != event.thread_id]
        if own and not other and not declared:
            if lock_type == "rwlock" and exclusive_mode(event.mode) and shared_mode(own.mode):
                self.requests_by_id[rid] = request
                self._wait_edges(
                    event, request, own, "rw_upgrade", "high",
                    event.id, event.id, event.stack,
                )
                self.pending.setdefault(key, []).append(request)
                return
            if event.mode and shared_mode(event.mode) and own.mode == "write":
                pass
            else:
                inferred_type = "recursive"
                self.inferred.setdefault(key, inferred_type)
                self.grant_request(event, request)
                return
        if own and not other and declared and lock_type == "recursive":
            self.grant_request(event, request)
            return
        if own and not other and declared and lock_type == "mutex":
            self.requests_by_id[rid] = request
            self.pending.setdefault(key, []).append(request)
            self._wait_edges(event, request, own, "mutex_self_wait", "high",
                             event.id, event.id, event.stack)
            return
        if other:
            blocked_by = []
            compatible = lock_type == "rwlock" and shared_mode(event.mode) and all(
                shared_mode(h.mode) for h in other
            )
            if not compatible:
                self.requests_by_id[rid] = request
                self.pending.setdefault(key, []).append(request)
                for holder in other:
                    reason = "rw_read_conflict" if lock_type == "rwlock" else "lock_held"
                    self._wait_edges(event, request, holder, reason, "high",
                                     event.id, event.id, event.stack)
                return
        self.grant_request(event, request)

    def handle_acquire_success(self, event: Event):
        key = self.key(event)
        if event.request_id:
            mark = f"{event.gen}:{event.request_id}"
            if mark in self.seen_success:
                self.note("warning", "duplicate_success", f"duplicate success for request {event.request_id}", [event.id])
                return
            self.seen_success.add(mark)
        request = self.find_pending(event)
        holders = self.active_holders(key)
        other = [h for h in holders if h.thread_id != event.thread_id]
        lock_type = self.lock_type(event.gen, event.lock_id)
        if other:
            compatible = lock_type == "rwlock" and shared_mode(event.mode or (request.mode if request else "")) and all(
                shared_mode(h.mode) for h in other
            )
            if not compatible:
                self.note(
                    "error", "success_while_lock_held",
                    f"acquire success for thread {event.thread_id} conflicts with active holder(s)",
                    [event.id] + [h.success_event_id for h in other],
                )
        if request and request in self.pending.get(key, []):
            if other:
                self.note(
                    "warning", "observed_conflicting_success",
                    "success was observed even though reconstructed holders still block it",
                    [event.id],
                )
            self.grant_request(event, request)
            return
        if request and not request.active:
            self.note("warning", "success_after_completion", "success arrived after request completed", [event.id])
            return
        if not event.request_id:
            pass
        elif event.request_id in self.requests_by_id:
            request = self.requests_by_id[event.request_id]
        else:
            request = Request(
                event_id=event.id, gen=event.gen, thread_id=event.thread_id,
                lock_id=event.lock_id, mode=event.mode,
                request_id=event.request_id or f"e{event.id}", stack=list(event.stack),
            )
        self.grant_request(event, request)

    def handle_release(self, event: Event):
        key = self.key(event)
        holders = self.holds.get(key, {})
        acquisition = holders.get(event.thread_id)
        if not acquisition or acquisition.count <= acquisition.suspended:
            self.note(
                "error", "release_without_hold",
                f"thread {event.thread_id} released lock {event.lock_id} without a reconstructed hold",
                [event.id],
            )
            return
        acquisition.count -= 1
        if acquisition.count <= acquisition.suspended:
            self.note(
                "error", "release_during_condition_wait",
                "release observed while the holder is suspended in condition wait",
                [acquisition.wait_event_id, event.id],
            )
        if acquisition.count <= 0:
            holders.pop(event.thread_id, None)
            if not holders:
                self.holds.pop(key, None)

    def cancel_request(self, event: Event, code: str):
        request = None
        if event.request_id and event.request_id in self.requests_by_id:
            request = self.requests_by_id[event.request_id]
        else:
            for candidate in self.pending.get(self.key(event), []):
                if candidate.thread_id == event.thread_id and candidate.active:
                    request = candidate
                    break
        if request:
            self.remove_pending(request)
        else:
            self.note("info", code, f"timeout/cancel did not match a pending acquire: {code}", [event.id])

    def handle_acquire_timeout(self, event: Event):
        self.cancel_request(event, "acquire_timeout_unmatched")

    def handle_acquire_cancel(self, event: Event):
        self.cancel_request(event, "acquire_cancel_unmatched")

    def handle_condition_wait_start(self, event: Event):
        key = self.key(event)
        acquisition = self.own_active(key, event.thread_id)
        if not acquisition:
            self.note(
                "error", "condition_wait_without_hold",
                f"thread {event.thread_id} waited on condition without holding {event.lock_id}",
                [event.id],
            )
            return
        rid = event.request_id or f"cond-e{event.id}"
        wait = CondWait(
            event_id=event.id, gen=event.gen, thread_id=event.thread_id,
            lock_id=event.lock_id, condition_id=event.condition_id or event.lock_id,
            mode=acquisition.mode, request_id=rid, stack=list(event.stack or acquisition.stack),
        )
        self.condwaits[rid] = wait
        acquisition.suspended = acquisition.count
        acquisition.wait_request_id = rid
        acquisition.wait_event_id = event.id
        acquisition.notified = False
        acquisition.notify_event_id = 0

    def notify_condition(self, event: Event):
        condition_id = event.condition_id or event.lock_id
        all_waiters = bool(event.payload.get("all", event.payload.get("notify_all", False)))
        count = int(event.payload.get("count", 1 if not all_waiters else 10**9))
        matches = [
            wait for wait in self.condwaits.values()
            if wait.active and wait.gen == event.gen and wait.lock_id == event.lock_id
            and wait.condition_id == condition_id
        ]
        if not matches:
            self.note("info", "notify_without_waiter", "condition notification found no active waiter", [event.id])
            return
        for wait in (matches if all_waiters else matches[:count]):
            wait.notified = True
            wait.notify_event_id = event.id
            acquisition = self.holds.get((event.gen, event.lock_id), {}).get(wait.thread_id)
            if acquisition:
                acquisition.notified = True
                acquisition.notify_event_id = event.id

    def handle_condition_notify(self, event: Event):
        self.notify_condition(event)

    def restore_condition(self, wait: CondWait, event: Event, phase: str):
        key = (wait.gen, wait.lock_id)
        acquisition = self.holds.get(key, {}).get(wait.thread_id)
        wait.active = False
        if phase == "end":
            return
        if not acquisition:
            self.note(
                "error", "condition_reacquire_without_hold",
                "condition wait completed but the original suspended holding was absent",
                [wait.event_id, event.id],
            )
            return
        others = [h for h in self.active_holders(key) if h.thread_id != wait.thread_id]
        lock_type = self.lock_type(wait.gen, wait.lock_id)
        compatible = lock_type == "rwlock" and shared_mode(wait.mode) and all(
            shared_mode(h.mode) for h in others
        )
        if others and not compatible:
            self.note(
                "error", "condition_reacquired_while_locked",
                "condition wait reacquired a lock still held incompatibly by another thread",
                [wait.event_id, event.id] + [h.success_event_id for h in others],
            )
        acquisition.suspended = 0
        acquisition.wait_request_id = ""
        acquisition.wait_event_id = 0
        acquisition.notified = False
        acquisition.notify_event_id = 0
        acquisition.success_event_id = event.id
        if event.stack:
            acquisition.stack = list(event.stack)

    def finish_condition_event(self, event: Event, default_phase: str = "reacquired"):
        phase = str(event.payload.get("phase", default_phase))
        wait = None
        if event.request_id and event.request_id in self.condwaits:
            wait = self.condwaits[event.request_id]
        else:
            for candidate in self.condwaits.values():
                if (
                    candidate.active and candidate.gen == event.gen
                    and candidate.thread_id == event.thread_id
                    and candidate.lock_id == event.lock_id
                ):
                    wait = candidate
                    break
        if not wait:
            self.note("info", "condition_end_without_wait", "condition completion did not match an active wait", [event.id])
            return
        self.restore_condition(wait, event, phase)

    def handle_condition_wait_end(self, event: Event):
        self.finish_condition_event(event, "reacquired")

    def handle_condition_reacquired(self, event: Event):
        self.finish_condition_event(event, "reacquired")

    def handle_condition_timeout(self, event: Event):
        self.finish_condition_event(event, "reacquired")

    def handle_condition_cancel(self, event: Event):
        self.finish_condition_event(event, "reacquired")

    def finish_condition(self, wait: CondWait, event: Event, cancelled: bool = False):
        wait.active = False
        acquisition = self.holds.get((wait.gen, wait.lock_id), {}).get(wait.thread_id)
        if acquisition:
            self.note(
                "warning", "condition_wait_abandoned",
                "condition wait ended with the original holding still suspended; no release was invented",
                [wait.event_id, event.id],
            )

    def hold_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for (gen, lock_id), holders in sorted(self.holds.items()):
            for thread_id, acquisition in sorted(holders.items()):
                active = acquisition.count > acquisition.suspended
                confidence = "high"
                if self.thread_ended(thread_id):
                    confidence = "conflicted"
                if acquisition.suspended:
                    confidence = "incomplete"
                edges.append({
                    "kind": "holds", "id": f"hold-{gen}-{lock_id}-{thread_id}",
                    "gen": gen, "from_thread": thread_id, "to_thread": None,
                    "lock_id": lock_id, "mode": acquisition.mode,
                    "count": acquisition.count, "suspended": acquisition.suspended,
                    "active": active, "confidence": confidence,
                    "blocked_event_id": acquisition.success_event_id,
                    "wait_event_id": None,
                    "owner_event_id": acquisition.success_event_id,
                    "event_id": acquisition.success_event_id,
                    "stack": list(acquisition.stack),
                    "reason": "condition_suspended" if acquisition.suspended else "held",
                })
        return edges

    def thread_ended(self, thread_id: str) -> bool:
        return bool(self.threads.get(thread_id, {}).get("ended"))

    def wait_edges(self) -> List[Dict[str, Any]]:
        edges = []
        for (gen, lock_id), queue in sorted(self.pending.items()):
            holders = self.active_holders((gen, lock_id))
            for request in list(queue):
                if not request.active:
                    continue
                upgrade_owner = next((h for h in holders if h.thread_id == request.thread_id), None)
                targets = [upgrade_owner] if upgrade_owner else [
                    h for h in holders if h.thread_id != request.thread_id
                ]
                for holder in targets:
                    reason = "rw_upgrade" if upgrade_owner else "lock_held"
                    edges.append({
                        "kind": "waits_for", "id": f"wait-{gen}-{lock_id}-{request.thread_id}-{holder.thread_id}",
                        "gen": gen, "from_thread": request.thread_id,
                        "to_thread": holder.thread_id, "lock_id": lock_id,
                        "condition_id": "", "mode": request.mode,
                        "reason": reason, "confidence": "high",
                        "blocked_event_id": request.event_id,
                        "wait_event_id": request.event_id,
                        "owner_event_id": holder.success_event_id,
                        "owner_stack": list(holder.stack),
                        "stack": list(request.stack),
                    })
        for wait in [w for w in self.condwaits.values() if w.active]:
            holders = self.active_holders((wait.gen, wait.lock_id))
            if wait.notified:
                for holder in holders:
                    if holder.thread_id == wait.thread_id:
                        continue
                    edges.append({
                        "kind": "waits_for",
                        "id": f"condwait-{wait.gen}-{wait.lock_id}-{wait.thread_id}-{holder.thread_id}-{wait.event_id}",
                        "gen": wait.gen, "from_thread": wait.thread_id,
                        "to_thread": holder.thread_id, "lock_id": wait.lock_id,
                        "condition_id": wait.condition_id, "mode": wait.mode,
                        "reason": "condition_reacquire", "confidence": "likely",
                        "blocked_event_id": wait.notify_event_id or wait.event_id,
                        "wait_event_id": wait.event_id,
                        "owner_event_id": holder.success_event_id,
                        "owner_stack": list(holder.stack),
                        "stack": list(wait.stack),
                    })
            else:
                edges.append({
                    "kind": "condition_wait",
                    "id": f"condwait-{wait.gen}-{wait.lock_id}-{wait.condition_id}-{wait.thread_id}-{wait.event_id}",
                    "gen": wait.gen, "from_thread": wait.thread_id,
                    "to_thread": None, "lock_id": wait.lock_id,
                    "condition_id": wait.condition_id, "mode": wait.mode,
                    "reason": "condition_not_notified", "confidence": "incomplete",
                    "blocked_event_id": wait.event_id, "wait_event_id": wait.event_id,
                    "owner_event_id": None, "stack": list(wait.stack),
                })
        return edges

    def as_dict(self, at_ts: Optional[int] = None, event_id: Optional[int] = None) -> Dict[str, Any]:
        hold_edges = self.hold_edges()
        wait_edges = self.wait_edges()
        cycles = detect_cycles(wait_edges)
        confidence = self.confidence
        if any(e["confidence"] == "incomplete" for e in wait_edges):
            confidence = "incomplete"
        elif confidence == "high" and any(e["confidence"] == "likely" for e in wait_edges):
            confidence = "likely"
        if not self.issues:
            confidence = "high" if confidence == "high" else confidence
        return {
            "at_ts": at_ts,
            "at_event_id": event_id,
            "order": self.order,
            "confidence": confidence,
            "issues": [issue.as_dict() for issue in self.issues],
            "threads": list(self.threads.values()),
            "holds": [acquisition.as_dict() | {"thread_id": thread_id}
                      for holders in self.holds.values()
                      for thread_id, acquisition in holders.items()],
            "pending_requests": [
                request.as_dict()
                for queue in self.pending.values()
                for request in queue
                if request.active
            ],
            "condition_waits": [
                wait.as_dict()
                for wait in self.condwaits.values()
                if wait.active
            ],
            "inferred_lock_types": [
                {"gen": gen, "lock_id": lock_id, "type": lock_type}
                for (gen, lock_id), lock_type in sorted(self.inferred.items())
            ],
            "hold_graph": {"nodes": self.graph_nodes(hold_edges), "edges": hold_edges},
            "wait_graph": {"nodes": self.graph_nodes(wait_edges, conditions=True), "edges": wait_edges},
            "cycles": cycles,
        }

    def graph_nodes(self, edges: List[Dict[str, Any]], conditions: bool = False) -> List[Dict[str, Any]]:
        nodes = {}
        for edge in edges:
            source = edge.get("from_thread")
            target = edge.get("to_thread")
            if source:
                nodes[source] = {"id": source, "type": "thread", "label": source}
            if target:
                nodes[target] = {"id": target, "type": "thread", "label": target}
            lock_id = edge.get("lock_id")
            if lock_id:
                node_id = f"lock:{edge['gen']}:{lock_id}"
                nodes[node_id] = {"id": node_id, "type": "lock", "label": f"g{edge['gen']}:{lock_id}"}
            if conditions and edge.get("condition_id"):
                node_id = f"condition:{edge['gen']}:{edge['condition_id']}"
                nodes[node_id] = {"id": node_id, "type": "condition", "label": f"g{edge['gen']}:{edge['condition_id']}"}
        return sorted(nodes.values(), key=lambda node: (node["type"], node["id"]))


def detect_cycles(edges: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    adjacency: Dict[str, List[Dict[str, Any]]] = {}
    for edge in edges:
        if edge["kind"] != "waits_for" or not edge.get("to_thread"):
            continue
        adjacency.setdefault(edge["from_thread"], []).append(edge)
    found = []
    seen = set()

    def dfs(start: str, current: str, path_edges: List[Dict[str, Any]], visited: set):
        for edge in adjacency.get(current, []):
            target = edge["to_thread"]
            if target == start and path_edges:
                ordered = tuple(sorted((item["id"] for item in path_edges + [edge])))
                if ordered not in seen:
                    seen.add(ordered)
                    found.append(path_edges + [edge])
            elif target not in visited and target > start:
                visited.add(target)
                dfs(start, target, path_edges + [edge], visited)
                visited.remove(target)

    for node in sorted(adjacency):
        dfs(node, node, [], {node})

    cycles = []
    for cycle_edges in found:
        threads = [edge["from_thread"] for edge in cycle_edges]
        cycles.append({
            "id": "cycle-" + "-".join(
                f"{edge['from_thread']}>{edge['to_thread']}@{edge['lock_id']}" for edge in cycle_edges
            ),
            "threads": threads,
            "locks": [edge["lock_id"] for edge in cycle_edges],
            "signature": " -> ".join(threads + [threads[0]]),
            "confidence": min_confidence([edge["confidence"] for edge in cycle_edges]),
            "edges": cycle_edges,
            "holding_stacks": [
                {
                    "thread_id": edge["to_thread"],
                    "lock_id": edge["lock_id"],
                    "event_id": edge["owner_event_id"],
                    "stack": owner_stack(edge),
                }
                for edge in cycle_edges
            ],
            "waiting_stacks": [
                {
                    "thread_id": edge["from_thread"],
                    "lock_id": edge["lock_id"],
                    "event_id": edge["wait_event_id"],
                    "stack": list(edge.get("stack") or []),
                }
                for edge in cycle_edges
            ],
        })
    return cycles


def owner_stack(edge: Dict[str, Any]) -> List[str]:
    return list(edge.get("owner_stack") or [])


def min_confidence(values: Iterable[str]) -> str:
    ranks = {"high": 0, "likely": 1, "incomplete": 2, "conflicted": 3}
    return max(values, key=lambda value: ranks.get(value, 0)) if values else "high"


class Timeline:
    def __init__(self, events: Iterable[Dict[str, Any] | Event], declarations: Optional[Dict[Tuple[int, str], str]] = None):
        self.events = [
            event if isinstance(event, Event) else Event.from_row(event)
            for event in events
        ]
        self.events.sort(key=lambda event: (event.seq, event.id))
        self.declarations = declarations or {}
        self._checkpoints: Dict[str, List[Tuple[int, State]]] = {}
        self.last_recompute: Dict[str, Any] = {}

    def ordered_events(self, order: str) -> List[Event]:
        if order == "arrival":
            return sorted(self.events, key=lambda event: (event.seq, event.id))
        return sorted(self.events, key=lambda event: (event.ts, event.seq, event.id))

    def cutoff_index(self, order: str, at_ts: Optional[int], at_event_id: Optional[int]) -> int:
        ordered = self.ordered_events(order)
        if at_event_id is not None:
            for index, event in enumerate(ordered):
                if event.id == at_event_id:
                    return index + 1
            raise KeyError(f"event {at_event_id} not found")
        if at_ts is None:
            return len(ordered)
        return bisect_right([event.ts for event in ordered], at_ts)

    def state_at(self, order: str = "timestamp", at_ts: Optional[int] = None,
                 at_event_id: Optional[int] = None) -> Tuple[State, int]:
        order = order if order in {"timestamp", "arrival"} else "timestamp"
        ordered = self.ordered_events(order)
        limit = self.cutoff_index(order, at_ts, at_event_id)
        state = self._state_for_prefix(order, ordered, limit)
        return state, limit

    def _state_for_prefix(self, order: str, ordered: List[Event], limit: int) -> State:
        checkpoints = self._checkpoints.setdefault(order, [])
        usable = [(index, state) for index, state in checkpoints if index <= limit]
        if usable:
            start, template = max(usable, key=lambda item: item[0])
        else:
            start = 0
            template = State(declarations=dict(self.declarations), events=list(ordered), order=order)
        state = template.clone()
        for event in ordered[start:limit]:
            state.process(event)
        if limit == len(ordered) and (not checkpoints or limit - max(i for i, _ in checkpoints) >= 32):
            checkpoints.append((limit, state.clone()))
            checkpoints.sort(key=lambda item: item[0])
        return state

    def invalidate_from(self, event_id: int, order: str):
        ordered_ids = [event.id for event in self.ordered_events(order)]
        try:
            position = ordered_ids.index(event_id)
        except ValueError:
            return
        self._checkpoints[order] = [
            (index, state) for index, state in self._checkpoints[order]
            if index <= position
        ]
        state, _ = self.state_at(order)
        self.last_recompute[order] = {
            "from_event_id": event_id,
            "reprocessed_event_count": len(ordered_ids) - position,
        }
        return self.last_recompute[order]

    def candidates(self, at_ts: Optional[int] = None, at_event_id: Optional[int] = None) -> List[Dict[str, Any]]:
        result = []
        signatures = set()
        for order in ("timestamp", "arrival"):
            state, _ = self.state_at(order, at_ts, at_event_id)
            snapshot = state.as_dict(at_ts=at_ts, event_id=at_event_id)
            signature = (
                tuple(edge["id"] for edge in snapshot["wait_graph"]["edges"]),
                tuple(edge["id"] for edge in snapshot["hold_graph"]["edges"]),
                tuple((issue.code, tuple(issue.event_ids)) for issue in snapshot["issues"]),
                tuple((cycle["id"],) for cycle in snapshot["cycles"]),
            )
            if signature not in signatures:
                signatures.add(signature)
                result.append(snapshot)
        return result

    def graph(self, at_ts: Optional[int] = None, at_event_id: Optional[int] = None,
              order: str = "timestamp") -> Dict[str, Any]:
        state, _ = self.state_at(order, at_ts, at_event_id)
        return state.as_dict(at_ts=at_ts, event_id=at_event_id)


def group_events(events: List[Event]) -> Dict[str, List[int]]:
    groups: Dict[str, List[int]] = {}
    for event in events:
        keys = [f"event-{event.id}"]
        if event.request_id:
            keys.append(f"request-{event.gen}-{event.request_id}")
        if event.kind.startswith("condition_wait") or event.kind in {
            "condition_reacquired", "condition_notify", "condition_timeout", "condition_cancel"
        }:
            keys.append(f"condition-{event.gen}-{event.condition_id or event.lock_id}-{event.thread_id}")
        if event.kind in {"acquire_success", "release"}:
            keys.append(f"hold-{event.gen}-{event.lock_id}-{event.thread_id}")
        for key in keys:
            groups.setdefault(key, []).append(event.id)
    merged: List[set] = []
    by_key = {key: set(ids) for key, ids in groups.items()}
    for ids in by_key.values():
        hit = next((group for group in merged if group & ids), None)
        if hit is None:
            merged.append(set(ids))
        else:
            hit.update(ids)
    output = {}
    for index, ids in enumerate(merged):
        output[f"g{index}"] = sorted(ids)
    return output


def cycle_signatures(timeline: Timeline, order: str = "timestamp") -> set:
    snapshot = timeline.graph(order=order)
    return {
        tuple(sorted((edge["from_thread"], edge["to_thread"], edge["lock_id"], edge["gen"])
                     for edge in cycle["edges"]))
        for cycle in snapshot["cycles"]
    }


def minimize_for_cycles(
    events: Iterable[Dict[str, Any] | Event],
    declarations: Optional[Dict[Tuple[int, str], str]] = None,
    target_signatures: Optional[set] = None,
    order: str = "timestamp",
) -> Dict[str, Any]:
    parsed = [event if isinstance(event, Event) else Event.from_row(event) for event in events]
    full = Timeline(parsed, declarations)
    wanted = target_signatures or cycle_signatures(full, order)
    if not wanted:
        return {"event_ids": [], "events": [], "cycles": [], "order": order}
    snapshot = full.graph(order=order)
    wanted_cycles = [
        cycle for cycle in snapshot["cycles"]
        if tuple(sorted((edge["from_thread"], edge["to_thread"], edge["lock_id"], edge["gen"])
                        for edge in cycle["edges"])) in wanted
    ]
    groups = group_events(parsed)
    id_to_event = {event.id: event for event in parsed}
    seed_ids = set()
    for cycle in wanted_cycles:
        for edge in cycle["edges"]:
            for field_name in ("wait_event_id", "owner_event_id", "blocked_event_id"):
                event_id = edge.get(field_name)
                if event_id:
                    seed_ids.add(event_id)
    seed_groups = set()
    for group_name, ids in groups.items():
        if any(event_id in seed_ids for event_id in ids):
            seed_groups.add(group_name)

    required = set(seed_groups)
    for group_name in sorted(set(groups) - required, key=lambda name: len(groups[name]), reverse=True):
        trial_ids = set()
        for included in required | {group_name}:
            trial_ids.update(groups[included])
        trial_events = [id_to_event[event_id].public() for event_id in sorted(trial_ids)]
        trial = Timeline(trial_events, declarations)
        if wanted.issubset(cycle_signatures(trial, order)):
            required.add(group_name)

    final_ids = set()
    for group_name in required:
        final_ids.update(groups[group_name])
    final_events = [event.public() for event in parsed if event.id in final_ids]
    minimized = Timeline(final_events, declarations)
    final_snapshot = minimized.graph(order=order)
    return {
        "event_ids": sorted(final_ids),
        "events": final_events,
        "cycles": [cycle for cycle in final_snapshot["cycles"]
                   if tuple(sorted((edge["from_thread"], edge["to_thread"], edge["lock_id"], edge["gen"])
                                   for edge in cycle["edges"])) in wanted],
        "order": order,
        "removed_event_count": len(parsed) - len(final_ids),
    }
