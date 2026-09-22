"""事件、持有帧、等待、异常与快照的数据模型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ---- 事件种类 -------------------------------------------------------------
EV_ACQ_REQUEST = "acq_request"
EV_ACQ_GRANTED = "acq_granted"
EV_RELEASE = "release"
EV_THREAD_START = "thread_start"
EV_THREAD_END = "thread_end"
EV_COND_WAIT = "cond_wait"
EV_COND_NOTIFY = "cond_notify"
EV_ACQ_TIMEOUT = "acq_timeout"
EVENT_KINDS = {
    EV_ACQ_REQUEST,
    EV_ACQ_GRANTED,
    EV_RELEASE,
    EV_THREAD_START,
    EV_THREAD_END,
    EV_COND_WAIT,
    EV_COND_NOTIFY,
    EV_ACQ_TIMEOUT,
}
LOCK_KINDS = {"mutex", "rwlock"}
MODE_X = "X"
MODE_S = "S"

# ---- 异常代码与修复策略 ---------------------------------------------------
ANOM_RELEASE_WITHOUT_HOLD = "release_without_hold"
ANOM_GRANT_WITHOUT_REQUEST = "grant_without_request"
ANOM_CONDWAIT_WITHOUT_HOLD = "condwait_without_hold"
ANOM_NOTIFY_NO_WAITER = "notify_no_waiter"
ANOM_THREAD_ENDED = "event_on_ended_thread"

REPAIR_INFERR_ACQUIRE = "infer_missing_acquire"
REPAIR_REJECT_EVENT = "reject_event"
REPAIR_INFERR_GRANT = "infer_synthetic_grant"
REPAIR_DROP_GRANT = "drop_orphan_grant"
REPAIR_LITERAL = "literal"

# 每种异常在「字面执行」之外允许的合法修复
ALTERNATIVE_REPAIRS: Dict[str, List[str]] = {
    ANOM_RELEASE_WITHOUT_HOLD: [REPAIR_INFERR_ACQUIRE, REPAIR_REJECT_EVENT],
    ANOM_GRANT_WITHOUT_REQUEST: [REPAIR_DROP_GRANT],
    ANOM_CONDWAIT_WITHOUT_HOLD: [REPAIR_REJECT_EVENT],
}
# 字面执行（不做修复）时异常的默认表现
PRIMARY_LITERAL = "literal"


@dataclass
class Event:
    """一条原始线程事件。按采集代次 (gen) 与到达序 (seq) 保留原始顺序。"""

    raw_id: Optional[int]
    gen: str
    seq: int
    ts: int
    kind: str
    thread: str
    lock: Optional[str] = None
    mode: Optional[str] = None
    gen_from: Optional[str] = None
    timeout: Optional[int] = None
    stack: List[str] = field(default_factory=list)
    deleted: bool = False

    @property
    def lock_uid(self) -> Optional[str]:
        """相同地址在进程重启 (不同 gen) 后属于不同锁。"""
        if self.lock is None:
            return None
        return f"{self.gen}:{self.lock}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.raw_id,
            "gen": self.gen,
            "seq": self.seq,
            "ts": self.ts,
            "kind": self.kind,
            "thread": self.thread,
            "lock": self.lock,
            "lock_uid": self.lock_uid,
            "mode": self.mode,
            "gen_from": self.gen_from,
            "timeout": self.timeout,
            "stack": list(self.stack),
            "deleted": self.deleted,
        }


@dataclass
class Frame:
    """持有栈中的一帧：一次加锁 (可重入时同锁多帧)。"""

    lock: str
    mode: str
    acquire_event_id: int
    acquire_ts: int
    acquire_stack: List[str]
    reason: str = "acquire"  # acquire | cond_reacquire
    reentrant: bool = False
    confidence: str = "confirmed"  # confirmed | low | stale
    saved_cond_wait_id: Optional[int] = None

    def clone(self) -> "Frame":
        return Frame(
            lock=self.lock,
            mode=self.mode,
            acquire_event_id=self.acquire_event_id,
            acquire_ts=self.acquire_ts,
            acquire_stack=list(self.acquire_stack),
            reason=self.reason,
            reentrant=self.reentrant,
            confidence=self.confidence,
            saved_cond_wait_id=self.saved_cond_wait_id,
        )


@dataclass
class Wait:
    """一个进行中 (或已结束) 的等待。"""

    event_id: int
    ts: int
    lock: str
    mode: str
    stack: List[str]
    reason: str  # acquire | upgrade | cond_reacquire
    timeout: Optional[int] = None
    deadline: Optional[int] = None
    cond_wait_event_id: Optional[int] = None
    status: str = "waiting"  # waiting|granted|timeout|thread_exit|rejected
    end_event_id: Optional[int] = None
    confidence: str = "confirmed"
    saved_frame: Optional[Frame] = None

    def clone(self) -> "Wait":
        return Wait(
            event_id=self.event_id,
            ts=self.ts,
            lock=self.lock,
            mode=self.mode,
            stack=list(self.stack),
            reason=self.reason,
            timeout=self.timeout,
            deadline=self.deadline,
            cond_wait_event_id=self.cond_wait_event_id,
            status=self.status,
            end_event_id=self.end_event_id,
            confidence=self.confidence,
            saved_frame=None if self.saved_frame is None else self.saved_frame.clone(),
        )


@dataclass
class Anomaly:
    """缺事件 / 语义不一致，带置信状态，不凭空补 release。"""

    code: str
    event_id: Optional[int]
    thread: Optional[str]
    lock: Optional[str]
    message: str
    confidence: str = "low"

    @property
    def key(self) -> str:
        return f"{self.code}:{self.event_id}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "event_id": self.event_id,
            "thread": self.thread,
            "lock": self.lock,
            "message": self.message,
            "confidence": self.confidence,
            "alternatives": ALTERNATIVE_REPAIRS.get(self.code, []),
        }


@dataclass
class HoldEdge:
    thread: str
    lock: str
    mode: str
    depth: int
    since_event_id: int
    since_ts: int
    stack: List[str]
    confidence: str
    reason: str
    reentrant: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "thread": self.thread,
            "lock": self.lock,
            "mode": self.mode,
            "depth": self.depth,
            "since_event_id": self.since_event_id,
            "since_ts": self.since_ts,
            "stack": list(self.stack),
            "confidence": self.confidence,
            "reason": self.reason,
            "reentrant": self.reentrant,
            "edge_kind": "hold",
            "evidence_event_ids": [self.since_event_id],
        }


@dataclass
class WaitEdge:
    thread: str
    lock: str
    mode: str
    event_id: int
    ts: int
    stack: List[str]
    reason: str
    blockers: List[str]
    confidence: str
    deadline: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "thread": self.thread,
            "lock": self.lock,
            "mode": self.mode,
            "event_id": self.event_id,
            "ts": self.ts,
            "stack": list(self.stack),
            "reason": self.reason,
            "blockers": list(self.blockers),
            "confidence": self.confidence,
            "deadline": self.deadline,
            "edge_kind": "wait",
            "evidence_event_ids": [self.event_id],
        }
