"""事件模型与基础常量。

事件以 JSON 字典形式导入，规范化为 :class:`Event`。每个事件属于一个采集代次
``generation``，相同锁地址在不同代次中互不相干。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional

# -- 事件类型 ---------------------------------------------------------------

ACQ_REQUEST = "acquire_request"   # 线程请求获取锁
ACQUIRED = "acquired"             # 获取成功
RELEASED = "released"             # 释放
T_START = "thread_start"          # 线程开始
T_END = "thread_end"              # 线程结束
COND_WAIT = "cond_wait"           # condition wait：暂时释放锁，等待被通知后重新竞争
COND_NOTIFY = "cond_notify"       # 单个通知（signal）
COND_NOTIFY_ALL = "cond_notify_all"  # 广播通知（broadcast）
COND_TIMEOUT = "cond_timeout"     # condition wait 超时/被取消
TIMEOUT = "timeout"               # 锁请求超时：撤销等待
CANCEL = "cancel"                 # 锁请求被取消：撤销等待

EVENT_TYPES = frozenset({
    ACQ_REQUEST, ACQUIRED, RELEASED, T_START, T_END,
    COND_WAIT, COND_NOTIFY, COND_NOTIFY_ALL, COND_TIMEOUT,
    TIMEOUT, CANCEL,
})

# -- 锁类型 -----------------------------------------------------------------

MUTEX = "mutex"          # 普通互斥锁
RECURSIVE = "recursive"  # 可重入互斥锁
RWLOCK = "rwlock"        # 读写锁：共享读 / 独占写 / 升级

LOCK_TYPES = frozenset({MUTEX, RECURSIVE, RWLOCK})

READ = "read"
WRITE = "write"
SHARED = "shared"
EXCLUSIVE = "exclusive"
UPGRADE = "upgrade"

# -- 置信状态 ---------------------------------------------------------------

HIGH = "high"
MEDIUM = "medium"
LOW = "low"

# 原始字段名 -> 规范字段名（容忍采集端差异）
_FIELD_ALIASES = {
    "seq": "seq",
    "sequence": "seq",
    "ts": "ts",
    "timestamp": "ts",
    "time": "ts",
    "gen": "generation",
    "generation_id": "generation",
    "type": "type",
    "event": "type",
    "event_type": "type",
    "kind": "type",
    "thread": "thread",
    "thread_id": "thread",
    "tid": "thread",
    "lock": "lock",
    "lock_addr": "lock",
    "addr": "lock",
    "address": "lock",
    "resource": "lock",
    "mode": "mode",
    "access": "mode",
    "rw": "mode",
    "cv": "cv",
    "cond": "cv",
    "condition": "cv",
    "cond_addr": "cv",
    "granted": "granted",
    "stack": "stack",
    "wait_stack": "wait_stack",
    "hold_stack": "hold_stack",
    "note": "note",
}


def normalize_mode(value: Any) -> str:
    if value is None:
        return EXCLUSIVE
    v = str(value).strip().lower()
    if v in ("r", "rd", "read", "shared", "s", "sh", "share"):
        return READ
    if v in ("w", "wr", "write", "exclusive", "x", "excl"):
        return WRITE
    if v in ("u", "up", "upgrade", "rw"):
        return UPGRADE
    return v


@dataclass
class Event:
    """规范化后的单个采集事件。"""

    id: int
    seq: int
    ts: float
    generation: str
    type: str
    thread: str
    lock: Optional[str] = None
    mode: str = EXCLUSIVE
    cv: Optional[str] = None
    granted: bool = True
    stack: Optional[list] = None
    wait_stack: Optional[list] = None
    hold_stack: Optional[list] = None
    note: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @property
    def mode_norm(self) -> str:
        return self.mode


def normalize_event(raw: dict, event_id: int, seq: int,
                    default_gen: str = "gen-0") -> Event:
    """把一条原始 JSON 事件转成 :class:`Event`，不做语义校验。"""
    fields: dict[str, Any] = {}
    for key, value in raw.items():
        canonical = _FIELD_ALIASES.get(str(key).lower(), str(key).lower())
        fields.setdefault(canonical, value)

    etype = str(fields.get("type", "")).strip()
    if etype not in EVENT_TYPES:
        raise ValueError(f"未知事件类型: {etype!r}")

    ts = fields.get("ts", 0)
    try:
        ts = float(ts)
    except (TypeError, ValueError):
        ts = 0.0

    thread = fields.get("thread")
    thread = str(thread) if thread is not None else ""

    lock = fields.get("lock")
    lock = str(lock) if lock is not None else None

    cv = fields.get("cv")
    cv = str(cv) if cv is not None else None

    mode = normalize_mode(fields.get("mode"))

    def _stack(name: str) -> Optional[list]:
        val = fields.get(name)
        if val is None:
            return None
        if isinstance(val, (list, tuple)):
            return [str(x) for x in val]
        return [str(val).strip()] if str(val).strip() else None

    return Event(
        id=event_id,
        seq=int(fields.get("seq", seq)),
        ts=ts,
        generation=str(fields.get("generation") or default_gen),
        type=etype,
        thread=thread,
        lock=lock,
        mode=mode,
        cv=cv,
        granted=bool(fields.get("granted", True)),
        stack=_stack("stack"),
        wait_stack=_stack("wait_stack"),
        hold_stack=_stack("hold_stack"),
        note=(str(fields["note"]) if fields.get("note") is not None else None),
    )
