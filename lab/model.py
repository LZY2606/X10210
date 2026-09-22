"""事件模型与归一化。

支持的事件类型（含常见中文别名）::

    restart                 进程重启 -> 新代次
    thread_start / thread_end
    request / acquired / release / timeout
    cond_wait / cond_notify / cond_notify_all

每个事件都有 ``thread``、``lock``（condition 事件 lock 为关联互斥锁，
另用 ``cond`` 指定条件变量）、时间戳 ``ts`` 与可选的 ``stack``。
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Dict, List, Optional


TYPE_ALIASES = {
    "request": "request",
    "acquire_request": "request",
    "try_acquire": "request",
    "wait_lock": "request",
    "请求": "request",
    "acquired": "acquired",
    "acquire": "acquired",
    "acquire_success": "acquired",
    "lock_acquired": "acquired",
    "获得": "acquired",
    "release": "release",
    "unlock": "release",
    "释放": "release",
    "timeout": "timeout",
    "cancel": "timeout",
    "acquire_timeout": "timeout",
    "超时": "timeout",
    "thread_start": "thread_start",
    "thread_begin": "thread_start",
    "start": "thread_start",
    "线程开始": "thread_start",
    "thread_end": "thread_end",
    "thread_exit": "thread_end",
    "end": "thread_end",
    "terminate": "thread_end",
    "线程结束": "thread_end",
    "cond_wait": "cond_wait",
    "wait": "cond_wait",
    "condition_wait": "cond_wait",
    "条件等待": "cond_wait",
    "cond_notify": "cond_notify",
    "notify": "cond_notify",
    "signal": "cond_notify",
    "通知": "cond_notify",
    "cond_notify_all": "cond_notify_all",
    "notify_all": "cond_notify_all",
    "broadcast": "cond_notify_all",
    "restart": "restart",
    "process_start": "restart",
    "重启": "restart",
}


def norm_mode(value: Any) -> str:
    if value is None:
        return "exclusive"
    v = str(value).strip().lower()
    if v in ("r", "read", "shared", "share", "s", "rd", "读", "共享"):
        return "read"
    if v in ("w", "write", "exclusive", "x", "wr", "写", "独占"):
        return "write"
    return "write"


@dataclass
class Event:
    seq: int  # 到达序号（全局，1 起，存储层分配）
    type: str
    thread: str
    lock: Optional[str] = None
    ts: Optional[float] = None
    gen: str = "g0"
    cond: Optional[str] = None
    mode: str = "write"  # read / write（写包含互斥独占）
    stack: List[str] = field(default_factory=list)
    source_id: Optional[str] = None
    late: bool = False  # 相对同代已到达事件是否乱序
    raw: Dict[str, Any] = field(default_factory=dict)

    def key(self) -> Any:
        """归一化事件的稳定身份，用于校正引用（优先客户端 id）。"""
        return self.source_id if self.source_id is not None else self.seq

    def to_dict(self) -> Dict[str, Any]:
        return {
            "seq": self.seq,
            "type": self.type,
            "thread": self.thread,
            "lock": self.lock,
            "ts": self.ts,
            "gen": self.gen,
            "cond": self.cond,
            "mode": self.mode,
            "stack": list(self.stack),
            "source_id": self.source_id,
            "late": self.late,
        }


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def normalize_event(
    item: Dict[str, Any],
    seq: int,
    *,
    default_gen: str = "g0",
    prev_ts: Optional[float] = None,
) -> Event:
    """把一条外部 JSON 记录归一化为 :class:`Event`。

    缺失的时间戳用前序事件时间兜底（仍为 None 时交给重放层）。
    """
    raw = dict(item)
    raw_type = str(item.get("type") or item.get("op") or item.get("event") or "").strip()
    etype = TYPE_ALIASES.get(raw_type, TYPE_ALIASES.get(raw_type.lower(), raw_type))

    thread = _as_str(item.get("thread") or item.get("tid") or item.get("thread_id"))
    if thread is None and etype != "restart":
        raise ValueError("event missing thread: %r" % raw)

    lock = _as_str(item.get("lock") or item.get("lock_addr") or item.get("addr") or item.get("mutex"))
    cond = _as_str(item.get("cond") or item.get("condition") or item.get("cv"))

    ts_val = item.get("ts")
    if ts_val is None:
        ts_val = item.get("timestamp") or item.get("time")
    ts: Optional[float]
    if ts_val is None:
        ts = prev_ts
    else:
        ts = float(ts_val)

    gen = _as_str(item.get("gen") or item.get("generation")) or default_gen
    source_id = item.get("id")
    source_id = None if source_id is None else str(source_id)
    stack = item.get("stack") or item.get("backtrace") or []
    if isinstance(stack, str):
        stack = [stack]
    stack = [str(s) for s in stack]

    mode = norm_mode(item.get("mode"))
    if etype == "cond_wait" and not mode:
        mode = "write"

    return Event(
        seq=seq,
        type=etype,
        thread=thread or "",
        lock=lock,
        ts=ts,
        gen=gen,
        cond=cond,
        mode=mode,
        stack=stack,
        source_id=source_id,
        raw=raw,
    )


def event_without_seq(item: Dict[str, Any]) -> Dict[str, Any]:
    """校正新增事件时剥离客户端 seq，避免与存储层序号冲突。"""
    clean = dict(item)
    clean.pop("seq", None)
    return clean


def clone_event(event: Event, **changes: Any) -> Event:
    return replace(event, **changes)
