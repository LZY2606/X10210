"""事件模型与归一化。

事件类型（kind）：
  thread_start / thread_end        线程开始 / 结束
  request                          一次加锁请求（可带超时，可被 cancel 撤销）
  acquire                          加锁成功（request 的完成事件；对乱序缺失会推断）
  release                          释放锁
  cond_wait                        在条件变量上等待（原子释放锁；醒来后重新竞争）
  cond_notify                      唤醒一个（all=false）或全部（all=true）等待者
  cond_wait_cancel                 condition 等待超时 / 取消（醒来但不必然持锁）
  cancel                           撤销一个加锁 request

身份键：
  锁  = (generation, lock)；条件变量 = (generation, cond)；线程 = (generation, thread)
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional

KINDS = {
    "thread_start", "thread_end", "request", "acquire", "release",
    "cond_wait", "cond_notify", "cond_wait_cancel", "cancel",
}
LOCK_KINDS = {"request", "acquire", "release"}
MODES = {"X", "S"}
LOCK_TYPES = {"mutex", "recursive", "rwlock"}


class EventError(ValueError):
    pass


@dataclass
class Event:
    seq: int                     # 存储分配的全局自增序（流式到达序）
    generation: str              # 采集代次（进程代次）
    ev_id: str                   # 外部事件标识（采集端给出）
    kind: str
    ts: int                      # 时间戳（微秒）
    thread: str = ""
    lock: Optional[str] = None
    mode: str = "X"              # X=独占 S=共享
    cond: Optional[str] = None
    lock_type: Optional[str] = None
    request_id: Optional[str] = None
    timeout_ms: Optional[int] = None
    all: bool = False
    stack: Optional[list] = None
    raw_order: int = 0           # 采集代次内的原始顺序（缺省按 ts/到达序）

    def key(self) -> tuple:
        return (self.generation, self.ev_id)

    def to_dict(self) -> dict:
        return asdict(self)


def _norm_mode(value: Any) -> str:
    if value in (None, "", "X", "x", "exclusive", "write", "W"):
        return "X"
    if value in ("S", "s", "shared", "read", "R"):
        return "S"
    raise EventError(f"未知锁模式: {value!r}")


def normalize(raw: dict) -> dict:
    """归一化一条原始事件（尚未分配 seq）。"""
    kind = raw.get("kind")
    if kind not in KINDS:
        raise EventError(f"未知事件类型: {kind!r}，允许 {sorted(KINDS)}")
    gen = str(raw.get("generation", "g0"))
    ev_id = raw.get("ev_id")
    if ev_id is None or str(ev_id) == "":
        raise EventError("事件缺少 ev_id")
    ev_id = str(ev_id)
    ts = raw.get("ts", 0)
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        raise EventError("ts 必须是整数")

    out: dict = {
        "generation": gen,
        "ev_id": ev_id,
        "kind": kind,
        "ts": ts,
        "thread": str(raw.get("thread", "")),
        "lock": raw.get("lock"),
        "mode": _norm_mode(raw.get("mode")),
        "cond": raw.get("cond"),
        "lock_type": raw.get("lock_type"),
        "request_id": (str(raw["request_id"]) if raw.get("request_id") is not None else None),
        "timeout_ms": raw.get("timeout_ms"),
        "all": bool(raw.get("all", False)),
        "stack": list(raw["stack"]) if raw.get("stack") else None,
        "raw_order": int(raw.get("raw_order", 0) or 0),
    }
    if out["lock_type"] is not None and out["lock_type"] not in LOCK_TYPES:
        raise EventError(f"未知锁类型: {out['lock_type']!r}，允许 {sorted(LOCK_TYPES)}")
    if kind in LOCK_KINDS | {"cond_wait"} and not out["lock"]:
        raise EventError(f"{kind} 事件缺少 lock")
    if kind in LOCK_KINDS | {"cond_wait"} and not out["thread"]:
        raise EventError(f"{kind} 事件缺少 thread")
    if kind in {"cond_wait", "cond_notify", "cond_wait_cancel"} and not out["cond"]:
        raise EventError(f"{kind} 事件缺少 cond")
    if out["lock"] is not None:
        out["lock"] = str(out["lock"])
    if out["cond"] is not None:
        out["cond"] = str(out["cond"])
    return out
