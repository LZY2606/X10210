"""事件模型。

线程/锁采集器会产生下列事件（均带时间戳与采集代次）：

  thread_start / thread_end          线程开始 / 终止
  acquire_req                        发起获取锁（可带 timeout_ms）
  acquire_ok                         获取成功（含重入 / 条件变量重新竞争成功）
  acquire_timeout                    超时，未获取到锁（等待撤销）
  acquire_cancel                     其它原因取消等待（如 interrupt）
  release                            释放一次（重入锁每次减 1；读写锁释放一层）
  cond_wait                          在条件变量上等待，进入前必须持有锁，等待期间释放锁
  cond_wait_end                      被唤醒 / 超时退出等待，开始重新竞争锁
  cond_notify                        唤醒一个等待者
  cond_notify_all                    唤醒全部等待者
  gen_start / gen_end                采集代次边界（同一地址跨代次视为不同锁）

到达顺序由 ingest 顺序（arrival_seq）保留，与时间戳顺序可能不一致。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional


EVENT_KINDS = {
    "thread_start",
    "thread_end",
    "acquire_req",
    "acquire_ok",
    "acquire_timeout",
    "acquire_cancel",
    "release",
    "cond_wait",
    "cond_wait_end",
    "cond_notify",
    "cond_notify_all",
    "gen_start",
    "gen_end",
}

REQ_KINDS = {"acquire_req"}
OK_KINDS = {"acquire_ok"}
CANCEL_KINDS = {"acquire_timeout", "acquire_cancel"}
RETRY_KINDS = {"cond_wait_end"}


@dataclass
class Event:
    id: int
    kind: str
    ts: float
    thread: Optional[str] = None
    lock: Optional[str] = None
    cv: Optional[str] = None
    gen: int = 0
    arrival_seq: int = 0
    stack: list[str] = field(default_factory=list)
    timeout_ms: Optional[float] = None
    mode: Optional[str] = None  # R / W（读写锁）；升级请求 mode=W
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def lock_key(self) -> str:
        return f"{self.gen}:{self.lock}"

    @property
    def cv_key(self) -> str:
        return f"{self.gen}:{self.cv}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_mode(mode: Any) -> Optional[str]:
    if mode is None:
        return None
    m = str(mode).upper()
    return "W" if m in {"W", "X", "WRITE", "EXCLUSIVE"} else "R"


def event_from_dict(row: dict[str, Any]) -> Event:
    return Event(
        id=row["id"],
        kind=row["kind"],
        ts=float(row["ts"]),
        thread=row.get("thread"),
        lock=row.get("lock"),
        cv=row.get("cv"),
        gen=int(row.get("gen") or 0),
        arrival_seq=int(row.get("arrival_seq") or 0),
        stack=list(row.get("stack") or []),
        timeout_ms=row.get("timeout_ms"),
        mode=normalize_mode(row.get("mode")),
        extra=dict(row.get("extra") or {}),
    )


def ordered_for_replay(events: list[Event]) -> list[Event]:
    """按采集代次保留原始顺序：先按代次首次出现顺序，代次内按 (ts, arrival_seq)。

    晚到（arrival_seq 比已见最大 seq 大却插到较小 ts）的事件仍参与重放，
    由重放器标记 reorder 置信信息，绝不丢弃。
    """
    gen_order: dict[int, int] = {}
    for ev in events:
        gen_order.setdefault(ev.gen, len(gen_order))
    return sorted(events, key=lambda e: (gen_order[e.gen], e.ts, e.arrival_seq))
