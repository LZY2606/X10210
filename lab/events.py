"""事件模型与校验。

系统导入的原始事件种类：

- ``request``  : 线程请求获取锁 (mode: X/S, 可带 timeout)
- ``acq``      : 获取成功
- ``release``  : 释放
- ``start``    : 线程开始
- ``end``      : 线程结束
- ``cwait``    : condition wait（暂时释放锁，被 notify 后重新竞争）
- ``notify``   : signal 一个等待者
- ``notify_all``: broadcast 全部等待者
- ``ctimeout`` : condition wait 超时（重新竞争锁）
- ``timeout``  : 锁请求超时，撤销等待
- ``restart``  : 进程重启（相同地址进入新代次）

每条事件属于一个采集代次 ``gen``；相同锁地址在不同 ``gen`` 中是不同的锁实例。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

VALID_KINDS = {
    "request", "acq", "release", "start", "end",
    "cwait", "notify", "notify_all", "ctimeout", "timeout", "restart",
}
LOCK_EVENTS = {"request", "acq", "release", "timeout"}
MODE_X = "X"
MODE_S = "S"


def norm_mode(value: Any) -> str:
    if value is None:
        return MODE_X
    v = str(value).strip().lower()
    if v in ("x", "exclusive", "write", "w", "lock"):
        return MODE_X
    if v in ("s", "shared", "read", "r"):
        return MODE_S
    raise ValueError(f"未知锁模式: {value!r}")


@dataclass
class Event:
    kind: str
    seq: int
    ts: float
    thread: str
    gen: int = 0
    lock: Optional[str] = None
    cond: Optional[str] = None
    mode: str = MODE_X
    timeout: bool = False
    stack: List[str] = field(default_factory=list)
    eid: Optional[str] = None
    synthetic: bool = False
    note: Optional[str] = None
    arrival: int = 0  # 到达序号，用于识别乱序到达

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d

    def label(self) -> str:
        if self.kind in ("start", "end"):
            return f"{self.thread} {self.kind}"
        if self.kind in ("notify", "notify_all", "cwait", "ctimeout"):
            return f"{self.thread} {self.kind} {self.cond or ''}".strip()
        m = self.mode if self.mode == MODE_S else ""
        return f"{self.thread} {self.kind}{m} {self.lock or ''}".strip()


def make_event(rec: Dict[str, Any], arrival: int = 0) -> Event:
    """把一条外部 JSON 记录转成 :class:`Event`，非法时抛 ValueError。"""
    if not isinstance(rec, dict):
        raise ValueError("事件必须是 JSON 对象")
    kind = rec.get("kind")
    if kind not in VALID_KINDS:
        raise ValueError(f"未知事件种类: {kind!r}（合法值: {sorted(VALID_KINDS)}）")
    if "ts" not in rec:
        raise ValueError("事件缺少 ts")
    try:
        ts = float(rec["ts"])
    except (TypeError, ValueError):
        raise ValueError(f"ts 必须是数字: {rec.get('ts')!r}")
    seq = int(rec.get("seq", 0))
    thread = str(rec.get("thread", "?"))
    gen = int(rec.get("gen", 0))
    lock = rec.get("lock")
    if lock is not None:
        lock = str(lock)
    cond = rec.get("cond")
    if cond is not None:
        cond = str(cond)
    mode = norm_mode(rec.get("mode"))
    timeout = bool(rec.get("timeout", False))
    stack = rec.get("stack") or []
    if not isinstance(stack, list):
        raise ValueError("stack 必须是字符串数组")
    stack = [str(s) for s in stack]
    eid = rec.get("id") or rec.get("eid")
    if eid is not None:
        eid = str(eid)
    note = rec.get("note")
    if kind in LOCK_EVENTS and lock is None:
        raise ValueError(f"{kind} 事件缺少 lock")
    if kind in ("cwait", "ctimeout") and (cond is None or lock is None):
        raise ValueError(f"{kind} 事件需要 lock 与 cond")
    return Event(
        kind=kind, seq=seq, ts=ts, thread=thread, gen=gen,
        lock=lock, cond=cond, mode=mode, timeout=timeout,
        stack=stack, eid=eid, synthetic=bool(rec.get("synthetic", False)),
        note=note, arrival=arrival,
    )


def sort_key(ev: Event):
    """按采集代次保留原始顺序：代次 → 时间戳 → 原始序号 → 到达序号。"""
    return (ev.gen, ev.ts, ev.seq, ev.arrival)


def key_of(ev: Event):
    """线程配对使用的稳定标识。"""
    return ev.eid or f"a{ev.arrival}"
