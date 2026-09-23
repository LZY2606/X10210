"""锁序重放引擎：从有序事件流重建持有图与等待图。

设计原则：
- 绝不凭空补 ``release``；缺 release 只产生 "missing_release" 软异常并保持持有边开放。
- 每条持有边/等待边都带产生它的事件（acquire 事件与等待请求事件），可回溯。
- 锁类型未声明时按用法推断（出现共享模式 => rw，否则 mutex）；推断锁上的边标为 inferred。
- 乱序到达（到达序号与重放顺序不一致）会把相关边标为 tentative。
- 每个采集代次独立重建；``restart`` 开启新代次，相同锁地址互不相干。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .events import Event, MODE_S, MODE_X, sort_key, key_of

MUTEX = "mutex"
RW = "rw"

# 置信度：firm（确定） / inferred（锁类型靠推断） / tentative（受乱序或缺失影响）
FIRM, INFERRED, TENTATIVE = "firm", "inferred", "tentative"


def weaker(a: str, b: str) -> str:
    order = {FIRM: 0, INFERRED: 1, TENTATIVE: 2}
    return a if order[a] >= order[b] else b


@dataclass
class QItem:
    """锁等待队列中的一个请求（线程在这把锁上排队）。"""
    thread: str
    mode: str
    req_event: Event
    timeout: bool = False
    edge_id: Optional[str] = None  # 当前开放等待边 id


@dataclass
class Susp:
    """condition wait 挂起记录：cwait 后暂时释放锁。"""
    thread: str
    lock: str
    cond: str
    mode: str
    depth: int
    event: Event
    notified: bool = False


@dataclass
class HEdge:
    id: str
    thread: str
    lock: str
    mode: str
    gen: int
    acq_event: Event
    rel_event: Optional[Event] = None
    depth_after: int = 1
    confidence: str = FIRM


@dataclass
class WEdge:
    id: str
    thread: str
    lock: str
    mode: str
    gen: int
    req_event: Event          # 产生等待的 request / ctimeout 事件
    kind: str = "holder"       # holder | queue | cond | self
    target_thread: Optional[str] = None
    target_lock: Optional[str] = None
    open_event: Optional[Event] = None   # 等待结束事件（acq/timeout/end）
    confidence: str = FIRM


@dataclass
class Anomaly:
    code: str
    severity: str              # hard（重建不合法） | soft（缺数据但仍可呈现）
    message: str
    event: Optional[Event] = None
    thread: Optional[str] = None
    lock: Optional[str] = None


@dataclass
class _LockState:
    type: str = MUTEX
    declared: bool = False
    # thread -> 模式栈 [(mode, acq_event), ...]
    holds: Dict[str, List[Tuple[str, Event]]] = field(default_factory=dict)
    queue: List[QItem] = field(default_factory=list)


@dataclass
class _State:
    gen: int
    alive: Dict[str, bool] = field(default_factory=dict)
    locks: Dict[str, _LockState] = field(default_factory=lambda: defaultdict(_LockState))
    conds: Dict[str, List[Susp]] = field(default_factory=lambda: defaultdict(list))
    # 正在等待锁的请求（含 condition 被唤醒后的重新竞争）：thread -> QItem
    pending: Dict[str, QItem] = field(default_factory=dict)
    susp: Dict[str, Susp] = field(default_factory=dict)  # thread -> condition 挂起


class Analysis:
    def __init__(self, events: List[Event], lock_types: Optional[Dict[str, str]] = None):
        # 按采集代次保留原始顺序
        self.events: List[Event] = sorted(events, key=sort_key)
        for i, ev in enumerate(self.events):
            ev.idx = i
        self.decl_types: Dict[str, str] = dict(lock_types or {})
        self.gen_boundaries: List[int] = []
        self.hedges: List[HEdge] = []
        self.wedges: List[WEdge] = []
        self.anoms: List[Anomaly] = []
        self._hc = 0
        self._wc = 0
        self._late_arrivals = self._detect_late()
        self._run()
