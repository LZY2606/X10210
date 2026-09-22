"""代次归并、乱序标记与带检查点的时间轴重放。"""
from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Tuple

from .model import Event, clone_event, normalize_event
from .replay import Replayer


def assign_generations(events: List[Event]) -> List[Event]:
    """按原始到达顺序计算“有效代次”，并标记时间戳乱序。

    规则：``restart`` 强制开启新代；显式 gen 标签变化也开启新代；
    同代内若新到达事件的 ts 小于此前同代任意 ts，打 ``late`` 标记。
    相同锁地址跨代自然成为不同锁节点（gen 前缀参与图节点标识）。
    """
    result: List[Event] = []
    counter = -1
    current_effective = "g0"
    current_raw = None
    seen_raw: Dict[str, int] = {}
    max_ts: Dict[str, float] = {}

    for event in events:
        if event.type == "restart":
            counter += 1
            current_effective = "g%d" % counter
            # 哨兵：下一条事件无论什么原始标签都视为本代，不再次换代
            current_raw = None
            result.append(clone_event(event, gen=current_effective, late=False))
            continue

        raw_gen = event.gen or "g0"
        if current_raw is not None and raw_gen != current_raw:
            if raw_gen not in seen_raw:
                counter += 1
                seen_raw[raw_gen] = counter
            current_raw = raw_gen
            current_effective = "g%d" % seen_raw[raw_gen]
        else:
            if current_raw is None and raw_gen not in seen_raw:
                counter += 1
                seen_raw[raw_gen] = counter
                current_effective = "g%d" % seen_raw[raw_gen]
            current_raw = raw_gen

        late = False
        ts = event.ts
        if ts is not None:
            if current_effective in max_ts and ts < max_ts[current_effective]:
                late = True
            else:
                max_ts[current_effective] = max(
                    ts, max_ts.get(current_effective, ts)
                )
        result.append(clone_event(event, gen=current_effective, late=late))
    return result


def play_order(events: List[Event]) -> List[Event]:
    """展示/分析顺序：先按代次，再按 ts（缺失排最后），平局按到达 seq。"""
    gen_order: Dict[str, int] = {}
    for event in sorted(events, key=lambda e: e.seq):
        if event.gen not in gen_order:
            gen_order[event.gen] = len(gen_order)
    return sorted(
        events,
        key=lambda e: (
            gen_order.get(e.gen, 0),
            e.ts is None,
            e.ts if e.ts is not None else 0.0,
            e.seq,
        ),
    )


def prepare(events: List[Event]) -> Tuple[List[Event], List[Event]]:
    """返回 (按代次+时间排序的分析序列, 原始到达序列)。"""
    assigned = assign_generations(events)
    return play_order(assigned), sorted(assigned, key=lambda e: e.seq)


class Timeline:
    """支持任意前缀快照与流式追加的时间轴。"""

    CHECKPOINT_EVERY = 32

    def __init__(
        self,
        raw_events: Optional[List[Event]] = None,
        lock_types: Optional[Dict[str, str]] = None,
    ) -> None:
        self.lock_types = dict(lock_types or {})
        self.raw: List[Event] = list(raw_events or [])
        self.ordered: List[Event] = []
        self._checkpoints: Dict[int, Replayer] = {}
        self._rebuild()

    def _rebuild(self) -> None:
        self.ordered, self.raw = prepare(self.raw)
        self._checkpoints = {}

    # ---- 查询 -----------------------------------------------------------

    def _state_at(self, index: int) -> Replayer:
        """重放 ordered[:index]（index 为已处理条数）。"""
        index = max(0, min(index, len(self.ordered)))
        cp_index = (index // self.CHECKPOINT_EVERY) * self.CHECKPOINT_EVERY
        if cp_index in self._checkpoints:
            replayer = copy.deepcopy(self._checkpoints[cp_index])
            start = cp_index
        else:
            replayer = Replayer(self.lock_types)
            start = 0
            if cp_index > 0:
                for event in self.ordered[:cp_index]:
                    replayer.feed(event)
                self._checkpoints[cp_index] = copy.deepcopy(replayer)
                start = cp_index
        for event in self.ordered[start:index]:
            replayer.feed(event)
        return replayer

    def snapshot(self, index: Optional[int] = None) -> Dict[str, Any]:
        if index is None:
            index = len(self.ordered)
        replayer = self._state_at(index)
        snap = replayer.snapshot()
        snap["index"] = index
        snap["total"] = len(self.ordered)
        return snap

    def final(self) -> Replayer:
        return self._state_at(len(self.ordered))

    def cycle_moments(self) -> List[Dict[str, Any]]:
        """扫描所有时间点，返回每次环首次出现/变化的时刻。"""
        replayer = Replayer(self.lock_types)
        seen: Dict[str, int] = {}
        moments: List[Dict[str, Any]] = []
        active: Dict[str, Dict[str, Any]] = {}
        for pos, event in enumerate(self.ordered, start=1):
            replayer.feed(event)
            snap = replayer.snapshot()
            current = {c["signature"]: c for c in snap["cycles"]}
            for sig, cycle in current.items():
                if sig not in active:
                    moments.append(
                        {
                            "signature": sig,
                            "first_index": pos,
                            "seq": event.seq,
                            "ts": event.ts,
                            "gen": event.gen,
                            "cycle": cycle,
                        }
                    )
            active = current
        return moments

    def state_at_cycle(self, signature: str) -> Dict[str, Any]:
        for moment in self.cycle_moments():
            if moment["signature"] == signature:
                snap = self.snapshot(moment["first_index"])
                snap["moment"] = {
                    "index": moment["first_index"],
                    "seq": moment["seq"],
                    "ts": moment["ts"],
                    "gen": moment["gen"],
                }
                return snap
        return self.snapshot(len(self.ordered))

    def swimlanes(self) -> Dict[str, Any]:
        lanes: Dict[str, List[Dict[str, Any]]] = {}
        for pos, event in enumerate(self.ordered):
            lanes.setdefault(event.thread or "system", []).append(
                {
                    "pos": pos,
                    "seq": event.seq,
                    "type": event.type,
                    "lock": event.lock,
                    "cond": event.cond,
                    "mode": event.mode,
                    "ts": event.ts,
                    "gen": event.gen,
                    "late": event.late,
                }
            )
        return {
            "generations": sorted({e.gen for e in self.ordered}),
            "lanes": lanes,
        }

    # ---- 流式追加 -------------------------------------------------------

    def append(self, new_events: List[Event]) -> Dict[str, Any]:
        """追加按到达顺序的新事件，仅丢弃受影响检查点。"""
        old_total = len(self.ordered)
        self.raw.extend(new_events)
        self.ordered, self.raw = prepare(self.raw)

        affected_from = len(self.ordered)
        late_seqs = [e.seq for e in self.ordered if e.late]
        if late_seqs:
            first_late_pos = min(
                pos for pos, e in enumerate(self.ordered) if e.late
            )
            affected_from = min(affected_from, first_late_pos)

        # 丢弃位于受影响位置之后的检查点
        for cp in [k for k in self._checkpoints if k >= affected_from]:
            del self._checkpoints[cp]

        return {
            "added": len(new_events),
            "total": len(self.ordered),
            "previous_total": old_total,
            "affected_from": affected_from,
            "late_seqs": late_seqs,
        }
