"""轨迹缩减：把长轨迹缩减成仍复现同一环的最小事件片段，保持线程配对与 condition 语义。"""
from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from .engine import Engine


def _build_units(events: List[dict]) -> List[Set[int]]:
    """把必须同进同出的事件编组：request/success/release 配对、cond wait 配对。"""
    by_id = {e["id"]: e for e in events}
    paired: Dict[int, Set[int]] = {}

    def group(ids):
        ids = [i for i in ids if i is not None and i in by_id]
        if not ids:
            return
        unit = set(ids)
        for i in ids:
            if i in paired:
                unit |= paired[i]
        for i in unit:
            paired[i] = unit

    pending_req: Dict[Tuple, List[int]] = {}
    pending_success: Dict[Tuple, List[int]] = {}
    pending_cond: Dict[str, List[int]] = {}
    for e in events:
        key = (e.get("thread"), e.get("generation"), e.get("lock"))
        if e["type"] == "acquire_request":
            pending_req.setdefault(key, []).append(e["id"])
        elif e["type"] == "acquire_success":
            req = pending_req.get(key, [])
            rid = req.pop(0) if req else None
            group([rid, e["id"]])
            pending_success.setdefault(key, []).append(e["id"])
        elif e["type"] == "release":
            suc = pending_success.get(key, [])
            sid = suc.pop(0) if suc else None
            if sid is not None:
                group([sid, e["id"]])
        elif e["type"] == "cond_wait_start":
            pending_cond.setdefault(e.get("thread"), []).append(e["id"])
        elif e["type"] == "cond_wait_end":
            starts = pending_cond.get(e.get("thread"), [])
            sid = starts.pop(0) if starts else None
            if sid is not None:
                group([sid, e["id"]])
    units: List[Set[int]] = []
    seen: Set[int] = set()
    for e in events:
        if e["id"] in seen:
            continue
        unit = paired.get(e["id"], {e["id"]})
        units.append(set(unit))
        seen |= unit
    return units


def minimize(events: List[dict], lock_types: dict, t: float,
             target_signature: tuple) -> List[dict]:
    """贪心 delta-debugging：逐个移除事件组，只要目标环仍复现就保留移除。"""
    target = tuple(sorted(target_signature))
    units = _build_units(events)

    def reproduces(removed: Set[int]) -> bool:
        kept = [e for e in events if e["id"] not in removed]
        engine = Engine(kept, lock_types)
        for c in engine.cycles_at(t):
            if tuple(sorted(c["signature"])) == target:
                return True
        return False

    removed: Set[int] = set()
    changed = True
    while changed:
        changed = False
        for unit in units:
            if unit & removed:
                continue
            if reproduces(removed | unit):
                removed |= unit
                changed = True
    return [e for e in events if e["id"] not in removed]
