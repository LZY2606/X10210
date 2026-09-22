"""候选重建分支、最小事件片段缩减与流式重算。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .engine import Replay, order_events
from .models import (
    ALTERNATIVE_REPAIRS,
    Event,
)

MAX_CANDIDATES = 6


def enumerate_candidates(
    events: Sequence[Event],
    lock_types: Optional[Dict[str, str]] = None,
    limit: int = MAX_CANDIDATES,
) -> List[Dict[str, Any]]:
    """主重建 + 每个异常可选修复的并列合法重建。

    主重建始终保留（字面执行，异常全部可见）。其余候选只改变修复选择；
    任何候选都不凭空补 release。
    """
    base = Replay(events, lock_types=lock_types)
    base.run()
    candidates: List[Dict[str, Any]] = [
        {
            "name": "primary",
            "label": "主重建（字面执行）",
            "repair_choices": {},
            "report": base.final_report(),
            "legal": True,
        }
    ]

    seen: Set[frozenset] = {frozenset()}
    anomalies = list(base.anomalies)
    for anomaly in anomalies:
        for repair in ALTERNATIVE_REPAIRS.get(anomaly.code, []):
            key = anomaly.key
            choices = {key: repair}
            fingerprint = frozenset(choices.items())
            if fingerprint in seen:
                continue
            seen.add(fingerprint)
            replay = Replay(events, lock_types=lock_types, repair_choices=choices)
            replay.run()
            candidates.append(
                {
                    "name": f"cand-{len(candidates)}",
                    "label": f"候选：{anomaly.code} → {repair}",
                    "repair_choices": choices,
                    "report": replay.final_report(),
                    "legal": True,
                }
            )
            if len(candidates) >= limit:
                return candidates
    return candidates


def cycle_signature(cycle: Dict[str, Any]) -> Tuple[Any, ...]:
    if cycle["kind"] == "thread_wait_cycle":
        return (
            "thread",
            tuple(sorted(cycle["threads"])),
            tuple(sorted(cycle["locks"])),
        )
    return ("lock", tuple(sorted(cycle["threads"]),), tuple(sorted(cycle["locks"])))


def _has_target_cycle(
    events: Sequence[Event],
    lock_types: Optional[Dict[str, str]],
    repair_choices: Optional[Dict[str, str]],
    target: Tuple[Any, ...],
) -> Tuple[bool, List[Dict[str, Any]]]:
    replay = Replay(
        events, lock_types=lock_types, repair_choices=repair_choices
    )
    replay.run()
    final = replay.final_report()["final"]
    cycles = final["cycles"]
    signatures = {cycle_signature(cycle) for cycle in cycles}
    return target in signatures, cycles


def _pairing_groups(events: Sequence[Event]) -> Dict[int, int]:
    """给需要成对/语义联动的事件打上“同组 id”，缩减时同生共死。"""
    ordered = order_events(events)
    group: Dict[int, int] = {}
    next_group = 1
    by_id = {event.raw_id: event for event in ordered if event.raw_id is not None}

    def assign(ids: List[int]) -> None:
        nonlocal next_group
        valid = [item for item in ids if item is not None]
        for item in valid:
            group[item] = next_group
        next_group += 1

    # condition wait 与由它产生的重新竞争授予（证据链保存在 wait 事件的配对里）
    # 通过重放找配对关系，同时处理 acquire request -> granted/timeout/thread_end。
    replay = Replay(ordered, lock_types=None)
    replay.run()
    for _name, state in replay.threads.items():
        for wait in state.waits:
            ids = [wait.event_id]
            if wait.cond_wait_event_id:
                ids.append(wait.cond_wait_event_id)
            if wait.end_event_id:
                ids.append(wait.end_event_id)
            assign(ids)
    return group


def minimize_events(
    events: Sequence[Event],
    target_cycle: Optional[Dict[str, Any]] = None,
    lock_types: Optional[Dict[str, str]] = None,
    repair_choices: Optional[Dict[str, str]] = None,
) -> List[Event]:
    """ddmin 风格缩减：保留仍复现同一环的最小事件片段。

    约束：保持每个线程的事件配对（request/grant/timeout、thread_start/end、
    cond_wait 与其重新竞争）与 condition 语义（notify 只在 waiter 存在时保留）。
    """
    ordered = order_events(events)
    if target_cycle is None:
        replay = Replay(ordered, lock_types=lock_types, repair_choices=repair_choices)
        replay.run()
        cycles = replay.final_report()["final"]["cycles"]
        if not cycles:
            return ordered
        target_cycle = cycles[0]
    target = cycle_signature(target_cycle)
    evidence = set(target_cycle.get("evidence_event_ids", []))

    # 0) 先裁剪到证据事件的最小连续包围窗（保留泳道可读性的同时大副缩减）。
    if evidence:
        positions = [
            index
            for index, event in enumerate(ordered)
            if event.raw_id in evidence
        ]
        lo, hi = min(positions), max(positions)
        window = ordered[lo : hi + 1]
    else:
        window = list(ordered)

    groups = _pairing_groups(ordered)
    present_ids = {event.raw_id for event in window}

    def expand_group(event_ids: Set[Optional[int]]) -> Set[int]:
        result: Set[int] = set()
        for event_id in event_ids:
            if event_id is None:
                continue
            result.add(event_id)
            gid = groups.get(event_id)
            if gid is not None:
                for other_event_id, other_gid in groups.items():
                    if other_gid == gid:
                        result.add(other_event_id)
        return result

    required = expand_group(set(evidence))
    removable = [
        event
        for event in window
        if event.raw_id not in required
    ]

    def keeps_cycle(kept_events: List[Event]) -> bool:
        ok, _ = _has_target_cycle(kept_events, lock_types, repair_choices, target)
        return ok

    kept: Set[Optional[int]] = {event.raw_id for event in window}
    # 1) 逐个事件尝试删除（受配对组保护）。
    for event in removable:
        candidate_ids = set(kept)
        candidate_ids.discard(event.raw_id)
        gid = groups.get(event.raw_id)
        if gid is not None:
            candidate_ids = {
                event_id
                for event_id in candidate_ids
                if groups.get(event_id) != gid
            }
        trial = [item for item in window if item.raw_id in candidate_ids]
        if keeps_cycle(trial):
            kept = candidate_ids

    # 2) ddmin 分块再压一轮，处理成对事件之间的整段冗余区间。
    subset = [item for item in window if item.raw_id in kept]
    chunk_count = 2
    while chunk_count <= len(subset):
        size = max(1, len(subset) // chunk_count)
        changed = False
        for start in range(0, len(subset), size):
            block = subset[start : start + size]
            block_ids = {item.raw_id for item in block}
            trial = [item for item in subset if item.raw_id not in block_ids]
            if keeps_cycle(trial):
                subset = trial
                changed = True
                break
        if not changed:
            chunk_count *= 2
        elif len(subset) < 2:
            break

    return subset


def diff_branches(
    left: Dict[str, Any], right: Dict[str, Any]
) -> Dict[str, Any]:
    """两个分支的图与环差异。"""
    final_left = left["report"]["final"]
    final_right = right["report"]["final"]

    def edge_index(snapshot: Dict[str, Any]) -> Dict[str, Set[str]]:
        holds = {
            f"H:{edge['thread']}:{edge['lock']}:{edge['mode']}"
            for edge in snapshot["holds"]
        }
        waits = {
            f"W:{edge['thread']}:{edge['lock']}:{edge['mode']}"
            for edge in snapshot["waits"]
        }
        return {"holds": holds, "waits": waits}

    index_left = edge_index(final_left)
    index_right = edge_index(final_right)
    cycles_left = {cycle_signature(c) for c in final_left["cycles"]}
    cycles_right = {cycle_signature(c) for c in final_right["cycles"]}
    return {
        "left_name": left.get("name"),
        "right_name": right.get("name"),
        "holds_only_left": sorted(index_left["holds"] - index_right["holds"]),
        "holds_only_right": sorted(index_right["holds"] - index_left["holds"]),
        "waits_only_left": sorted(index_left["waits"] - index_right["waits"]),
        "waits_only_right": sorted(index_right["waits"] - index_left["waits"]),
        "cycles_only_left": [list(item) for item in sorted(cycles_left - cycles_right)],
        "cycles_only_right": [
            list(item) for item in sorted(cycles_right - cycles_left)
        ],
    }
