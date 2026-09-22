"""把长轨迹缩减成仍复现同一环的最小事件片段。

缩减保证：

* 每个线程的 request/acquired/release、cond_wait/notify、timeout 配对保持完整
  （利用重放器的 ``pairs`` 前置关系做闭包）；
* condition 语义需要的 cond_wait / notify 同时保留；
* 环中线程的 thread_start 与其代次的 restart 保留；
* 用 ddmin 做保序子集缩减，并校验目标环签名仍然存在。
"""
from __future__ import annotations

from collections import deque
from typing import Any, Dict, List, Optional, Set

from .replay import Replayer
from .timeline import prepare
from .timeline import Timeline


def _closure(ordered: List[Any], replayer: Replayer, cycle: Dict[str, Any]) -> Set[int]:
    seqs: Set[int] = set()
    for step in cycle["steps"]:
        wait_ev = step.get("wait_evidence") or {}
        hold_ev = step.get("hold_evidence") or {}
        for key in ("request_seq", "acquired_seq", "wait_seq"):
            if wait_ev.get(key):
                seqs.add(wait_ev[key])
            if hold_ev.get(key):
                seqs.add(hold_ev[key])

    # BFS 展开配对关系
    queue = deque(seqs)
    while queue:
        seq = queue.popleft()
        for prereq in replayer.pairs.get(seq, []):
            if prereq not in seqs:
                seqs.add(prereq)
                queue.append(prereq)

    # 仅保留环中线程自身的开始事件，以及它们所在代次的 restart
    thread_gens: Dict[str, Set[str]] = {}
    for event in ordered:
        if event.thread in cycle["threads"]:
            thread_gens.setdefault(event.thread, set()).add(event.gen)
    target_gens = {g for gens in thread_gens.values() for g in gens}
    for event in ordered:
        if event.type == "restart" and event.gen in target_gens:
            seqs.add(event.seq)
        elif (
            event.type == "thread_start"
            and event.thread in cycle["threads"]
            and event.gen in thread_gens.get(event.thread, set())
        ):
            seqs.add(event.seq)
    return seqs


def _cycle_signatures(snapshot: Dict[str, Any]) -> Set[str]:
    return {c["signature"] for c in snapshot.get("cycles", [])}


def _replay_subset(
    ordered: List[Any],
    lock_types: Dict[str, str],
    keep: Set[int],
    upto: Optional[int] = None,
) -> Replayer:
    replayer = Replayer(lock_types)
    scope = ordered if upto is None else ordered[:upto]
    selected, _ = prepare([e for e in scope if e.seq in keep])
    for event in selected:
        if event.seq in keep:
            replayer.feed(event)
    return replayer


def minimize(
    timeline: Timeline,
    cycle_signature: Optional[str] = None,
) -> Dict[str, Any]:
    moments = timeline.cycle_moments()
    if not moments:
        return {
            "found": False,
            "message": "整条时间轴上都没有检测到环",
            "events": [],
            "original_count": len(timeline.ordered),
            "reduced_count": 0,
        }
    chosen_moment = moments[0]
    if cycle_signature:
        chosen_moment = next(
            (m for m in moments if m["signature"] == cycle_signature), moments[0]
        )
    chosen = chosen_moment["cycle"]
    target = chosen["signature"]
    moment_index = chosen_moment["first_index"]
    final = timeline._state_at(moment_index)
    cycles = [m["cycle"] for m in moments]
    if not cycles:
        return {
            "found": False,
            "message": "当前轨迹不存在环，无法最小化",
            "events": [],
            "original_count": len(timeline.ordered),
            "reduced_count": 0,
        }

    ordered = list(timeline.ordered[:moment_index])
    closure = _closure(ordered, final, chosen)

    def still_reproduces(keep: Set[int]) -> bool:
        snap = _replay_subset(
            timeline.ordered, timeline.lock_types, keep, upto=moment_index
        ).snapshot()
        return target in _cycle_signatures(snap)

    # 闭包事件受保护；同时“配对簇”必须整体保留/删除，避免拆散 request/acquired
    # 或 cond_wait/notify。用并查集按 pairs 合并事件。
    parent = {e.seq: e.seq for e in ordered}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for seq, prereqs in final.pairs.items():
        for prereq in prereqs:
            if seq in parent or prereq in parent:
                union(seq, prereq)
    clusters: Dict[int, List[int]] = {}
    for seq in parent:
        clusters.setdefault(find(seq), []).append(seq)
    # 触及闭包的簇整体受保护
    protected_seqs: Set[int] = set()
    removable_clusters: List[List[int]] = []
    for members in clusters.values():
        if any(m in closure for m in members):
            protected_seqs.update(members)
        else:
            removable_clusters.append(sorted(members))
    # 不在任何配对关系中的孤立事件也可删
    paired = set(parent)
    for event in ordered:
        if event.seq not in paired and event.seq not in closure:
            removable_clusters.append([event.seq])

    all_seqs = {e.seq for e in ordered}
    removable = removable_clusters
    n = 2
    while removable and n <= len(removable):
        step = (len(removable) + n - 1) // n
        reduced = False
        start = 0
        while start < len(removable):
            chunk_clusters = removable[start : start + step]
            chunk = {s for cluster in chunk_clusters for s in cluster}
            trial = all_seqs - chunk
            if still_reproduces(trial):
                removable = [
                    cluster
                    for cluster in removable
                    if not any(s in chunk for s in cluster)
                ]
                all_seqs = trial
                reduced = True
                n = max(n - 1, 2)
                break
            start += step
        if not reduced:
            n *= 2

    keep = all_seqs
    if not still_reproduces(keep):
        keep = {e.seq for e in ordered}

    reduced_events = [e for e in ordered if e.seq in keep]
    verify = _replay_subset(
        timeline.ordered, timeline.lock_types,
        {e.seq for e in reduced_events}, upto=moment_index,
    ).snapshot()
    return {
        "found": True,
        "target_signature": target,
        "moment_index": moment_index,
        "moment_ts": chosen_moment["ts"],
        "cycle": chosen,
        "closure_size": len(closure),
        "original_count": len(ordered),
        "reduced_count": len(reduced_events),
        "reproduced": target in _cycle_signatures(verify),
        "events": [e.to_dict() for e in reduced_events],
        "removed_sequences": [e.seq for e in ordered if e.seq not in keep],
    }
