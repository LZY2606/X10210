"""轨迹缩减：把长轨迹缩减成仍复现同一环的最小事件片段。

约束：
- 不引入新的异常类型（保持每个线程的 acquire/release 配对与 condition 语义）。
- 目标环的签名（等待边集合）在缩减后的重放终点仍出现。
"""
from collections import Counter

from engine import BranchEngine, detect_cycles


def _anomaly_types(engine):
    return Counter(a["type"] for a in engine.state["anomalies"])


def _cycle_signatures(engine):
    graph, _ = engine.graph_at()
    return {tuple(c["signature"]) for c in detect_cycles(graph)}


def minimize(events, lock_types, target_signatures):
    """events: 按时序排列（通常取 ts <= T 的部分）。返回最小子序列。"""
    lock_types = lock_types or {}
    base = BranchEngine(lock_types)
    base.set_events(events)
    base_anoms = _anomaly_types(base)
    targets = {tuple(s) for s in target_signatures}

    def ok(candidate):
        eng = BranchEngine(lock_types)
        eng.set_events(candidate)
        # 不允许出现原轨迹没有的异常类型（保持配对与 condition 语义）
        for atype, n in (_anomaly_types(eng) - base_anoms).items():
            if n > 0:
                return False
        return targets <= _cycle_signatures(eng)

    current = list(events)
    if not ok(current):
        return current
    changed = True
    while changed:
        changed = False
        for ev in list(current):
            trial = [e for e in current if e is not ev]
            if ok(trial):
                current = trial
                changed = True
    return current
