"""把长轨迹缩减成仍复现同一环的最小事件片段（ddmin）。

谓词要求：目标环仍然复现，且不引入原始轨迹中不存在的异常类别，
从而保持每个线程的 acquire/release 配对与 condition 语义。
"""
from __future__ import annotations

from .engine import Runner


def cycle_key(cycle):
    if cycle["kind"] == "deadlock":
        return ("deadlock", tuple(sorted(cycle["threads"])), tuple(sorted(cycle["locks"])))
    return ("near_miss", tuple(sorted(cycle["locks"])))


def analyze(events, declared):
    runner = Runner(declared)
    runner.rebuild(events)
    graph, _ = runner.graph_at()
    keys = {cycle_key(c) for c in graph["cycles"]}
    keys |= {cycle_key(c) for c in graph["near_misses"]}
    anomaly_kinds = {a["kind"] for a in graph["anomalies"]}
    return keys, anomaly_kinds, graph


def minimize(events, declared, target):
    base_keys, base_anoms, _ = analyze(events, declared)
    if target not in base_keys:
        return None

    def pred(evs):
        if not evs:
            return False
        keys, anoms, _ = analyze(evs, declared)
        return target in keys and anoms <= base_anoms

    current = list(events)
    n = 2
    while len(current) >= 2:
        chunk = max(1, len(current) // n)
        reduced = False
        i = 0
        while i < len(current):
            cand = current[:i] + current[i + chunk:]
            if pred(cand):
                current = cand
                n = max(2, n - 1)
                reduced = True
                break
            i += chunk
        if not reduced:
            if n >= len(current):
                break
            n = min(len(current), n * 2)
    return current
