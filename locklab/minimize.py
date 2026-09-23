"""轨迹缩减：把长事件轨迹缩减为仍复现同一环的最小片段。

约束：缩减必须保持每个线程的 acquire/release 配对与 condition 语义
（cond_wait 之后必须先重新 acquire 同一把锁才能再 release；
thread_end 必须是该线程最后一个事件）。
"""
from __future__ import annotations

from .engine import replay


def pairing_valid(events):
    """逐线程检查配对与 condition 语义是否合法。"""
    by_thread = {}
    for e in sorted(events, key=lambda x: (x.get("epoch", 0), x.get("seq", 0))):
        by_thread.setdefault(e.get("thread"), []).append(e)
    for thread, evs in by_thread.items():
        if thread is None:
            continue
        held = {}       # lock -> count
        cond_open = {}  # lock -> True（等待重新竞争）
        for i, e in enumerate(evs):
            et = e["type"]
            lock = e.get("lock")
            if et == "thread_end" and i != len(evs) - 1:
                return False
            if et == "acquire":
                if cond_open.pop(lock, None) is not None:
                    pass  # 重新竞争成功，闭合 condition 语义
                held[lock] = held.get(lock, 0) + 1
            elif et == "release":
                if cond_open.get(lock):
                    return False  # cond_wait 后未重新 acquire 就 release
                if held.get(lock, 0) <= 0:
                    return False  # release 无配对 acquire
                held[lock] -= 1
            elif et == "cond_wait":
                if held.get(lock, 0) <= 0 or cond_open.get(lock):
                    return False
                cond_open[lock] = True
                held[lock] = 0
        # 轨迹末尾允许未闭合的持有（缺 release 场景）与仍在 cond 等待的线程
    return True


def minimize(events, target_threads, declarations=None):
    """ddmin 风格贪心缩减：在保持配对合法且仍复现目标环的前提下删除事件。"""
    target = set(target_threads)

    def reproduces(evs):
        snap = replay(evs, declarations=declarations)
        return any(target <= set(c["threads"]) for c in snap["cycles"])

    def ok(evs):
        return pairing_valid(evs) and reproduces(evs)

    current = list(events)
    if not ok(current):
        raise ValueError("初始轨迹未复现目标环或配对不合法")

    # 阶段一：按块递减删除
    granularity = 2
    while len(current) >= 2:
        chunk = max(1, len(current) // granularity)
        removed = False
        i = 0
        while i < len(current):
            candidate = current[:i] + current[i + chunk:]
            if candidate and ok(candidate):
                current = candidate
                removed = True
            else:
                i += chunk
        if not removed:
            if chunk == 1:
                break
            granularity = min(len(current), granularity * 2)

    # 阶段二：逐个事件再扫一遍
    changed = True
    while changed:
        changed = False
        for e in list(current):
            candidate = [x for x in current if x is not e]
            if candidate and ok(candidate):
                current = candidate
                changed = True
    return current
