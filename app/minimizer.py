"""把长轨迹缩减成仍能复现目标环的最小事件片段。

缩减单位是“配对组”而非单个事件，保证：
- acquire_request/acquire_ok/release 的配对不被拆散（可重入按 LIFO 配对）；
- acquire_fail（超时取消）与它的 request 成组；
- cond_wait/cond_wake 成组，cond_notify 并入被通知的 wait 组；
- thread_start/thread_end 作为独立边界组。
"""
from __future__ import annotations

from .engine import Engine, detect_cycles
from .reconstruction import ordered_events


def _group_events(events: list[dict]) -> dict[str, int]:
    events = ordered_events(events)
    gids: dict[str, int] = {}
    next_gid = 0

    def new_gid() -> int:
        nonlocal next_gid
        gid = next_gid
        next_gid += 1
        return gid

    # 线程 -> 锁 -> 待配对的持有栈 [(gid)]
    held_stack: dict[str, dict[str, list]] = {}
    # 线程 -> cond -> 当前 cond_wait 组
    wait_gid: dict[str, dict[str, int]] = {}
    cond_wait_gids: dict[str, list[int]] = {}

    for ev in events:
        t, thr = ev["type"], ev["thread"]
        gid: int | None = None
        if t == "acquire_request":
            gid = new_gid()
            held_stack.setdefault(thr, {}).setdefault(ev["lock"], []).append(gid)
        elif t in {"acquire_ok", "acquire_fail"}:
            stack = held_stack.get(thr, {}).get(ev["lock"], [])
            gid = stack.pop() if stack else new_gid()
            if t == "acquire_fail":
                pass  # 失败请求整组就是 request+fail
            else:
                # 获取成功：组继续延伸，直到 release
                held_stack.setdefault(thr, {}).setdefault(ev["lock"], []).append(gid)
        elif t == "release":
            stack = held_stack.get(thr, {}).get(ev["lock"], [])
            gid = stack.pop() if stack else new_gid()
        elif t == "cond_wait":
            gid = new_gid()
            wait_gid.setdefault(thr, {})[ev["cond"]] = gid
            cond_wait_gids.setdefault(ev["cond"], []).append(gid)
        elif t == "cond_wake":
            waiting = wait_gid.get(thr)
            if ev.get("cond") and waiting and ev["cond"] in waiting:
                gid = waiting.pop(ev["cond"])
            elif waiting:
                _, gid = waiting.popitem()
            else:
                gid = new_gid()
        elif t == "cond_notify":
            gid = new_gid()
            wg = cond_wait_gids.get(ev["cond"], [])
            if wg:
                gid = wg[-1]
        else:  # thread_start / thread_end
            gid = new_gid()
        gids[ev["id"]] = gid
    return gids


def _cycle_signatures(evs: list[dict]) -> set[str]:
    eng = Engine(evs).run()
    return {c["signature"] for c in
            detect_cycles([e.to_dict() for e in eng.edges.values()])}


def minimize(events: list[dict],
             target_signature: str | None = None) -> dict:
    events = ordered_events(events)
    full_sigs = _cycle_signatures(events)
    if not full_sigs:
        return {"events": [], "cycles": [], "event_ids": [],
                "original_count": len(events), "reduced_count": 0}
    target = target_signature or sorted(full_sigs)[0]

    gids = _group_events(events)
    chosen = {ev["id"] for ev in events}

    for gid in sorted(set(gids.values())):
        member_ids = {eid for eid, g in gids.items() if g == gid}
        trial = chosen - member_ids
        evs = [e for e in events if e["id"] in trial]
        if target in _cycle_signatures(evs):
            chosen = trial

    minimal = [e for e in events if e["id"] in chosen]
    eng = Engine(minimal).run()
    cycles = [c for c in detect_cycles(
        [e.to_dict() for e in eng.edges.values()])
        if c["signature"] == target]
    return {
        "target": target,
        "events": minimal,
        "event_ids": [e["id"] for e in minimal],
        "cycles": cycles,
        "original_count": len(events),
        "reduced_count": len(minimal),
    }
