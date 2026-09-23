"""轨迹缩减：把长轨迹缩减成仍复现同一环的最小事件片段。

缩减单位保持每个线程的配对与 condition 语义：
acquire_success 与其配对的 release 构成同一单元，同删同留；
cond_wait 与其重新竞争成功的 acquire_success（及配对 release）构成同一单元；
其余事件为独立单元。在单元集合上运行 ddmin，谓词为重建后仍出现目标环签名。
"""
from engine import replay, build_graphs, cycle_reports


def _signature(cycle):
    return tuple(tuple(x) for x in cycle["signature"])


def build_units(events):
    units = []
    hold_stack = {}
    pending_cond = {}
    for i, ev in enumerate(events):
        kind = ev.get("kind")
        key = (ev.get("thread"), ev.get("lock"))
        if kind == "acquire_success":
            if key in pending_cond:
                w = pending_cond.pop(key)
                units.append([w, i])
                hold_stack.setdefault(key, []).append((i, len(units) - 1))
            else:
                units.append([i])
                hold_stack.setdefault(key, []).append((i, len(units) - 1))
        elif kind == "release":
            st = hold_stack.get(key)
            if st:
                _, ui = st.pop()
                units[ui].append(i)
            else:
                units.append([i])
        elif kind == "cond_wait":
            pending_cond[key] = i
        else:
            units.append([i])
    for w in pending_cond.values():
        units.append([w])
    return units


def _ddmin(units, test):
    units = list(units)
    n = 2
    while len(units) >= 2:
        chunk = max(1, -(-len(units) // n))
        reduced = False
        i = 0
        while i < len(units):
            cand = units[:i] + units[i + chunk:]
            if cand and test(cand):
                units = cand
                n = max(n - 1, 2)
                reduced = True
                break
            i += chunk
        if not reduced:
            if n >= len(units):
                break
            n = min(len(units), n * 2)
    changed = True
    while changed:
        changed = False
        for u in list(units):
            cand = [x for x in units if x is not u]
            if cand and test(cand):
                units = cand
                changed = True
    return units


def minimize_trace(events, declared, target_signature):
    units = build_units(events)

    def test(unit_list):
        idx = sorted(i for u in unit_list for i in u)
        kept = [events[i] for i in idx]
        st = replay(kept, declared)
        cycles = cycle_reports(st, build_graphs(st))
        return any(_signature(c) == target_signature for c in cycles)

    kept_units = _ddmin(units, test)
    idx = sorted(i for u in kept_units for i in u)
    return [events[i] for i in idx]
