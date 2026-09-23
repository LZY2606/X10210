"""等待图（wait-for graph）边与有向环枚举。"""

from __future__ import annotations

from typing import Any


def _holder_event(holder: dict[str, Any]) -> Any:
    events = holder.get("events") or []
    return events[-1] if events else None


def build_wait_edges(locks: dict[str, dict]) -> list[dict[str, Any]]:
    """从锁快照构造 等待线程 -> 持锁线程 的边，每条边带可回溯事件。"""
    edges: list[dict[str, Any]] = []
    for lock_key, lock in locks.items():
        ltype = lock.get("type", "mutex")
        owners = lock.get("owners", {})
        readers = lock.get("readers", {})
        for waiter in lock.get("waiters", []):
            tid = waiter["thread"]
            mode = waiter.get("mode", "W")
            blockers: list[tuple[str, dict]] = []
            for holder_tid, holder in owners.items():
                if holder_tid == tid:
                    # W 等待且自己 W 持有：非重入 mutex 才是自死锁
                    if ltype == "mutex":
                        blockers.append((holder_tid, holder))
                    continue
                blockers.append((holder_tid, holder))
            if mode == "W":
                # 写等待被其它读持有阻塞（含读写锁升级时等待其它读者排空）
                for reader_tid, reader in readers.items():
                    if reader_tid == tid:
                        continue
                    blockers.append((reader_tid, reader))
            seen = set()
            for holder_tid, holder in blockers:
                if holder_tid in seen:
                    continue
                seen.add(holder_tid)
                edges.append(
                    {
                        "kind": "wait_lock",
                        "waiter": tid,
                        "holder": holder_tid,
                        "lock_key": lock_key,
                        "lock_addr": lock.get("addr"),
                        "gen": lock.get("gen", 0),
                        "mode": mode,
                        "holder_mode": holder.get("mode", "W"),
                        "wait_event": waiter.get("req_id"),
                        "hold_event": _holder_event(holder),
                        "wait_stack": waiter.get("stack", []),
                        "hold_stack": holder.get("stacks", [])[-1:]
                        if holder.get("stacks")
                        else [],
                        "since": waiter.get("since"),
                        "confidence": dict(waiter.get("edge_confidence") or {}),
                    }
                )
    return edges


def _johnson_cycles(nodes: list[str], adj: dict[str, list[str]], limit: int = 16):
    """枚举基本有向环（Johnson 简化实现），小规模图足够。"""
    result: list[list[str]] = []
    node_set = set(nodes)

    def strongconnect(sub: set[str]) -> list[set[str]]:
        index: dict[str, int] = {}
        low: dict[str, int] = {}
        stack: list[str] = []
        on: set[str] = set()
        comps: list[set[str]] = []
        counter = [0]

        def dfs(v: str):
            index[v] = low[v] = counter[0]
            counter[0] += 1
            stack.append(v)
            on.add(v)
            for w in adj.get(v, []):
                if w not in sub:
                    continue
                if w not in index:
                    dfs(w)
                    low[v] = min(low[v], low[w])
                elif w in on:
                    low[v] = min(low[v], index[w])
            if low[v] == index[v]:
                comp: set[str] = set()
                while True:
                    w = stack.pop()
                    on.discard(w)
                    comp.add(w)
                    if w == v:
                        break
                comps.append(comp)

        for v in sub:
            if v not in index:
                dfs(v)
        return comps

    def search(s: str, sub: set[str]) -> None:
        blocked: set[str] = {s}
        blocked_map: dict[str, set[str]] = {}
        path: list[str] = []

        def unblock(u: str) -> None:
            blocked.discard(u)
            while blocked_map.get(u):
                w = blocked_map[u].pop()
                if w in blocked:
                    unblock(w)

        def circuit(v: str) -> bool:
            found = False
            path.append(v)
            blocked.add(v)
            for w in adj.get(v, []):
                if w not in sub:
                    continue
                if w == s:
                    result.append(path[:])
                    found = True
                    if len(result) >= limit:
                        return True
                elif w not in blocked:
                    if circuit(w):
                        found = True
            if found:
                unblock(v)
            else:
                for w in adj.get(v, []):
                    if w in sub:
                        blocked_map.setdefault(w, set()).add(v)
            path.pop()
            return found

        circuit(s)

    remaining = set(node_set)
    ordered = list(nodes)
    for s in ordered:
        if s not in remaining or len(result) >= limit:
            break
        comps = strongconnect(remaining)
        scc = next((c for c in comps if s in c and len(c) > 1), None)
        self_loop = s in adj and s in adj[s]
        if scc is None and not self_loop:
            remaining.discard(s)
            continue
        if self_loop and scc is None:
            result.append([s])
            remaining.discard(s)
            continue
        search(s, scc)
        remaining.discard(s)
    # 自环
    for n in nodes:
        if len(result) >= limit:
            break
        if n in adj.get(n, []) and not any(c == [n] for c in result):
            result.append([n])
    return result


def _canon(cycle: list[str]) -> tuple[str, ...]:
    n = len(cycle)
    rotations = [tuple(cycle[i:] + cycle[:i]) for i in range(n)]
    return min(rotations)


def find_cycles(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    adj: dict[str, list[str]] = {}
    edge_index: dict[tuple[str, str], dict[str, Any]] = {}
    nodes: set[str] = set()
    for e in edges:
        a, b = e["waiter"], e["holder"]
        nodes.add(a)
        nodes.add(b)
        adj.setdefault(a, [])
        if b not in adj[a]:
            adj[a].append(b)
        edge_index.setdefault((a, b), e)

    node_list = sorted(nodes)
    raw = _johnson_cycles(node_list, dict(adj))
    reports = []
    seen_sig = set()
    for cyc in raw:
        canonical = _canon(cyc)
        if canonical in seen_sig:
            continue
        seen_sig.add(canonical)
        chain = []
        for i, waiter in enumerate(cyc):
            holder = cyc[(i + 1) % len(cyc)]
            edge = edge_index[(waiter, holder)]
            chain.append(edge)
        signature = "->".join(list(canonical) + [canonical[0]])
        lock_set = sorted({e["lock_key"] for e in chain})
        reports.append(
            {
                "threads": list(canonical),
                "signature": signature,
                "lock_keys": lock_set,
                "chain": chain,
                "confidence": _confidence(chain),
            }
        )
    reports.sort(key=lambda c: (len(c["threads"]), c["signature"]))
    return reports


def _confidence(chain: list[dict[str, Any]]) -> dict[str, Any]:
    flags: set[str] = set()
    for e in chain:
        flags.update(e.get("confidence", {}).get("flags", []))
    level = "low" if flags else "high"
    return {"level": level, "flags": sorted(flags)}


def cycle_signature(report: dict[str, Any]) -> str:
    return report["signature"]
