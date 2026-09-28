"""Wait-for graph construction, elementary cycle enumeration and confidence."""
from __future__ import annotations

from typing import Any


def build_edges(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Merge hold edges + pending wait edges into one bipartite graph.

    Nodes are namespaced: thread ``T:<gen>:<tid>``, lock ``L:<gen>:<lock>``.
    Edges T->L mean "waits for", L->T mean "held by" (mode annotated).
    """
    edges: list[dict[str, Any]] = []
    for lock in state["locks"]:
        for holder in lock["holders"]:
            edges.append({
                "source": f'L:{lock["gen"]}:{lock["lock"]}',
                "target": f'T:{holder["gen"]}:{holder["tid"]}',
                "type": "hold",
                "mode": holder["mode"],
                "depth": holder["depth"],
                "gen": lock["gen"],
                "stack": holder["top_stack"],
                "eid": holder["top_eid"],
            })
    for t in state["threads"]:
        p = t.get("pending")
        if p:
            edges.append({
                "source": f'T:{p["gen"]}:{t["tid"]}',
                "target": f'L:{p["gen"]}:{p["lock"]}',
                "type": "wait",
                "mode": p["mode"],
                "gen": p["gen"],
                "stack": p["stack"],
                "eid": p["eid"],
                "reason": p["reason"],
                "deadline": p["deadline"],
            })
    return edges


def tarjan_scc(nodes: list[str], adj: dict[str, list[str]]) -> list[list[str]]:
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    onstack: set[str] = set()
    stack: list[str] = []
    counter = [0]
    result: list[list[str]] = []

    def strong(v: str) -> None:
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        onstack.add(v)
        for w in adj.get(v, ()):
            if w not in index:
                strong(w)
                low[v] = min(low[v], low[w])
            elif w in onstack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp: list[str] = []
            while True:
                w = stack.pop()
                onstack.discard(w)
                comp.append(w)
                if w == v:
                    break
            result.append(comp)

    for n in nodes:
        if n not in index:
            strong(n)
    return result


def elementary_cycles(nodes: list[str], adj: dict[str, list[str]],
                      limit: int = 16) -> list[list[str]]:
    """Enumerate elementary cycles, restricted to cyclic SCCs.

    Simplified Johnson-style search: from each node search only within the
    sub-graph of nodes >= it (each elementary cycle is found once, rooted at
    its smallest member).
    """
    sccs = [c for c in tarjan_scc(nodes, adj)
            if len(c) > 1 or (len(c) == 1 and c[0] in adj.get(c[0], ()))]
    cycles: list[list[str]] = []
    for comp in sccs:
        members = set(comp)
        ordered = sorted(comp)
        for i, start in enumerate(ordered):
            allowed = set(ordered[i:])
            path = [start]
            blocked = {start}

            def dfs(v: str) -> None:
                if len(cycles) >= limit:
                    return
                for w in adj.get(v, ()):
                    if w not in members or w not in allowed:
                        continue
                    if w == start:
                        cycles.append(list(path))
                        if len(cycles) >= limit:
                            return
                    elif w not in blocked:
                        blocked.add(w)
                        path.append(w)
                        dfs(w)
                        path.pop()
                        blocked.discard(w)

            dfs(start)
            if len(cycles) >= limit:
                return cycles
    return cycles


def cycle_evidence(state: dict[str, Any], limit: int = 16) -> list[dict[str, Any]]:
    edges = build_edges(state)
    nodes = sorted({e["source"] for e in edges} | {e["target"] for e in edges})
    adj: dict[str, list[str]] = {n: [] for n in nodes}
    edge_index: dict[tuple[str, str], dict[str, Any]] = {}
    for e in edges:
        adj[e["source"]].append(e["target"])
        edge_index.setdefault((e["source"], e["target"]), e)

    threads = {f'T:{t["gen"]}:{t["tid"]}': t for t in state["threads"]}
    locks = {f'L:{l["gen"]}:{l["lock"]}': l for l in state["locks"]}

    evidence: list[dict[str, Any]] = []
    for cyc in elementary_cycles(nodes, adj, limit):
        steps: list[dict[str, Any]] = []
        tids: list[str] = []
        locknames: list[str] = []
        for i, node in enumerate(cyc):
            nxt = cyc[(i + 1) % len(cyc)]
            edge = edge_index[(node, nxt)]
            if node.startswith("T:"):
                t = threads[node]
                p = t["pending"]
                tids.append(t["tid"])
                if nxt.startswith("L:"):
                    locknames.append(locks[nxt]["lock"])
                steps.append({
                    "actor": {"kind": "thread", "tid": t["tid"], "gen": t["gen"],
                              "status": t["status"]},
                    "edge": {
                        "type": "wait", "mode": edge["mode"],
                        "target_lock": locks[nxt]["lock"] if nxt.startswith("L:") else None,
                        "stack": p["stack"], "eid": p["eid"], "ts": p["ts"],
                        "reason": p["reason"], "deadline": p["deadline"],
                        "blockers": p["blockers"],
                    },
                })
            else:
                lk = locks[node]
                holder = next(h for h in lk["holders"]
                              if f'T:{h["gen"]}:{h["tid"]}' == nxt)
                if node.startswith("L:"):
                    locknames.append(lk["lock"])
                steps.append({
                    "actor": {"kind": "lock", "lock": lk["lock"], "gen": lk["gen"],
                              "ltype": lk["ltype"]},
                    "edge": {
                        "type": "hold", "mode": holder["mode"],
                        "owner_tid": holder["tid"],
                        "stack": holder["top_stack"], "eid": holder["top_eid"],
                        "depth": holder["depth"],
                    },
                })
        confidence = 1.0
        for step in steps:
            conf = state["edge_confidence"].get(
                f'{step["edge"].get("eid")}:{step["edge"]["type"]}', 1.0)
            confidence = min(confidence, conf)
        signature = "->".join(
            (f'T:{s["actor"].get("tid")}' if s["actor"]["kind"] == "thread"
             else f'L:{s["actor"].get("lock")}') for s in steps)
        evidence.append({
            "signature": signature,
            "threads": sorted(set(tids)),
            "locks": sorted(set(locknames)),
            "length": len(steps),
            "confidence": round(confidence, 3),
            "steps": steps,
        })
    evidence.sort(key=lambda c: (-c["confidence"], c["signature"]))
    return evidence
