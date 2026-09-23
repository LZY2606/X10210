"""Deadlock cycle detection over the wait-for graph.

Every reported cycle carries concrete threads, locks, held stacks and waiting
stacks -- each edge already links back to the events that produced it.
Multiple elementary cycles coexist and are all enumerated (Johnson, 1975).
"""
from __future__ import annotations

from typing import Dict, List, Set, Tuple

Edge = dict
Node = str


def _adjacency(wait_view: Dict[str, dict]) -> Dict[str, List[Edge]]:
    adj: Dict[str, List[Edge]] = {}
    for tid, node in wait_view.items():
        for edge in node["edges"]:
            adj.setdefault(edge["from"], []).append(edge)
    return adj


def _plain_adj(adj: Dict[str, List[Edge]]) -> Dict[str, Set[str]]:
    return {node: {edge["to"] for edge in edges} for node, edges in adj.items()}


def _self_loops(adj: Dict[str, List[Edge]]) -> List[List[Edge]]:
    loops = []
    for node, edges in adj.items():
        for edge in edges:
            if edge["to"] == node:
                loops.append([edge])
    return loops


def _johnson_cycles(adj: Dict[str, List[Edge]]) -> List[List[Edge]]:
    graph = _plain_adj(adj)
    nodes = sorted(graph.keys() | {e["to"] for edges in adj.values() for e in edges})
    result: List[List[Edge]] = []

    def edge_between(src: Node, dst: Node) -> Edge:
        for edge in adj.get(src, []):
            if edge["to"] == dst:
                return edge
        raise KeyError((src, dst))

    def strongly_connected(sub_nodes: Set[Node], start: Node):
        """Tarjan SCC restricted to ``sub_nodes``; return SCC containing start."""
        index = 0
        stack: List[Node] = []
        on_stack: Set[Node] = set()
        indices: Dict[Node, int] = {}
        lowlink: Dict[Node, int] = {}
        containing: List[Set[Node]] = []

        def strongconnect(v: Node) -> None:
            nonlocal index
            indices[v] = index
            lowlink[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in graph.get(v, ()):  # type: ignore[union-attr]
                if w not in sub_nodes:
                    continue
                if w not in indices:
                    strongconnect(w)
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = min(lowlink[v], indices[w])
            if lowlink[v] == indices[v]:
                comp: Set[Node] = set()
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    comp.add(w)
                    if w == v:
                        break
                if start in comp:
                    containing.append(comp)

        for v in sorted(sub_nodes):
            if v not in indices:
                strongconnect(v)
        return containing[0] if containing else set()

    def circuits(start: Node, sub_nodes: Set[Node]) -> None:
        blocked: Set[Node] = set()
        blocked_map: Dict[Node, Set[Node]] = {n: set() for n in sub_nodes}
        path: List[Node] = []
        path_edges: List[Edge] = []
        found_stack: List[bool] = []

        def unblock(u: Node) -> None:
            blocked.discard(u)
            while blocked_map.get(u):
                w = blocked_map[u].pop()
                if w in blocked:
                    unblock(w)

        def circuit(v: Node) -> bool:
            found = False
            path.append(v)
            blocked.add(v)
            for w in sorted(graph.get(v, ())):  # type: ignore[union-attr]
                if w not in sub_nodes:
                    continue
                edge = edge_between(v, w)
                if w == start:
                    path_edges.append(edge)
                    result.append(list(path_edges))
                    path_edges.pop()
                    found = True
                elif w not in blocked:
                    path_edges.append(edge)
                    if circuit(w):
                        found = True
                    path_edges.pop()
            if found:
                unblock(v)
            else:
                for w in graph.get(v, ()):  # type: ignore[union-attr]
                    if w in sub_nodes:
                        blocked_map[w].add(v)
            path.pop()
            return found

        found_stack.append(True)
        circuit(start)

    remaining = set(nodes)
    for start in nodes:
        if start not in remaining:
            continue
        comp = strongly_connected(remaining, start)
        if len(comp) > 1:
            circuits(start, comp)
        # nodes from comp are removed from the search subgraph per Johnson
        remaining -= comp if comp else {start}
        if not comp:
            remaining.discard(start)
    return result
