"""Bipartite thread/lock graph and directed cycle extraction."""
from __future__ import annotations

from typing import Any


def _cycle_key(nodes: list[str]) -> tuple[str, ...]:
    # Rotate so the lexicographically smallest node starts the cycle.
    if not nodes:
        return tuple()
    start = min(range(len(nodes)), key=lambda index: nodes[index])
    rotated = nodes[start:] + nodes[:start]
    return tuple(rotated)


def find_cycles(
    held_edges: list[dict[str, Any]], waited_edges: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return elementary cycles in the held/waited-for graph.

    Edges are directed ``thread -> lock`` (waiting) and ``lock -> thread``
    (held).  A cycle therefore alternates threads and locks and represents a
    concrete deadlock (or self-deadlock / rwlock upgrade deadlock).
    """
    outgoing: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    held_by_lock: dict[str, dict[str, dict[str, Any]]] = {}
    for edge in held_edges:
        outgoing.setdefault(edge["lock"], []).append((edge["thread"], edge))
        held_by_lock.setdefault(edge["lock"], {})[edge["thread"]] = edge
    for edge in waited_edges:
        outgoing.setdefault(edge["thread"], []).append((edge["lock"], edge))

    found: dict[tuple[str, ...], list[dict[str, Any]]] = {}

    def walk(start: str, node: str, path: list[str], edges: list[dict[str, Any]]) -> None:
        for nxt, edge in outgoing.get(node, []):
            if nxt == start and len(path) >= 2:
                key = _cycle_key(path)
                found.setdefault(key, edges + [edge])
                continue
            if nxt in path:
                continue
            if nxt < start:
                continue
            walk(start, nxt, path + [nxt], edges + [edge])

    nodes = set(outgoing)
    for node in sorted(nodes):
        walk(node, node, [node], [])

    cycles: list[dict[str, Any]] = []
    for key, edges in found.items():
        thread_nodes = [node for node in key if not node.startswith("g") or ":" not in node]
        lock_nodes = [node for node in key if node not in thread_nodes]
        participants = []
        for edge in edges:
            if edge["kind"] == "wait":
                hold = held_by_lock.get(edge["lock"], {}).get(edge["thread"])
            else:
                hold = edge
            participants.append(
                {
                    "thread": edge["thread"] if edge["kind"] == "wait" else edge.get("thread"),
                    "lock": edge["lock"],
                    "role": edge["kind"],
                    "mode": edge.get("mode"),
                    "confidence": edge.get("confidence", "high"),
                    "wait_kind": edge.get("wait_kind"),
                    "wait_stack": edge.get("wait_stack", []),
                    "hold_stack": (hold or {}).get("hold_stack", edge.get("hold_stack", [])),
                    "events": edge.get("events", []),
                    "hold_events": (hold or {}).get("events", []),
                }
            )
        confidence = (
            "low"
            if any(part["confidence"] == "low" for part in participants)
            else "medium"
            if any(part["confidence"] == "medium" for part in participants)
            else "high"
        )
        cycles.append(
            {
                "id": "cycle-" + "-".join(key),
                "nodes": list(key),
                "threads": sorted(set(thread_nodes)),
                "locks": sorted(set(lock_nodes)),
                "confidence": confidence,
                "participants": participants,
                "edges": edges,
            }
        )
    cycles.sort(key=lambda cycle: (cycle["confidence"], cycle["id"]))
    return cycles
