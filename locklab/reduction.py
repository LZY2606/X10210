from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .engine import Event, analyze, cycle_signature, normalize_events


def build_groups(events: Sequence[Event]) -> List[Set[str]]:
    groups: Dict[str, Set[str]] = {}

    def group(event_id: str) -> Set[str]:
        groups.setdefault(event_id, {event_id})
        return groups[event_id]

    def bind(left: str, right: Optional[str]) -> None:
        if not right:
            return
        first = group(left)
        second = group(right)
        merged = first | second
        for event_id in merged:
            groups[event_id] = merged

    success_stacks: Dict[Tuple[str, str, str], List[str]] = {}
    cond_waits: Dict[str, List[str]] = {}
    request_by_id: Dict[str, str] = {}

    for event in events:
        group(event.id)
        if event.kind == "acquire_request":
            request_id = event.request_id or event.id
            request_by_id[request_id] = event.id
        elif event.kind == "acquire_success":
            if event.lock is not None:
                key = (event.generation, event.thread, event.lock)
                success_stacks.setdefault(key, []).append(event.id)
            request_id = event.request_id or event.id
            if request_id in request_by_id:
                bind(event.id, request_by_id[request_id])
        elif event.kind == "release" and event.lock is not None:
            key = (event.generation, event.thread, event.lock)
            if success_stacks.get(key):
                bind(event.id, success_stacks[key][-1])
        elif event.kind == "cond_wait":
            bind(event.id, event.request_id)
            cond = event.cond or f"{event.generation}:{event.lock or ''}"
            cond_waits.setdefault(cond, []).append(event.id)
            if event.lock is not None:
                key = (event.generation, event.thread, event.lock)
                if success_stacks.get(key):
                    bind(event.id, success_stacks[key][-1])
        elif event.kind == "cond_notify":
            cond = event.cond
            if cond and cond_waits.get(cond):
                bind(event.id, cond_waits[cond][0])

    unique: List[Set[str]] = []
    seen: Set[Tuple[str, ...]] = set()
    for values in groups.values():
        signature = tuple(sorted(values))
        if signature not in seen:
            seen.add(signature)
            unique.append(set(values))
    return sorted(unique, key=lambda item: (len(item), sorted(item)))


def cycle_target(analysis: Dict[str, Any], target_id: Optional[str]) -> Dict[str, Any]:
    cycles = analysis["snapshots"][-1]["cycles"] if analysis["snapshots"] else []
    if not cycles:
        raise ValueError("trace contains no cycle")
    if target_id is None:
        return cycles[0]
    for cycle in cycles:
        if cycle["id"] == target_id:
            return cycle
    raise ValueError("target cycle not found")


def same_cycle(candidate: Dict[str, Any], target: Dict[str, Any]) -> bool:
    wanted = set(cycle_signature(target))
    for cycle in candidate:
        if set(cycle_signature(cycle)) == wanted:
            return True
    return False


def reduce_cycle(
    rows: Sequence[Dict[str, Any]],
    lock_types: Optional[Dict[Tuple[str, str], str]] = None,
    target_id: Optional[str] = None,
) -> Dict[str, Any]:
    events = normalize_events(rows)
    baseline = analyze(events, lock_types)
    target = cycle_target(baseline, target_id)
    target_signature = cycle_signature(target)
    retained = {event.id for event in events}
    grouped = build_groups(events)
    changed = True
    while changed:
        changed = False
        current_events = [event for event in events if event.id in retained]
        current_groups = [item for item in build_groups(current_events) if len(item) < len(retained)]
        for removable in current_groups:
                candidate_ids = retained - removable
                candidate_events = [event for event in events if event.id in candidate_ids]
                result = analyze(candidate_events, lock_types)
                final_snapshot = result["snapshots"][-1] if result["snapshots"] else {"cycles": []}
                if same_cycle(final_snapshot["cycles"], target):
                    retained = candidate_ids
                    changed = True
                    break
    reduced_events = [event_to_dict(event) for event in events if event.id in retained]
    result = analyze([event for event in events if event.id in retained], lock_types)
    cycle = next(item for item in result["snapshots"][-1]["cycles"] if set(cycle_signature(item)) == set(target_signature))
    return {
        "target_cycle_id": target["id"],
        "event_count": len(reduced_events),
        "events": reduced_events,
        "cycle": cycle,
        "removed_count": len(events) - len(reduced_events),
    }


def event_to_dict(event: Event) -> Dict[str, Any]:
    return {
        "id": event.id,
        "generation": event.generation,
        "timestamp": event.timestamp,
        "seq": event.seq,
        "kind": event.kind,
        "thread": event.thread,
        "lock": event.lock,
        "mode": event.mode,
        "stack": list(event.stack),
        "request_id": event.request_id,
        "cond": event.cond,
        "broadcast": event.broadcast,
    }
