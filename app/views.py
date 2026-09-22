"""Read-only views over a ReplayResult: graph snapshots, near-miss report."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, List, Optional

from .models import FIRM, TENTATIVE, LOW
from .engine import ReplayResult, GenState, detect_cycles_for_gen


def graph_snapshot(result: ReplayResult, frame_index: int) -> dict:
    """Hold graph + wait graph reconstructed at the given frame.

    Replaying from scratch per query keeps snapshots honest (no stale mutable
    state); streams are small enough for that in this offline tool.
    """
    from .engine import ReplayEngine
    frame_index = max(0, min(frame_index, len(result.frames) - 1))
    upto = result.events[: frame_index + 1]
    engine = ReplayEngine(lock_types=result.lock_types)
    r = engine.run(upto)

    gens = []
    for gen in sorted(r.gens):
        gs = r.gens[gen]
        gens.append(_gen_snapshot(gen, gs, r))

    cycles = r.cycles_at.get(frame_index, [])
    # first-seen map across *this* prefix replay
    first_seen = {}
    for fi, cs in r.cycles_at.items():
        for c in cs:
            first_seen.setdefault(c["signature"], fi)

    return {
        "frame_index": frame_index,
        "event_id": result.frames[frame_index].event_id if result.frames else None,
        "ts": result.frames[frame_index].ts if result.frames else None,
        "kind": result.frames[frame_index].kind if result.frames else None,
        "total_frames": len(result.frames),
        "generations": gens,
        "cycles": cycles,
        "cycle_first_frame": {sig: fi for sig, fi in first_seen.items()},
        "anomalies_so_far": [a.to_dict() for a in r.anomalies],
    }


def _gen_snapshot(gen: int, gs: GenState, r: ReplayResult) -> dict:
    hold_edges = []
    for t, hh in gs.holders.items():
        for lock, h in hh.items():
            hold_edges.append({
                "type": "holds",
                "thread": t, "lock": lock, "gen": gen,
                "mode": h.mode, "count": h.count,
                "alive": t not in gs.ended,
                "request_event": h.request_id,
                "grant_event": h.grant_id,
                "stack": h.stack,
                "confidence": h.confidence if t not in gs.ended else LOW,
                "provenance": list(h.provenance),
                "lock_type": r.lock_types.get(f"{gen}:{lock}", "mutex"),
            })
    wait_edges = []
    for lock, waiters in gs.waiting.items():
        for t, w in waiters.items():
            wait_edges.append({
                "type": "waits",
                "thread": t, "lock": lock, "gen": gen,
                "mode": w.mode, "request_event": w.request_id,
                "request_ts": w.request_ts, "timeout": w.timeout,
                "stack": w.stack, "confidence": w.confidence,
                "provenance": list(w.provenance),
                "cond": w.cond,
                "alive": t not in gs.ended,
            })
    cond_edges = []
    for t, rec in gs.cond_waiting.items():
        cond_edges.append({
            "type": "cond_wait",
            "thread": t, "lock": rec.lock, "cond": rec.cond,
            "gen": gen, "mode": rec.mode,
            "cond_wait_event": rec.cond_wait_id,
            "stack": rec.stack,
            "timeout": rec.timeout,
            "alive": t not in gs.ended,
            "provenance": list(rec.released_provenance),
        })
    threads = [
        {"name": t, "alive": t not in gs.ended,
         "started": t in gs.threads,
         "holds": list(gs.held.get(t, []))}
        for t in sorted(gs.threads)
    ]
    return {
        "gen": gen,
        "threads": threads,
        "hold_edges": hold_edges,
        "wait_edges": wait_edges,
        "cond_edges": cond_edges,
    }


def near_misses(result: ReplayResult) -> List[dict]:
    """Lock-order inversions (A->B and B->A observed across threads).

    A live cycle already observed is reported as ``deadlock``; an inversion
    that never overlapped in time is a ``near_miss``.  Edges participating in
    any cycle frame are flagged ``lived`` during replay.
    """
    pairs: Dict[tuple, Dict[str, object]] = defaultdict(lambda: {"edges": []})
    by_key = {}
    for oe in result.order_edges.values():
        pairs[(oe.gen, oe.a, oe.b)]["edges"].append(oe)

    out = []
    seen = set()
    for (gen, a, b), one in pairs.items():
        rev = pairs.get((gen, b, a))
        if not rev:
            continue
        canon = (gen, min(a, b), max(a, b))
        if canon in seen:
            continue
        seen.add(canon)
        fwd_edges = [e for e in one["edges"]]
        rev_edges = [e for e in rev["edges"]]
        lived = any(e.lived for e in fwd_edges + rev_edges)
        overlap = _order_windows_overlap(result, fwd_edges, rev_edges)
        out.append({
            "gen": gen,
            "a": a, "b": b,
            "classification": "deadlock" if lived else
                              ("near_miss_overlap" if overlap else "near_miss"),
            "lived": lived,
            "forward": [e.to_dict() for e in fwd_edges],
            "reverse": [e.to_dict() for e in rev_edges],
        })
    out.sort(key=lambda d: (d["gen"], d["a"], d["b"]))
    return out


def _order_windows_overlap(result: ReplayResult, fwd, rev) -> bool:
    """True if some fwd edge's (hold A intersect hold B) window overlaps in
    time with some reverse edge's (hold B intersect hold A) window.

    A fwd edge at ts means both locks were held then.  Intersecting the
    recorded hold windows of both locks for its thread bounds the nested
    region; same for the reverse direction.  Any intersection between the two
    threads' nested regions is a genuinely dangerous near miss.
    """
    def nested_regions(edges):
        regions = []
        for e in edges:
            wa = result.hold_windows.get((e.gen, e.thread, e.a), [])
            wb = result.hold_windows.get((e.gen, e.thread, e.b), [])
            for sa, ea, _ma in wa:
                for sb, eb, _mb in wb:
                    lo = max(sa, sb)
                    hi = min(ea, eb)
                    if hi > lo:
                        regions.append((lo, hi))
        return regions

    rf = nested_regions(fwd)
    rr = nested_regions(rev)
    for x0, x1 in rf:
        for y0, y1 in rr:
            if x0 < y1 and y0 < x1:
                return True
    return False


def cycle_timeline(result: ReplayResult) -> List[dict]:
    rows = []
    for fi, frame in enumerate(result.frames):
        cycles = result.cycles_at.get(fi)
        if not cycles:
            rows.append({"frame": fi, "event_id": frame.event_id,
                         "gen": frame.gen, "ts": frame.ts,
                         "kind": frame.kind, "cycles": []})
        else:
            rows.append({"frame": fi, "event_id": frame.event_id,
                         "gen": frame.gen, "ts": frame.ts,
                         "kind": frame.kind,
                         "cycles": [c["signature"] for c in cycles]})
    return rows
