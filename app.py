"""锁序解剖室 — FastAPI 入口。

复盘线程事件中的死锁与近失事件：导入事件、按时点查看持有图/等待图、
环证据回溯、事件校正分支、分支比较、最小复现片段缩减、流式增量重算。
"""
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import BranchEngine, detect_cycles
from minimize import minimize as minimize_trace
from store import Store

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")
store = Store(DB_PATH)

app = FastAPI(title="锁序解剖室")
_engines = {}          # branch_id -> BranchEngine
_append_stats = {}     # branch_id -> 最近一次增量重算统计


def apply_corrections(events, corrections):
    """事件校正形成新分支：drop_event / retime_event。并列合法重建都保留。"""
    out = []
    drops = {c["event_id"] for c in corrections if c.get("type") == "drop_event"}
    retimes = {c["event_id"]: c["ts"] for c in corrections if c.get("type") == "retime_event"}
    for ev in events:
        if ev["id"] in drops:
            continue
        ev = dict(ev)
        if ev["id"] in retimes:
            ev["ts"] = float(retimes[ev["id"]])
        out.append(ev)
    return out


def get_engine(branch_id=0):
    if branch_id in _engines:
        return _engines[branch_id]
    br = store.branch(branch_id)
    if br is None:
        raise HTTPException(404, "branch not found")
    events = apply_corrections(store.all_events(), br["corrections"])
    eng = BranchEngine(br["lock_types"])
    eng.set_events(events)
    _engines[branch_id] = eng
    return eng


def cycles_payload(eng, gen, t):
    graph, confidence = eng.graph_at(t)
    if gen is not None:
        for key in ("held", "wait"):
            graph[key] = [e for e in graph[key]
                          if _event_gen(eng, e.get("event_id")) in (None, gen)]
    cycles = detect_cycles(graph)
    return graph, confidence, cycles


def _event_gen(eng, event_id):
    if event_id is None:
        return None
    for e in eng.events:
        if e.get("id") == event_id:
            return e.get("gen")
    return None


class EventsIn(BaseModel):
    events: list


class BranchIn(BaseModel):
    name: str
    corrections: list = []
    lock_types: dict = {}


@app.get("/")
def index():
    return FileResponse(os.path.join("static", "index.html"))


@app.post("/api/reset")
def reset():
    store.reset()
    _engines.clear()
    _append_stats.clear()
    return {"ok": True}


@app.post("/api/events")
def import_events(body: EventsIn):
    ids = store.add_events(body.events)
    for ev, evid in zip(body.events, ids):
        ev["id"] = evid
    _engines.clear()
    return {"imported": len(ids), "ids": ids}


@app.post("/api/events/append")
def append_events(body: EventsIn):
    """流式追加：只重算受影响时间段，返回重算统计。"""
    ids = store.add_events(body.events)
    for ev, evid in zip(body.events, ids):
        ev["id"] = evid
    stats = {}
    for bid, eng in list(_engines.items()):
        stats[str(bid)] = eng.append(body.events)
    if not _engines:
        eng = get_engine(0)
        stats["0"] = dict(eng.stats, total=len(eng.events))
    _append_stats.update(stats)
    return {"appended": len(ids), "ids": ids, "recompute": stats}


@app.get("/api/events")
def list_events(gen: int = None, branch: int = 0):
    eng = get_engine(branch)
    evs = eng.events
    if gen is not None:
        evs = [e for e in evs if e.get("gen") == gen]
    return {"events": evs}


@app.get("/api/timeline")
def timeline(gen: int = None, branch: int = 0):
    eng = get_engine(branch)
    evs = [e for e in eng.events if gen is None or e.get("gen") == gen]
    threads = sorted({e["thread"] for e in evs if e.get("thread")})
    ts = [e["ts"] for e in evs]
    return {"threads": threads, "events": evs,
            "t_min": min(ts) if ts else 0, "t_max": max(ts) if ts else 0}


@app.get("/api/graph")
def graph(gen: int = None, t: float = None, branch: int = 0):
    eng = get_engine(branch)
    g, confidence, cycles = cycles_payload(eng, gen, t)
    return {"t": t, "confidence": confidence, "held": g["held"], "wait": g["wait"],
            "cycles": cycles, "anomalies": eng.state["anomalies"]}


@app.get("/api/cycles")
def cycles(gen: int = None, t: float = None, branch: int = 0):
    eng = get_engine(branch)
    g, confidence, cyc = cycles_payload(eng, gen, t)
    return {"t": t, "confidence": confidence, "cycles": cyc}


@app.post("/api/branches")
def create_branch(body: BranchIn):
    bid = store.create_branch(body.name, body.corrections, body.lock_types)
    return {"id": bid, "name": body.name}


@app.get("/api/branches")
def list_branches():
    return {"branches": store.branches()}


@app.get("/api/branches/compare")
def compare_branches(a: int, b: int, gen: int = None, t: float = None):
    out = {}
    for label, bid in (("a", a), ("b", b)):
        eng = get_engine(bid)
        _, confidence, cyc = cycles_payload(eng, gen, t)
        out[label] = {"confidence": confidence, "cycles": cyc,
                      "signatures": [c["signature"] for c in cyc]}
    sa = {tuple(s) for s in out["a"]["signatures"]}
    sb = {tuple(s) for s in out["b"]["signatures"]}
    out["only_a"] = sorted(sa - sb)
    out["only_b"] = sorted(sb - sa)
    out["common"] = sorted(sa & sb)
    return out


@app.post("/api/minimize")
def minimize(body: dict):
    gen = body.get("gen")
    t = body.get("t")
    branch = body.get("branch", 0)
    eng = get_engine(branch)
    evs = [e for e in eng.events if gen is None or e.get("gen") == gen]
    if t is not None:
        evs = [e for e in evs if e["ts"] <= t]
    graph, _ = eng.graph_at(t)
    cycles = detect_cycles(graph)
    if not cycles:
        raise HTTPException(400, "该时点没有环，无法缩减")
    targets = [c["signature"] for c in cycles]
    reduced = minimize_trace(evs, eng.lock_types, targets)
    return {"original": len(evs), "reduced": len(reduced), "events": reduced,
            "cycles": targets}


app.mount("/static", StaticFiles(directory="static"), name="static")
