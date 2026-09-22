"""锁序解剖室 FastAPI 入口。"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

from locklab.engine import Engine
from locklab.minimize import minimize
from locklab.store import Store

DB_PATH = os.environ.get("LOCKLAB_DB", "locklab.db")
store = Store(DB_PATH)
app = FastAPI(title="锁序解剖室")

_engines: Dict[int, Engine] = {}


def _t_out(t: float):
    return t if t != float("inf") else "inf"


def get_engine(branch: int) -> Engine:
    if branch not in _engines:
        _engines[branch] = Engine(store.list_events(branch), store.lock_types(branch))
    return _engines[branch]


def reload_engine(branch: int) -> Engine:
    engine = get_engine(branch)
    engine.reload(store.list_events(branch), store.lock_types(branch))
    return engine


class ImportBody(BaseModel):
    branch: int = 0
    generation: int
    events: List[dict]


@app.post("/api/import")
def api_import(body: ImportBody):
    ids = store.import_events(body.branch, body.generation, body.events)
    engine = reload_engine(body.branch)
    return {"ok": True, "ids": ids, "replay_count": engine.replay_count}


@app.post("/api/events/append")
def api_append(body: ImportBody):
    return api_import(body)


@app.get("/api/branches")
def api_branches():
    return {"branches": store.branches()}


@app.get("/api/timeline")
def api_timeline(branch: int = 0):
    events = store.list_events(branch)
    threads = sorted({e.get("thread") for e in events if e.get("thread")})
    ts = [e["ts"] for e in events]
    return {"events": events, "threads": threads,
            "t_min": min(ts) if ts else 0, "t_max": max(ts) if ts else 0}


@app.get("/api/graph")
def api_graph(branch: int = 0, t: float = float("inf")):
    engine = get_engine(branch)
    state = engine.state_at(t)
    return {"t": _t_out(t), "branch": branch,
            "holds": state.held_graph(),
            "waits": state.waits(),
            "edges": state.edges(),
            "anomalies": state.anomalies}


@app.get("/api/cycles")
def api_cycles(branch: int = 0, t: float = float("inf")):
    engine = get_engine(branch)
    return {"t": _t_out(t), "branch": branch, "cycles": engine.cycles_at(t)}


class CorrectionBody(BaseModel):
    branch: int = 0
    kind: str  # edit_event | declare_lock
    event_id: Optional[int] = None
    fields: Optional[dict] = None
    generation: Optional[int] = None
    address: Optional[str] = None
    ltype: Optional[str] = None
    note: str = ""


@app.post("/api/corrections")
def api_correction(body: CorrectionBody):
    new_branch = store.create_branch(body.branch, body.note or body.kind)
    if body.kind == "edit_event":
        if body.event_id is None or not body.fields:
            raise HTTPException(400, "edit_event requires event_id and fields")
        store.edit_event(new_branch, body.event_id, body.fields)
    elif body.kind == "declare_lock":
        if body.generation is None or body.address is None or not body.ltype:
            raise HTTPException(400, "declare_lock requires generation, address, ltype")
        store.declare_lock(new_branch, body.generation, body.address, body.ltype)
    else:
        raise HTTPException(400, "unknown correction kind")
    reload_engine(new_branch)
    return {"ok": True, "branch": new_branch}


@app.get("/api/compare")
def api_compare(a: int = 0, b: int = 1, t: float = float("inf")):
    ca = get_engine(a).cycles_at(t)
    cb = get_engine(b).cycles_at(t)
    sa = {tuple(sorted(c["signature"])) for c in ca}
    sb = {tuple(sorted(c["signature"])) for c in cb}
    return {
        "a": {"branch": a, "cycles": ca},
        "b": {"branch": b, "cycles": cb},
        "only_a": [list(s) for s in sa - sb],
        "only_b": [list(s) for s in sb - sa],
    }


class MinimizeBody(BaseModel):
    branch: int = 0
    t: float = float("inf")
    cycle_index: int = 0


@app.post("/api/minimize")
def api_minimize(body: MinimizeBody):
    engine = get_engine(body.branch)
    cycles = engine.cycles_at(body.t)
    if body.cycle_index >= len(cycles):
        raise HTTPException(404, "no such cycle")
    target = cycles[body.cycle_index]["signature"]
    events = store.list_events(body.branch)
    kept = minimize(events, store.lock_types(body.branch), body.t, target)
    return {"ok": True, "original_count": len(events), "count": len(kept),
            "signature": [list(s) for s in target], "events": kept}


@app.post("/api/demo")
def api_demo():
    demo = [
        {"ts": 1, "thread": "T1", "type": "thread_start"},
        {"ts": 2, "thread": "T2", "type": "thread_start"},
        {"ts": 3, "thread": "T1", "lock": "0xA", "type": "acquire_request"},
        {"ts": 4, "thread": "T1", "lock": "0xA", "type": "acquire_success"},
        {"ts": 5, "thread": "T2", "lock": "0xB", "type": "acquire_request"},
        {"ts": 6, "thread": "T2", "lock": "0xB", "type": "acquire_success"},
        {"ts": 7, "thread": "T1", "lock": "0xB", "type": "acquire_request"},
        {"ts": 8, "thread": "T2", "lock": "0xA", "type": "acquire_request"},
        {"ts": 9, "thread": "T3", "type": "thread_start"},
        {"ts": 10, "thread": "T3", "lock": "0xC", "type": "acquire_request"},
        {"ts": 11, "thread": "T3", "lock": "0xC", "type": "acquire_success"},
        {"ts": 12, "thread": "T3", "lock": "0xC", "type": "cond_wait_start",
         "extra": {"cond": "0xQ"}},
        {"ts": 13, "thread": "T4", "type": "thread_start"},
        {"ts": 14, "thread": "T4", "lock": "0xC", "type": "acquire_request"},
        {"ts": 15, "thread": "T4", "lock": "0xC", "type": "acquire_success"},
        {"ts": 16, "thread": "T4", "lock": "0xC", "type": "notify",
         "extra": {"cond": "0xQ"}},
        {"ts": 17, "thread": "T4", "lock": "0xC", "type": "release"},
        {"ts": 18, "thread": "T3", "lock": "0xC", "type": "cond_wait_end",
         "extra": {"cond": "0xQ"}},
        {"ts": 19, "thread": "T3", "lock": "0xC", "type": "release"},
    ]
    ids = store.import_events(0, 1, demo)
    reload_engine(0)
    return {"ok": True, "ids": ids}


@app.get("/")
def index():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))
