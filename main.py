"""FastAPI application: Lock-Order Anatomy Lab (锁序解剖室)."""
from __future__ import annotations

import os
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.demo import demo_events, demo_lock_types, LATE_EVENT
from app.reduce import minimize
from app.storage import LabStore
from app.views import graph_snapshot, near_misses, cycle_timeline

DB_PATH = os.environ.get(
    "LAB_DB",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "lab.db"))

app = FastAPI(title="锁序解剖室", version="1.0")

_store: Optional[LabStore] = None


def get_store() -> LabStore:
    global _store
    if _store is None:
        _store = LabStore(DB_PATH)
    return _store


@app.on_event("startup")
def _startup():
    # fresh db on first start in this working dir; reseed demo when empty
    store = get_store()
    if not store.raw_events_base():
        store.add_events(demo_events())
        # simulate a late-arriving event (ts=920 arrives after ts>1000)
        store.add_events([LATE_EVENT])


# --- request / response schemas -------------------------------------------
class IngestIn(BaseModel):
    events: List[dict]


class BranchIn(BaseModel):
    id: str = Field(..., min_length=1, max_length=64)
    parent: str = "base"
    description: str = ""


class CorrectionIn(BaseModel):
    op: str
    id: Optional[int] = None
    patch: Optional[dict] = None
    event: Optional[dict] = None
    gen: Optional[int] = None
    lock: Optional[str] = None
    type: Optional[str] = None
    note: str = ""


class MinimizeIn(BaseModel):
    signature: str
    branch: str = "base"


# --- ingestion -------------------------------------------------------------
@app.post("/api/events")
def ingest(body: IngestIn):
    try:
        ids = get_store().add_events(body.events)
    except ValueError as e:
        raise HTTPException(400, str(e))
    stats = get_store().stream_stats()
    return {"ingested": len(ids), "ids": ids, "stream": stats}


@app.get("/api/events")
def events_raw():
    return {"events": get_store().raw_events_base()}


# --- branches --------------------------------------------------------------
@app.get("/api/branches")
def branches():
    store = get_store()
    return {"branches": store.list_branches()}


@app.post("/api/branches")
def create_branch(body: BranchIn):
    try:
        return get_store().create_branch(body.id, body.parent, body.description)
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.get("/api/branches/{branch_id}")
def branch_detail(branch_id: str):
    try:
        return get_store().branch_info(branch_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.post("/api/branches/{branch_id}/corrections")
def add_correction(branch_id: str, body: CorrectionIn):
    try:
        cor = get_store().add_correction(branch_id, body.dict())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"correction": cor}


@app.get("/api/branches/{branch_id}/events")
def branch_events(branch_id: str):
    return {"events": [e.to_dict() | {"id": e.id}
                       for e in get_store().branch_events(branch_id)]}


# --- graphs / timeline -----------------------------------------------------
@app.get("/api/state")
def state(branch: str = "base", frame: int = -1):
    store = get_store()
    result = store.replay(branch)
    if not result.frames:
        return {"frame_index": -1, "total_frames": 0, "generations": [],
                "cycles": [], "anomalies_so_far": []}
    fi = frame if frame >= 0 else len(result.frames) - 1
    snap = graph_snapshot(result, fi)
    snap["branch"] = branch
    snap["declarations"] = store.branch_info(branch).get(
        "declarations", {}) if branch != "base" else {}
    return snap


@app.get("/api/timeline")
def timeline(branch: str = "base"):
    result = get_store().replay(branch)
    return {"frames": cycle_timeline(result),
            "events": [
                {"id": e.id, "gen": e.gen, "ts": e.ts, "kind": e.kind,
                 "thread": e.thread, "lock": e.lock, "mode": e.mode,
                 "cond": e.cond, "timeout": e.timeout, "stack": e.stack}
                for e in result.events]}


@app.get("/api/near-misses")
def near_miss(branch: str = "base"):
    result = get_store().replay(branch)
    return {"near_misses": near_misses(result)}


@app.get("/api/anomalies")
def anomalies(branch: str = "base"):
    result = get_store().replay(branch)
    return {"anomalies": [a.to_dict() for a in result.anomalies]}


@app.get("/api/compare")
def compare(a: str = "base", b: str = "base"):
    store = get_store()
    ra, rb = store.replay(a), store.replay(b)
    sa = {c["signature"] for fr in ra.cycles_at.values() for c in fr}
    sb = {c["signature"] for fr in rb.cycles_at.values() for c in fr}
    return {
        "a": {"branch": a, "frames": len(ra.frames), "cycles": sorted(sa),
              "anomalies": len(ra.anomalies)},
        "b": {"branch": b, "frames": len(rb.frames), "cycles": sorted(sb),
              "anomalies": len(rb.anomalies)},
        "cycles_only_in_a": sorted(sa - sb),
        "cycles_only_in_b": sorted(sb - sa),
        "cycles_in_both": sorted(sa & sb),
    }


@app.post("/api/minimize")
def do_minimize(body: MinimizeIn):
    store = get_store()
    if body.branch != "base":
        info = store.branch_info(body.branch)
        decls = info.get("declarations", {})
    else:
        decls = {}
    events = store.branch_events(body.branch)
    result = minimize(events, body.signature, lock_types=decls)
    return result


@app.get("/api/stream-stats")
def stream_stats():
    return get_store().stream_stats()


@app.get("/api/health")
def health():
    return {"ok": True}


# --- static UI -------------------------------------------------------------
_STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=_STATIC), name="static")


@app.get("/")
def index():
    return FileResponse(os.path.join(_STATIC, "index.html"))
